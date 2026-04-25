"""
Universal AI Copilot — Hybrid Static + Dynamic SAP Agent.

Architecture (3-tier):
  WRITE ops → Static intent routing with Human-in-the-Loop confirmation
  READ ops  → Dynamic Text-to-OData Agent (2-step agentic loop):
    Step 1: Gemini generates OData query from natural language + SAP schema
    Step 2: SAP handler executes the raw query (with guardrails)
    Step 3: Raw JSON response fed back to Gemini for human-friendly answer

Entry points:
  bot.py MAIN_MENU state → MessageHandler → copilot.handle_text()
  bot.py MAIN_MENU state → MessageHandler(photo) → copilot.handle_photo()
  bot.py COPILOT_CONFIRM state → CallbackQueryHandler → copilot.handle_confirm()

Safety:
  Every external call (AI + SAP) is wrapped in try/except so the "thinking"
  message is ALWAYS updated — the bot can never hang silently.
  Dynamic queries are GET-only, whitelisted entity sets, $top capped at 50.
"""

import os
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode
from telegram.ext import CallbackContext
from ai_handler import AIHandler
from config import DEFAULT_EXPENSE_GL
from states import MAIN_MENU, COPILOT_CONFIRM
from logger_setup import get_logger

logger = get_logger(__name__)

# Minimum confidence threshold to act on an intent
MIN_CONFIDENCE = 0.55

# Error message shown when any part of the pipeline fails
_ERROR_MSG = (
    "🤖 *AI Copilot*\n━━━━━━━━━━━━━━━━━━\n"
    "⚠️ Something went wrong while processing your request.\n"
    "Please try again or use the menu buttons."
)


class CopilotFeature:
    def __init__(self, sap_handler, db_handler):
        self.sap = sap_handler
        self.db = db_handler
        self.ai = AIHandler()

    # ================================================================
    # ENTRY POINT: Free Text from MAIN_MENU
    # ================================================================
    def handle_text(self, update: Update, context: CallbackContext):
        """Global NLP router for free-text messages."""
        user = update.effective_user
        text = update.message.text.strip()

        # Skip commands and very short input
        if text.startswith('/') or len(text) < 3:
            return MAIN_MENU

        self.db.log_event(user, "COPILOT_QUERY", text)

        # Show thinking indicator
        thinking = update.message.reply_text(
            "🤖 *AI Copilot is thinking...*", parse_mode=ParseMode.MARKDOWN)

        try:
            return self._route_text(thinking, text, context)
        except Exception as e:
            logger.error("Copilot handle_text fatal error: %s", e, exc_info=True)
            self._safe_edit(thinking, _ERROR_MSG)
            return MAIN_MENU

    def _route_text(self, thinking, text, context):
        """Core routing logic — separated so the outer try/except catches everything."""
        # Step 1: Lightweight intent classification
        result = self.ai.classify_intent(text)

        reason = result.get('_reason', 'OK')

        if reason == 'ALL_MODELS_FAILED':
            logger.warning("Copilot: all AI models unavailable for: %s", text[:80])
            self._safe_edit(thinking,
                "🤖 *AI Copilot*\n━━━━━━━━━━━━━━━━━━\n"
                "⚠️ AI is temporarily unavailable (rate limit).\n"
                "Please wait 1-2 minutes and try again.")
            return MAIN_MENU

        if reason == 'PARSE_ERROR':
            raw_preview = result.get('_raw', 'N/A')[:150]
            logger.warning("Copilot: AI returned unparseable response: %s", raw_preview)
            self._safe_edit(thinking,
                "🤖 *AI Copilot*\n━━━━━━━━━━━━━━━━━━\n"
                "⚠️ AI returned an invalid response.\n"
                "Please rephrase your question and try again.")
            return MAIN_MENU

        if not result['success'] or result['intent'] == 'UNKNOWN' or result['confidence'] < MIN_CONFIDENCE:
            self._safe_edit(thinking,
                "🤖 *AI Copilot*\n━━━━━━━━━━━━━━━━━━\n"
                "I couldn't understand that request.\n\n"
                "💡 *Try something like:*\n"
                "• `How many orders were created today?`\n"
                "• `Show open invoices this month`\n"
                "• `What is the price of material 100?`\n"
                "• `Create order for customer 1000000 material 100 qty 10`\n"
                "• `List all quotations this week`")
            return MAIN_MENU

        intent = result['intent']
        entities = result['entities']
        summary = result['summary']

        logger.info("Copilot routing: intent=%s, conf=%.2f, summary=%s",
                     intent, result['confidence'], summary)

        # ============================================================
        # ROUTE: DYNAMIC QUERY (all READ/analytics/search operations)
        # ============================================================
        if intent == 'DYNAMIC_QUERY':
            try:
                return self._handle_dynamic_query(thinking, text)
            except Exception as e:
                logger.error("Copilot dynamic query error: %s", e, exc_info=True)
                self._safe_edit(thinking,
                                f"🤖 *AI Copilot*\n━━━━━━━━━━━━━━━━━━\n"
                                f"⚠️ Query failed.\n`{type(e).__name__}: {e}`")
                return MAIN_MENU

        # ============================================================
        # ROUTE: WRITE OPERATIONS (static, with confirmation screen)
        # ============================================================
        write_handlers = {
            'CREATE_ORDER': lambda: self._prepare_order_confirmation(thinking, entities, context, 'ORDER'),
            'CREATE_QUOTE': lambda: self._prepare_order_confirmation(thinking, entities, context, 'QUOTE'),
            'POST_PETTY_CASH': lambda: self._prepare_petty_cash_confirmation(thinking, entities, context),
        }

        if intent in write_handlers:
            try:
                return write_handlers[intent]()
            except Exception as e:
                logger.error("Copilot WRITE handler '%s' error: %s", intent, e, exc_info=True)
                self._safe_edit(thinking,
                                f"🤖 *AI Copilot*\n━━━━━━━━━━━━━━━━━━\n"
                                f"⚠️ Failed to prepare *{intent}*.\n"
                                f"`{type(e).__name__}: {e}`")
                return MAIN_MENU

        # Fallback — recognized but unhandled
        thinking.edit_text(
            f"🤖 *AI Copilot*\n━━━━━━━━━━━━━━━━━━\n"
            f"📋 Understood: _{summary}_\n\n"
            f"⚠️ This operation isn't supported via Copilot yet.\n"
            f"Please use the menu buttons.",
            reply_markup=self._menu_kb(),
            parse_mode=ParseMode.MARKDOWN)
        return MAIN_MENU

    # ================================================================
    # DYNAMIC TEXT-TO-ODATA AGENT (the 2-step agentic loop)
    # ================================================================
    def _handle_dynamic_query(self, thinking, user_text):
        """
        The core agentic loop:
          Step 1: AI generates OData query from natural language
          Step 2: SAP handler executes the query (with guardrails)
          Step 3: Raw results fed back to AI for human-friendly summarization
        """
        # --- Step 1: Generate OData query ---
        self._safe_edit(thinking,
                        "🤖 *AI Copilot is building your SAP query...*")

        query_result = self.ai.generate_odata_query(user_text)

        if not query_result['success']:
            self._safe_edit(thinking,
                f"🤖 *AI Copilot*\n━━━━━━━━━━━━━━━━━━\n"
                f"⚠️ Could not generate a query for your request.\n\n"
                f"💡 Try rephrasing your question.")
            return MAIN_MENU

        service = query_result['service']
        entity_set = query_result['entity_set']
        query_options = query_result['query_options']
        explanation = query_result['explanation']

        # --- Step 2: Execute the dynamic query against SAP ---
        self._safe_edit(thinking,
                        "🤖 *AI Copilot is querying SAP...*")

        sap_result = self.sap.execute_dynamic_odata_query(service, entity_set, query_options)

        if not sap_result['success']:
            error_msg = str(sap_result.get('error', 'Unknown'))[:200]
            self._safe_edit(thinking,
                f"🤖 *AI Copilot*\n━━━━━━━━━━━━━━━━━━\n"
                f"⚠️ SAP query failed.\n"
                f"📋 Query: `{entity_set}`")
            return MAIN_MENU

        data = sap_result['data']
        count = sap_result['count']

        # --- Step 3: Feed raw data back to AI for synthesis ---
        self._safe_edit(thinking,
                        f"🤖 *AI Copilot is analyzing {count} record(s)...*")

        answer = self.ai.synthesize_response(user_text, data, count)

        # Sanitize AI answer for Telegram Markdown safety
        safe_answer = self._sanitize_markdown(answer)

        # Build final response
        txt = (
            f"🤖 *AI Copilot*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"{safe_answer}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📊 _Records: {count}_ | _Source: {entity_set}_"
        )

        # Telegram message limit is 4096 chars
        if len(txt) > 4000:
            txt = txt[:3950] + "\n... (truncated)"

        # Try Markdown first, fall back to plain text if Telegram rejects it
        try:
            thinking.edit_text(txt, reply_markup=self._menu_kb(),
                               parse_mode=ParseMode.MARKDOWN)
        except Exception:
            # Strip all Markdown formatting and send plain
            plain_txt = (
                f"🤖 AI Copilot\n"
                f"{'━' * 18}\n"
                f"{answer}\n"
                f"{'━' * 18}\n"
                f"📊 Records: {count} | Source: {entity_set}"
            )
            if len(plain_txt) > 4000:
                plain_txt = plain_txt[:3950] + "\n... (truncated)"
            thinking.edit_text(plain_txt, reply_markup=self._menu_kb())
        return MAIN_MENU

    # ================================================================
    # ENTRY POINT: Photo + Caption from MAIN_MENU
    # ================================================================
    def handle_photo(self, update: Update, context: CallbackContext):
        """Handle photo messages with caption — multi-modal expense posting."""
        user = update.effective_user
        caption = update.message.caption or ""

        self.db.log_event(user, "COPILOT_PHOTO", caption or "Photo (no caption)")

        thinking = update.message.reply_text(
            "🤖 *AI Copilot is scanning your receipt...*", parse_mode=ParseMode.MARKDOWN)

        try:
            return self._route_photo(thinking, update, context, user, caption)
        except Exception as e:
            logger.error("Copilot handle_photo fatal error: %s", e, exc_info=True)
            self._safe_edit(thinking, _ERROR_MSG)
            return MAIN_MENU

    def _route_photo(self, thinking, update, context, user, caption):
        """Core photo routing — separated so the outer try/except catches everything."""
        # Download the photo
        photo_file = update.message.photo[-1].get_file()
        file_path = f"copilot_receipt_{user.id}.jpg"
        photo_file.download(file_path)

        # AI extraction
        result = self.ai.classify_expense_image(file_path, caption)

        if not result['success']:
            thinking.edit_text(
                f"❌ *AI could not process the image.*\n`{result.get('error', 'Unknown error')}`",
                reply_markup=self._menu_kb(),
                parse_mode=ParseMode.MARKDOWN)
            self._cleanup_file(file_path)
            return MAIN_MENU

        data = result['data']
        posting_type = data.get('posting_type', 'PETTY_CASH')

        # Store extracted data for confirmation
        today = datetime.now()
        context.user_data['copilot_action'] = 'POST_PETTY_CASH'
        context.user_data['copilot_photo'] = file_path
        context.user_data['copilot_data'] = {
            'amount': str(data.get('amount', '0')),
            'currency': data.get('currency', 'INR'),
            'description': data.get('description', 'Expense'),
            'date': data.get('date', today.strftime('%Y-%m-%d')),
            'vendor_name': data.get('vendor_name', 'Unknown'),
            'category': data.get('category', 'General'),
        }

        d = context.user_data['copilot_data']
        txt = (
            f"🤖 *AI Copilot — Receipt Scanned*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📄 *Type:* `Expense Claim`\n"
            f"🏪 *Vendor:* `{d['vendor_name']}`\n"
            f"💰 *Amount:* `{d['amount']} {d['currency']}`\n"
            f"📅 *Date:* `{d['date']}`\n"
            f"📝 *Description:* `{d['description']}`\n"
            f"🏷️ *Category:* `{d['category']}`\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"⚠️ *Review the details above before posting.*"
        )

        kb = [
            [InlineKeyboardButton("✅ Confirm & Post", callback_data="copilot_exec_petty_cash")],
            [InlineKeyboardButton("❌ Cancel", callback_data="copilot_cancel")]
        ]

        thinking.edit_text(txt, reply_markup=InlineKeyboardMarkup(kb),
                           parse_mode=ParseMode.MARKDOWN)
        return COPILOT_CONFIRM

    # ================================================================
    # CONFIRMATION HANDLER (Human-in-the-Loop)
    # ================================================================
    def handle_confirm(self, update: Update, context: CallbackContext):
        """Handle confirmation button clicks for write operations."""
        query = update.callback_query
        query.answer()
        action = query.data

        if action == 'copilot_cancel':
            self._cleanup_copilot(context)
            query.edit_message_text(
                "❌ *Cancelled.*",
                reply_markup=self._menu_kb(),
                parse_mode=ParseMode.MARKDOWN)
            return MAIN_MENU

        try:
            if action == 'copilot_exec_order':
                return self._execute_order(query, context)
            if action == 'copilot_exec_quote':
                return self._execute_quote(query, context)
            if action == 'copilot_exec_petty_cash':
                return self._execute_petty_cash(query, context)
        except Exception as e:
            logger.error("Copilot execution '%s' error: %s", action, e, exc_info=True)
            self._cleanup_copilot(context)
            query.edit_message_text(
                f"🤖 *AI Copilot*\n━━━━━━━━━━━━━━━━━━\n"
                f"⚠️ Execution failed.\n`{type(e).__name__}: {e}`",
                reply_markup=self._menu_kb(),
                parse_mode=ParseMode.MARKDOWN)
            return MAIN_MENU

        return MAIN_MENU

    # ================================================================
    # WRITE: CONFIRMATION BUILDERS
    # ================================================================
    def _prepare_order_confirmation(self, msg, entities, context, doc_type):
        """Build a confirmation screen for CREATE_ORDER / CREATE_QUOTE."""
        cust_id = entities.get('customer_id')
        cust_name = entities.get('customer_name')
        mat_id = entities.get('material_id')
        mat_name = entities.get('material_name')
        raw_qty = entities.get('quantity')
        raw_discount = entities.get('discount', 0) or 0
        ref = entities.get('reference', 'AI-Copilot')

        # --- Robust type casting (LLM may return strings) ---
        qty = None
        if raw_qty is not None:
            try:
                qty = int(raw_qty)
            except (ValueError, TypeError):
                logger.warning("Invalid quantity value from AI: %r", raw_qty)
                qty = None

        try:
            discount = float(raw_discount)
        except (ValueError, TypeError):
            logger.warning("Invalid discount value from AI: %r, defaulting to 0", raw_discount)
            discount = 0.0

        # Resolve customer name → ID
        if not cust_id and cust_name:
            results = self.sap.search_customers(cust_name)
            if results:
                cust_id = results[0]['Customer']
                cust_name = results[0]['CustomerName']

        # Resolve material name → ID
        if not mat_id and mat_name:
            products = self.sap.search_products(mat_name)
            if products:
                mat_id = products[0]['id']
                mat_name = products[0]['name']

        # Validate minimum required fields
        missing = []
        if not cust_id:
            missing.append("Customer ID")
        if not mat_id:
            missing.append("Material ID")
        if qty is None or qty <= 0:
            missing.append("Quantity (must be a positive number)")

        if missing:
            msg.edit_text(
                f"🤖 *AI Copilot — Missing Information*\n━━━━━━━━━━━━━━━━━━\n"
                f"I need more details to create the {doc_type.lower()}:\n\n"
                + "\n".join(f"❌ {m}" for m in missing)
                + "\n\n💡 Try: `Create order for customer 1000000 material 100 qty 10`",
                reply_markup=self._menu_kb(), parse_mode=ParseMode.MARKDOWN)
            return MAIN_MENU

        # Fetch price for display
        price_str = self.sap.get_product_price(mat_id, cust_id)
        try:
            unit_price = float(str(price_str).split(' ')[0].replace(',', ''))
            currency = str(price_str).split(' ')[1] if ' ' in str(price_str) else 'EUR'
        except (ValueError, IndexError):
            unit_price = 0.0
            currency = 'EUR'

        line_total = unit_price * qty
        discount_val = (line_total * discount) / 100
        net = line_total - discount_val

        # Store for execution
        context.user_data['copilot_action'] = f'CREATE_{doc_type}'
        context.user_data['copilot_data'] = {
            'customer': cust_id,
            'material': mat_id,
            'material_name': mat_name or mat_id,
            'quantity': qty,
            'unit_price': unit_price,
            'currency': currency,
            'discount': discount,
            'reference': ref or 'AI-Copilot',
            'line_total': line_total,
        }

        cb_action = 'copilot_exec_order' if doc_type == 'ORDER' else 'copilot_exec_quote'
        type_label = 'Sales Order' if doc_type == 'ORDER' else 'Sales Quotation'

        txt = (
            f"🤖 *AI Copilot — Confirm {type_label}*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"👤 *Customer:* `{cust_id}`"
            f"{f' ({cust_name})' if cust_name else ''}\n"
            f"📦 *Material:* `{mat_id}`"
            f"{f' ({mat_name})' if mat_name else ''}\n"
            f"🔢 *Quantity:* `{qty}`\n"
            f"🏷️ *Unit Price:* `{unit_price:.2f} {currency}`\n"
            f"💰 *Line Total:* `{line_total:.2f} {currency}`\n"
        )
        if discount > 0:
            txt += f"📉 *Discount:* `{discount}%` (-{discount_val:.2f})\n"
        txt += (
            f"💵 *Net Value:* `{net:.2f} {currency}`\n"
            f"🔖 *Reference:* `{ref or 'AI-Copilot'}`\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"⚠️ *Review carefully before confirming.*"
        )

        kb = [
            [InlineKeyboardButton(f"✅ Confirm & Create {type_label}", callback_data=cb_action)],
            [InlineKeyboardButton("❌ Cancel", callback_data="copilot_cancel")]
        ]

        msg.edit_text(txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        return COPILOT_CONFIRM

    def _prepare_petty_cash_confirmation(self, msg, entities, context):
        """Build confirmation for POST_PETTY_CASH from text (no image)."""
        raw_amount = entities.get('amount')
        desc = entities.get('description', 'Petty Cash Expense')
        currency = entities.get('currency', 'INR')

        # --- Robust type casting (LLM may return strings) ---
        amount = None
        if raw_amount is not None:
            try:
                amount = float(raw_amount)
            except (ValueError, TypeError):
                logger.warning("Invalid amount value from AI: %r", raw_amount)
                amount = None

        if not amount:
            msg.edit_text(
                "🤖 *AI Copilot — Missing Information*\n━━━━━━━━━━━━━━━━━━\n"
                "❌ I need at least an amount.\n\n"
                "💡 Try: `File expense claim 500 INR for office supplies`",
                reply_markup=self._menu_kb(), parse_mode=ParseMode.MARKDOWN)
            return MAIN_MENU

        today = datetime.now().strftime('%Y-%m-%d')
        context.user_data['copilot_action'] = 'POST_PETTY_CASH'
        context.user_data['copilot_data'] = {
            'amount': str(amount),
            'currency': currency,
            'description': desc,
            'date': today,
        }

        txt = (
            f"🤖 *AI Copilot — Confirm Expense Claim*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"💰 *Amount:* `{amount} {currency}`\n"
            f"📝 *Description:* `{desc}`\n"
            f"📅 *Date:* `{today}`\n"
            f"📂 *GL Account:* `{DEFAULT_EXPENSE_GL}` (Auto)\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"⚠️ *Review carefully before posting.*"
        )

        kb = [
            [InlineKeyboardButton("✅ Confirm & Post", callback_data="copilot_exec_petty_cash")],
            [InlineKeyboardButton("❌ Cancel", callback_data="copilot_cancel")]
        ]

        msg.edit_text(txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        return COPILOT_CONFIRM

    # ================================================================
    # WRITE: EXECUTION (post-confirmation)
    # ================================================================
    def _execute_order(self, query, context):
        """Execute confirmed Sales Order creation."""
        data = context.user_data.get('copilot_data', {})
        query.edit_message_text("⏳ *Creating Sales Order in SAP...*", parse_mode=ParseMode.MARKDOWN)

        items_list = [{
            'Material': data['material'],
            'Quantity': data['quantity'],
            'LineTotal': data['line_total'],
        }]

        res = self.sap.create_sales_order(
            customer_id=data['customer'],
            items_list=items_list,
            customer_ref=data.get('reference', 'AI-Copilot'),
            discount_pct=data.get('discount', 0),
        )

        self._cleanup_copilot(context)

        if res['success']:
            kb = [
                [InlineKeyboardButton("📄 PDF", callback_data=f"gen_pdf_{res['id']}"),
                 InlineKeyboardButton("🚚 Track", callback_data=f"track_order_{res['id']}")],
                [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
            ]
            query.edit_message_text(
                f"🤖✅ *Order Created Successfully!*\n━━━━━━━━━━━━━━━━━━\n"
                f"📄 SAP Order: `{res['id']}`\n"
                f"💰 Net: `{res.get('net', 'N/A')} {res.get('curr', '')}`",
                reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        else:
            query.edit_message_text(
                f"🤖❌ *Order Failed*\n━━━━━━━━━━━━━━━━━━\n`{res['error']}`",
                reply_markup=self._menu_kb(), parse_mode=ParseMode.MARKDOWN)

        return MAIN_MENU

    def _execute_quote(self, query, context):
        """Execute confirmed Sales Quotation creation."""
        data = context.user_data.get('copilot_data', {})
        query.edit_message_text("⏳ *Creating Quotation in SAP...*", parse_mode=ParseMode.MARKDOWN)

        items_list = [{
            'Material': data['material'],
            'Quantity': data['quantity'],
            'LineTotal': data['line_total'],
        }]

        res = self.sap.create_sales_quotation(
            customer_id=data['customer'],
            items_list=items_list,
            customer_ref=data.get('reference', 'AI-Copilot'),
        )

        self._cleanup_copilot(context)

        if res['success']:
            query.edit_message_text(
                f"🤖✅ *Quotation Created Successfully!*\n━━━━━━━━━━━━━━━━━━\n"
                f"📝 SAP Quote: `{res['id']}`",
                reply_markup=self._menu_kb(), parse_mode=ParseMode.MARKDOWN)
        else:
            query.edit_message_text(
                f"🤖❌ *Quotation Failed*\n━━━━━━━━━━━━━━━━━━\n`{res['error']}`",
                reply_markup=self._menu_kb(), parse_mode=ParseMode.MARKDOWN)

        return MAIN_MENU

    def _execute_petty_cash(self, query, context):
        """Execute confirmed Petty Cash journal entry."""
        data = context.user_data.get('copilot_data', {})
        query.edit_message_text("⏳ *Posting Petty Cash to SAP...*", parse_mode=ParseMode.MARKDOWN)

        today = datetime.now()
        date_str = data.get('date', today.strftime('%Y-%m-%d'))
        doc_date = f"{date_str}T00:00:00"

        exp_payload = {
            'company': '1000',
            'currency': data.get('currency', 'INR'),
            'doc_date': doc_date,
            'post_date': doc_date,
            'ref_id': data.get('description', 'AI Copilot Expense'),
            'items': [{
                'amount': data.get('amount', '0'),
                'desc': data.get('description', 'Petty Cash Expense'),
                'gl': DEFAULT_EXPENSE_GL,
                'cc': '',
                'tax': 'I0',
            }],
        }

        res = self.sap.create_journal_entry_without_vendor(exp_payload)

        if res['success']:
            self.db.log_event(
                type('User', (), {'id': 0, 'username': 'COPILOT'})(),
                "PETTY_CASH_POSTED", data.get('amount', '0'))

        self._cleanup_copilot(context)

        if res['success']:
            query.edit_message_text(
                f"🤖✅ *Petty Cash Posted!*\n━━━━━━━━━━━━━━━━━━\n"
                f"📄 Document: `{res['id']}`\n"
                f"📅 Year: `{res['year']}`",
                reply_markup=self._menu_kb(), parse_mode=ParseMode.MARKDOWN)
        else:
            query.edit_message_text(
                f"🤖❌ *Posting Failed*\n━━━━━━━━━━━━━━━━━━\n`{res['error']}`",
                reply_markup=self._menu_kb(), parse_mode=ParseMode.MARKDOWN)

        return MAIN_MENU

    # ================================================================
    # HELPERS
    # ================================================================
    def _menu_kb(self):
        return InlineKeyboardMarkup(
            [[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]])

    def _cleanup_copilot(self, context):
        """Clean up all copilot state from user_data, including temp photo files."""
        photo = context.user_data.pop('copilot_photo', None)
        if photo:
            self._cleanup_file(photo)
        context.user_data.pop('copilot_action', None)
        context.user_data.pop('copilot_data', None)

    @staticmethod
    def _cleanup_file(path):
        try:
            os.remove(path)
        except OSError:
            pass

    @staticmethod
    def _sanitize_markdown(text):
        """
        Ensure Telegram Markdown-safe text by balancing special characters.
        Unmatched `, *, _ cause Telegram BadRequest errors.
        """
        # Balance backticks — if odd count, strip them all
        if text.count('`') % 2 != 0:
            text = text.replace('`', "'")
        # Balance asterisks
        if text.count('*') % 2 != 0:
            text = text.replace('*', '')
        # Balance underscores (careful: underscores in IDs like SalesOrder_123 are common)
        if text.count('_') % 2 != 0:
            # Add a trailing underscore to balance
            text += '_'
        return text

    @staticmethod
    def _safe_edit(msg, text):
        """Edit a message safely — never let a Telegram API error crash the flow."""
        try:
            msg.edit_text(text, reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]]),
                parse_mode=ParseMode.MARKDOWN)
        except Exception:
            # Markdown failed, try plain text
            try:
                clean = text.replace('*', '').replace('_', '').replace('`', '')
                msg.edit_text(clean, reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]]))
            except Exception as e:
                logger.warning("Failed to edit thinking message: %s", e)
