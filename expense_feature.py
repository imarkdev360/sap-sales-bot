import os
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode, InlineQueryResultArticle, \
    InputTextMessageContent
from telegram.ext import CallbackContext, ConversationHandler
from telegram.error import BadRequest
from datetime import datetime
from uuid import uuid4
from ai_handler import AIHandler
from states import (
    E_CO, E_VENDOR_TYPE, E_V_SRCH, E_V_CONF, E_INPUT_METHOD, E_SCAN_PHOTO,
    E_REF, E_DATE_DOC, E_DATE_POST, E_AMT, E_DESC, E_GL_SRCH, E_CC_SRCH,
    E_TAX_SEL, E_ADD_MORE, E_FINAL_REVIEW, E_EDIT_MENU, E_EDIT_VALUE,
    EW_INPUT_METHOD,
)
from config import DEFAULT_EXPENSE_GL
from logger_setup import get_logger

logger = get_logger(__name__)


class ExpenseFeature:
    def __init__(self, sap_handler, db_handler):
        self.sap = sap_handler
        self.db = db_handler
        self.ai = AIHandler()

    # --- 1. START FLOW ---
    def start_flow(self, update: Update, context: CallbackContext):
        query = update.callback_query
        query.answer()
        # 📝 LOG: Process Start
        self.db.log_event(update.effective_user, "EXPENSE_START", "🚀 User started Expense Claim Process")

        context.user_data['exp'] = {'items': []}

        # Initial loading message
        query.edit_message_text("🔄 *Connecting to SAP...*", parse_mode=ParseMode.MARKDOWN)

        companies = self.sap.get_companies_dynamic()
        context.user_data['co_currencies'] = {c['id']: c['curr'] for c in companies}

        kb = [[InlineKeyboardButton(f"🏢 {c['name']} ({c['id']})", callback_data=f"eco_{c['id']}")] for c in companies]
        kb.append([InlineKeyboardButton("❌ Cancel", callback_data="main_menu")])

        query.edit_message_text("🆕 *New Expense Claim*\n━━━━━━━━━━━━━━━━━━\n\n🏢 **Select Company Code:**",
                                reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        return E_CO

    def handle_co(self, update, context):
        query = update.callback_query
        # Extract Company ID from callback data
        co_id = query.data.split('_')[1]

        # 📝 LOG: Company Selection
        self.db.log_event(update.effective_user, "EXPENSE_INPUT", f"🏢 Selected Company: {co_id}")

        context.user_data['exp']['company'] = co_id
        # Safe currency retrieval
        currencies = context.user_data.get('co_currencies', {})
        curr = currencies.get(co_id, "USD")
        context.user_data['exp']['currency'] = curr

        # Vendor type selection
        kb = [
            [InlineKeyboardButton("🏢 With Vendor", callback_data="exp_type_with")],
            [InlineKeyboardButton("💵 Without Vendor", callback_data="exp_type_without")],
            [InlineKeyboardButton("❌ Cancel", callback_data="main_menu")]
        ]

        query.edit_message_text(
            f"✅ Company: `{co_id}` ({curr})\n\n📋 **File Expense Claim — Select Type:**",
            reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        return E_VENDOR_TYPE

    def handle_vendor_type(self, update, context):
        query = update.callback_query
        query.answer()
        company = context.user_data['exp']['company']

        if query.data == "exp_type_with":
            context.user_data['exp']['is_without_vendor'] = False
            kb = [[InlineKeyboardButton("🔎 Start Live Vendor Search", switch_inline_query_current_chat=f"ven_{company} ")],
                  [InlineKeyboardButton("❌ Cancel", callback_data="main_menu")]]
            query.edit_message_text(
                f"👤 **Select Vendor:**\n🅰️ Type Name/ID below\n🅱️ Use 'Live Search' button",
                reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
            return E_V_SRCH

        elif query.data == "exp_type_without":
            context.user_data['exp']['is_without_vendor'] = True
            context.user_data['exp']['vendor'] = "N/A (Direct Expense)"

            kb = [[InlineKeyboardButton("📸 Scan Receipt (AI)", callback_data="ew_input_scan")],
                  [InlineKeyboardButton("⌨️ Manual Entry", callback_data="ew_input_manual")],
                  [InlineKeyboardButton("❌ Cancel", callback_data="main_menu")]]
            query.edit_message_text(
                "💵 **Without Vendor Selected.**\n\n🧾 **How do you want to enter details?**\nI can auto-fill from a photo using AI.",
                reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
            # Route to ExpenseWithoutVendorFeature states
            return EW_INPUT_METHOD

    # --- SEARCH LOGIC (FIXED TO HANDLE VENDOR, GL, CC, TAX) ---
    def handle_inline_vendor_query(self, update: Update, context: CallbackContext):
        query = update.inline_query.query
        if not query: return

        # Default empty result
        results = []

        try:
            # Format expected: type_company term (e.g., "ven_1000 ABC" or "gl_1000 rent")
            parts = query.split(' ')
            cmd_part = parts[0]  # e.g. ven_1000

            if '_' in cmd_part:
                q_type, company_code = cmd_part.split('_', 1)
            else:
                return  # Invalid format

            term = " ".join(parts[1:]).strip() if len(parts) > 1 else ""

            # 1. Vendor Search
            if q_type == 'ven':
                data = self.sap.search_vendors_dynamic(term, company_code)
                for i in data[:50]:
                    results.append(InlineQueryResultArticle(
                        id=str(uuid4()),
                        title=f"👤 {i['name']}",
                        description=f"ID: {i['id']}",
                        input_message_content=InputTextMessageContent(f"/sel_ven_{i['id']}")))

            # 2. GL Account Search (Moved here to handle all inline queries centrally)
            elif q_type == 'gl':
                data = self.sap.search_gl_accounts_dynamic(term)
                for i in data[:50]:
                    results.append(InlineQueryResultArticle(
                        id=str(uuid4()),
                        title=f"📂 {i['name']}",
                        description=f"GL: {i['id']}",
                        input_message_content=InputTextMessageContent(f"/sel_gl_{i['id']}")))

            # 3. Cost Center Search
            elif q_type == 'cc':
                data = self.sap.search_cost_centers_dynamic(term, company_code)
                for i in data[:50]:
                    results.append(InlineQueryResultArticle(
                        id=str(uuid4()),
                        title=f"🏢 {i['name']}",
                        description=f"CC: {i['id']}",
                        input_message_content=InputTextMessageContent(f"/sel_cc_{i['id']}")))

            # 4. Tax Code Search
            elif q_type == 'tax':
                data = self.sap.search_tax_codes_dynamic(term, company_code)
                for i in data[:50]:
                    results.append(InlineQueryResultArticle(
                        id=str(uuid4()),
                        title=f"🧾 {i['name']}",
                        description=f"Code: {i['id']}",
                        input_message_content=InputTextMessageContent(f"/sel_tax_{i['id']}")))

        except Exception as e:
            logger.error(f"Inline Search Error: {e}")
            return

        try:
            update.inline_query.answer(results, cache_time=0)
        except BadRequest as e:
            logger.warning("Inline query answer expired (SAP response too slow): %s", e)

    def search_vendor_text(self, update: Update, context: CallbackContext):
        keyword = update.message.text

        # If user pasted a selection command manually
        if keyword.startswith('/sel_ven_'):
            return self.select_vendor(update, context)

        company = context.user_data.get('exp', {}).get('company', '1000')
        msg = update.message.reply_text(f"🔍 Searching vendor '{keyword}' in {company}...")

        res = self.sap.search_vendors_dynamic(keyword, company)
        context.bot.delete_message(chat_id=msg.chat_id, message_id=msg.message_id)

        if not res:
            kb = [[InlineKeyboardButton("❌ Cancel", callback_data="main_menu")]]
            update.message.reply_text(f"❌ No vendor found in {company}. Try again:",
                                      reply_markup=InlineKeyboardMarkup(kb))
            return E_V_SRCH

        kb = [[InlineKeyboardButton(f"👤 {i['name']} ({i['id']})", callback_data=f"even_{i['id']}_{i['name'][:10]}")] for
              i in res[:5]]  # Show max 5 buttons
        kb.append([InlineKeyboardButton("❌ Cancel", callback_data="main_menu")])

        update.message.reply_text("👇 **Select Vendor:**", reply_markup=InlineKeyboardMarkup(kb),
                                  parse_mode=ParseMode.MARKDOWN)
        return E_V_SRCH

    def select_vendor(self, update: Update, context: CallbackContext):
        if update.callback_query:
            parts = update.callback_query.data.split('_')
            ven_id = parts[1]
            update.callback_query.answer()
            msg_func = update.callback_query.edit_message_text
        else:
            ven_id = update.message.text.replace("/sel_ven_", "").strip()
            msg_func = update.message.reply_text

        # 📝 LOG: Vendor Selection
        self.db.log_event(update.effective_user, "EXPENSE_INPUT", f"👤 Selected Vendor: {ven_id}")

        company = context.user_data['exp']['company']
        context.user_data['exp']['vendor'] = ven_id

        kb = [[InlineKeyboardButton("✅ Yes, Correct", callback_data="vconf_yes")],
              [InlineKeyboardButton("🔄 Re-enter Vendor", callback_data="vconf_no")],
              [InlineKeyboardButton("❌ Cancel", callback_data="main_menu")]]

        msg_func(f"👤 **Selected Vendor:**\n`{ven_id}`\n\nIs this correct?", reply_markup=InlineKeyboardMarkup(kb),
                 parse_mode=ParseMode.MARKDOWN)
        return E_V_CONF

    def handle_ven_confirm(self, update, context):
        query = update.callback_query
        query.answer()
        company = context.user_data['exp']['company']

        if query.data == "vconf_no":
            kb = [[InlineKeyboardButton("🔎 Live Vendor Search", switch_inline_query_current_chat=f"ven_{company} ")],
                  [InlineKeyboardButton("❌ Cancel", callback_data="main_menu")]]
            query.edit_message_text("🔄 **Re-enter Vendor:**\nType Name/ID or Use Button:",
                                    reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
            return E_V_SRCH

        kb = [[InlineKeyboardButton("📸 Scan Receipt (AI)", callback_data="input_scan")],
              [InlineKeyboardButton("⌨️ Manual Entry", callback_data="input_manual")],
              [InlineKeyboardButton("❌ Cancel", callback_data="main_menu")]]
        query.edit_message_text(
            "🧾 **How do you want to enter details?**\n\nI can auto-fill details from a photo using AI.",
            reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        return E_INPUT_METHOD

    def handle_input_method(self, update, context):
        query = update.callback_query
        query.answer()

        # 📝 LOG: Method Selection
        method = "Scan (AI)" if query.data == "input_scan" else "Manual Entry"
        self.db.log_event(update.effective_user, "EXPENSE_ACTION", f"Selected Input Method: {method}")

        if query.data == "input_manual":
            kb = [[InlineKeyboardButton("❌ Cancel", callback_data="main_menu")]]
            query.edit_message_text("🆔 **Enter Reference Number:**\nExample: `INV-12345`",
                                    reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
            return E_REF
        elif query.data == "input_scan":
            query.edit_message_text("📸 **Please upload the Receipt Photo.**\n\n(Send it as 'Photo', not 'File')",
                                    parse_mode=ParseMode.MARKDOWN)
            return E_SCAN_PHOTO

    # --- 2. INTELLIGENT AI LOGIC ---
    def handle_photo_scan(self, update, context):
        user = update.effective_user
        try:
            if not update.message.photo:
                update.message.reply_text("⚠️ Please send a PHOTO, not a file.")
                return E_SCAN_PHOTO

            photo_file = update.message.photo[-1].get_file()
            file_path = f"receipt_{user.id}.jpg"
            photo_file.download(file_path)

            msg = update.message.reply_text("🤖 **AI is analyzing receipt...**")

            result = self.ai.analyze_receipt(file_path)

            if result['success']:
                context.user_data['photo_path'] = file_path
            else:
                if os.path.exists(file_path): os.remove(file_path)

            if not result['success']:
                msg.edit_text(f"⚠️ **AI Error:**\n{result.get('error')}\n\nPlease enter manually.")
                context.bot.send_message(chat_id=update.effective_chat.id, text="🆔 **Enter Reference Number:**")
                return E_REF

            data = result['data']

            # 📝 LOG: AI Data Extraction
            self.db.log_event(user, "AI_SCAN", f"Extracted Data: {data}")

            # --- HEADER LOGIC ---
            vendor_name = data.get('vendor_name', '')
            if vendor_name and len(vendor_name) > 2:
                ref_id = vendor_name[:16].upper()
            else:
                ref_id = f"SCAN-{int(datetime.now().timestamp())}"
            context.user_data['exp']['ref_id'] = ref_id

            # Dates
            raw_date = data.get('date', datetime.now().strftime('%Y-%m-%d'))
            doc_date = f"{raw_date}T00:00:00"
            today_date = datetime.now().strftime('%Y-%m-%d')
            post_date = f"{today_date}T00:00:00"

            context.user_data['exp']['doc_date'] = doc_date
            context.user_data['exp']['post_date'] = post_date

            company = context.user_data['exp']['company']

            # --- 3. INTELLIGENT GL SELECTION ---
            gl_candidates = self.sap.search_gl_accounts_dynamic("6", company)
            valid_gls = []
            if gl_candidates:
                valid_gls = [gl for gl in gl_candidates if gl['id'].startswith('6')]

            selected_gl = DEFAULT_EXPENSE_GL

            if valid_gls:
                cat = data.get('category', '').lower()
                desc = data.get('description', '').lower()
                best_match = None

                for gl in valid_gls:
                    gl_name = gl['name'].lower()
                    if cat and cat in gl_name:
                        best_match = gl['id']
                        break
                    if desc and any(word in gl_name for word in desc.split()):
                        best_match = gl['id']
                        break

                if best_match:
                    selected_gl = best_match
                else:
                    has_default = any(gl['id'] == DEFAULT_EXPENSE_GL for gl in valid_gls)
                    if has_default:
                        selected_gl = DEFAULT_EXPENSE_GL
                    else:
                        selected_gl = valid_gls[0]['id']

            # --- 4. COST CENTER (Hardcoded per Company Code) ---
            cc_map = {'1000': '10001010', '2000': '20001010'}
            selected_cc = cc_map.get(company, '10001010')

            # --- 5. INTELLIGENT TAX MAPPING ---
            tax_map = {'1000': 'V0', '2000': 'I0'}
            selected_tax = tax_map.get(company, 'I0')

            item = {
                'amount': str(data.get('amount', '0.00')),
                'desc': data.get('description', 'Expense'),
                'gl': selected_gl,
                'cc': selected_cc,
                'tax': selected_tax
            }
            context.user_data['temp_item'] = item

            return self._show_review_screen(msg.edit_text, context)

        except Exception as e:
            logger.error("Photo scan error: %s", e, exc_info=True)
            update.message.reply_text("System Error. Try Manual.")
            return E_REF

    # --- 6. UNIVERSAL SUMMARY SCREEN ---
    def _show_review_screen(self, msg_func, context, custom_text=None):
        header = context.user_data['exp']

        if 'items' in header and header['items']:
            item = header['items'][-1]
            context.user_data['temp_item'] = item
        else:
            item = context.user_data.get('temp_item', {})

        d_doc = header.get('doc_date', '').split('T')[0]
        d_post = header.get('post_date', '').split('T')[0]

        intro = custom_text if custom_text else "📋 **Expense Claim Summary**"

        txt = (
            f"{intro}\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📅 **Doc Date:** `{d_doc}` (Receipt)\n"
            f"📅 **Post Date:** `{d_post}` (Today)\n"
            f"👤 **Vendor:** `{header.get('vendor')}`\n"
            f"🆔 **Ref ID:** `{header.get('ref_id')}`\n"
            f"────────────────\n"
            f"💰 **Amount:** `{item.get('amount')} {header.get('currency')}`\n"
            f"📝 **Desc:** {item.get('desc')}\n"
            f"📂 **GL:** `{item.get('gl')}`\n"
            f"🏢 **CC:** `{item.get('cc')}`\n"
            f"🧾 **Tax:** `{item.get('tax')}`\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"✅ **Check details above.**\nClick 'Edit' to change anything."
        )

        kb = [
            [InlineKeyboardButton("✅ File Expense Claim", callback_data="post_now")],
            [InlineKeyboardButton("✏️ Edit Fields", callback_data="edit_menu")],
            [InlineKeyboardButton("❌ Discard", callback_data="main_menu")]
        ]
        msg_func(txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        return E_FINAL_REVIEW

    def handle_final_review(self, update, context):
        query = update.callback_query
        query.answer()

        if query.data == "post_now":
            return self.execute_posting(update, context)

        elif query.data == "edit_menu":
            # 📝 LOG: Edit Button Clicked
            self.db.log_event(query.from_user, "EXPENSE_ACTION", "🖱️ User clicked 'Edit Fields'")

            kb = [
                [InlineKeyboardButton("📅 Doc Date", callback_data="edit_field_doc_date"),
                 InlineKeyboardButton("📅 Post Date", callback_data="edit_field_post_date")],
                [InlineKeyboardButton("💰 Amount", callback_data="edit_field_amount"),
                 InlineKeyboardButton("📝 Desc", callback_data="edit_field_desc")],
                [InlineKeyboardButton("📂 GL Account", callback_data="edit_field_gl"),
                 InlineKeyboardButton("🧾 Tax Code", callback_data="edit_field_tax")],
                [InlineKeyboardButton("🆔 Ref ID", callback_data="edit_field_ref")],
                [InlineKeyboardButton("🔙 Back to Summary", callback_data="back_to_review")]
            ]
            query.edit_message_text("✏️ **Select Field to Edit:**", reply_markup=InlineKeyboardMarkup(kb),
                                    parse_mode=ParseMode.MARKDOWN)
            return E_EDIT_MENU

    def handle_edit_menu(self, update, context):
        query = update.callback_query
        query.answer()
        data = query.data

        if data == "back_to_review":
            return self._show_review_screen(query.edit_message_text, context)

        field = data.replace("edit_field_", "")
        context.user_data['editing_field'] = field

        # 📝 LOG: Field Selection
        self.db.log_event(query.from_user, "EXPENSE_ACTION", f"👇 User selected to edit: {field}")

        company = context.user_data['exp']['company']

        if field == 'gl':
            kb = [[InlineKeyboardButton("🔎 Live GL Search", switch_inline_query_current_chat=f"gl_{company} ")]]
            query.edit_message_text(f"📂 **Select New GL Account:**\nUse the button below:",
                                    reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        elif field == 'cc':
            kb = [[InlineKeyboardButton("🔎 Live CC Search", switch_inline_query_current_chat=f"cc_{company} ")]]
            query.edit_message_text(f"🏢 **Select New Cost Center:**\nUse the button below:",
                                    reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        elif field == 'tax':
            kb = [[InlineKeyboardButton("🔎 Live Tax Search", switch_inline_query_current_chat=f"tax_{company} ")]]
            query.edit_message_text(f"🧾 **Select New Tax Code:**\nUse the button below:",
                                    reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        elif field in ['doc_date', 'post_date']:
            query.edit_message_text(f"📅 **Enter New Date:**\nFormat: `YYYY-MM-DD` (e.g. 2025-01-30)",
                                    parse_mode=ParseMode.MARKDOWN)
        else:
            query.edit_message_text(f"✏️ **Enter new value:**", parse_mode=ParseMode.MARKDOWN)

        return E_EDIT_VALUE

    def handle_edit_value(self, update, context):
        field = context.user_data.get('editing_field')
        new_val = None
        msg_func = update.message.reply_text

        if update.message:
            new_val = update.message.text
        elif update.message and update.message.text.startswith('/sel_'):
            parts = update.message.text.split('_')
            new_val = parts[-1].strip()

        if field and new_val:
            # --- 📝 AUDIT LOG LOGIC ---
            old_val = "N/A"
            if field in ['doc_date', 'post_date', 'ref_id']:
                old_val = context.user_data['exp'].get(field, 'N/A')
                if field in ['doc_date', 'post_date']:
                    try:
                        datetime.strptime(new_val, '%Y-%m-%d')
                        context.user_data['exp'][field] = f"{new_val}T00:00:00"
                    except ValueError:
                        update.message.reply_text("❌ Invalid Date Format. Use YYYY-MM-DD")
                        return E_EDIT_VALUE
                else:
                    context.user_data['exp']['ref_id'] = new_val
            else:
                old_val = context.user_data['temp_item'].get(field, 'N/A')
                context.user_data['temp_item'][field] = new_val

            # 📝 LOG: The Change
            log_msg = f"✏️ CHANGED {field.upper()}: From '{old_val}' TO '{new_val}'"
            self.db.log_event(update.effective_user, "EXPENSE_EDIT", log_msg)
            # ---------------------------

        return self._show_review_screen(msg_func, context)

    def execute_posting(self, update, context):
        query = update.callback_query
        query.edit_message_text("⏳ *Posting to SAP S/4HANA...*", parse_mode=ParseMode.MARKDOWN)

        # 📝 LOG
        self.db.log_event(query.from_user, "EXPENSE", "⏳ Sending to SAP...")

        if 'items' not in context.user_data['exp']:
            context.user_data['exp']['items'] = []

        if not context.user_data['exp']['items'] and 'temp_item' in context.user_data:
            context.user_data['exp']['items'].append(context.user_data['temp_item'])
        elif 'temp_item' in context.user_data and context.user_data['exp']['items']:
            context.user_data['exp']['items'][-1] = context.user_data['temp_item']

        # --- DUPLICATE DETECTION (Sprint 1) ---
        exp = context.user_data.get('exp', {})
        vendor_id = exp.get('vendor_id', '')
        ref_id = exp.get('ref', '')
        total_amt = sum(float(it.get('amount', 0)) for it in exp.get('items', []))
        if self.db.check_expense_duplicate(vendor_id, str(total_amt), '', ref_id):
            if not context.user_data.get('dup_override'):
                kb = [
                    [InlineKeyboardButton("✅ Post Anyway", callback_data="post_now"),
                     InlineKeyboardButton("❌ Cancel", callback_data="main_menu")]
                ]
                context.user_data['dup_override'] = True
                query.edit_message_text(
                    "⚠️ *Possible Duplicate Detected!*\n━━━━━━━━━━━━━━━━━━\n"
                    f"A similar expense (Vendor: `{vendor_id}`, Amount: `{total_amt}`, "
                    f"Ref: `{ref_id}`) was posted recently.\n\n"
                    "Do you want to proceed anyway?",
                    reply_markup=InlineKeyboardMarkup(kb),
                    parse_mode=ParseMode.MARKDOWN)
                return E_FINAL_REVIEW
        context.user_data.pop('dup_override', None)

        res = self.sap.create_supplier_invoice_dynamic(context.user_data['exp'])

        if res['success']:
            msg = f"✅ *Success!*\n📄 Invoice: `{res['id']}`\n📅 Year: `{res['year']}`"

            if 'photo_path' in context.user_data:
                file_path = context.user_data['photo_path']
                query.edit_message_text(f"{msg}\n\n📎 *Uploading Receipt Image...*", parse_mode=ParseMode.MARKDOWN)

                att_res = self.sap.upload_attachment(res['id'], res['year'], file_path)

                if att_res['success']:
                    msg += "\n📎 **Receipt Attached!**"
                    # 📝 LOG
                    self.db.log_event(query.from_user, "EXPENSE_ATTACH", f"Linked image to {res['id']}")
                    try:
                        os.remove(file_path)
                    except:
                        pass
                else:
                    msg += "\n⚠️ **Attachment Failed.** (Invoice created)"
                    # 📝 LOG
                    self.db.log_event(query.from_user, "EXPENSE_ERROR", f"Attachment Error: {att_res.get('error')}")

            # 📝 LOG: Final Success
            self.db.log_event(query.from_user, "EXPENSE_SUCCESS", f"Created Invoice {res['id']}")

            query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]]), parse_mode=ParseMode.MARKDOWN)
            return ConversationHandler.END
        else:
            msg = f"⚠️ *SAP Error:*\n`{res['error']}`\n\n👇 **Click 'Edit Fields' to fix:**"
            # 📝 LOG: Error
            self.db.log_event(query.from_user, "EXPENSE_ERROR", f"SAP Error: {res['error']}")

            context.user_data['exp']['items'] = []
            return self._show_review_screen(query.edit_message_text, context, custom_text=msg)

    # ... [Manual Handlers] ...

    def handle_ref(self, update, context):
        ref_id = update.message.text.strip()
        context.user_data['exp']['ref_id'] = ref_id
        # 📝 LOG
        self.db.log_event(update.effective_user, "EXPENSE_INPUT", f"Manual Ref: {ref_id}")

        today_str = datetime.now().strftime('%d-%m-%Y')
        kb = [[InlineKeyboardButton(f"📅 Today ({today_str})", callback_data=f"curr_date_doc")]]
        update.message.reply_text("📅 **Enter Document Date:**\nFormat: `DD-MM-YYYY`\nOr click button:",
                                  reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        return E_DATE_DOC

    def handle_doc_date(self, update, context):
        if update.callback_query:
            doc_date_str = datetime.now().strftime("%Y-%m-%dT00:00:00")
            update.callback_query.answer()
            msg_func = update.callback_query.edit_message_text
        else:
            try:
                date_obj = datetime.strptime(update.message.text.strip(), "%d-%m-%Y")
                doc_date_str = date_obj.strftime("%Y-%m-%dT00:00:00")
                msg_func = update.message.reply_text
            except ValueError:
                update.message.reply_text("❌ Invalid Format! Use DD-MM-YYYY")
                return E_DATE_DOC
        context.user_data['exp']['doc_date'] = doc_date_str
        # 📝 LOG
        self.db.log_event(update.effective_user, "EXPENSE_INPUT", f"Doc Date: {doc_date_str}")

        today_str = datetime.now().strftime('%d-%m-%Y')
        kb = [[InlineKeyboardButton(f"📅 Today ({today_str})", callback_data=f"curr_date_post")]]
        msg_func("📅 **Enter Posting Date:**\nFormat: `DD-MM-YYYY`\nOr click button:",
                 reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        return E_DATE_POST

    def handle_post_date(self, update, context):
        if update.callback_query:
            post_date_str = datetime.now().strftime("%Y-%m-%dT00:00:00")
            update.callback_query.answer()
            msg_func = update.callback_query.edit_message_text
        else:
            try:
                date_obj = datetime.strptime(update.message.text.strip(), "%d-%m-%Y")
                post_date_str = date_obj.strftime("%Y-%m-%dT00:00:00")
                msg_func = update.message.reply_text
            except ValueError:
                update.message.reply_text("❌ Invalid Format! Use DD-MM-YYYY")
                return E_DATE_POST
        context.user_data['exp']['post_date'] = post_date_str
        # 📝 LOG
        self.db.log_event(update.effective_user, "EXPENSE_INPUT", f"Post Date: {post_date_str}")

        curr = context.user_data['exp']['currency']
        kb = [[InlineKeyboardButton("❌ Cancel", callback_data="main_menu")]]
        msg_func(f"✅ Details Saved.\n\n💰 **Enter Amount ({curr}):**\nExample: 500.00",
                 reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        return E_AMT

    def handle_amt(self, update, context):
        try:
            amt = "{:.2f}".format(float(update.message.text))
            context.user_data['temp_item'] = {'amount': amt}
            # 📝 LOG
            self.db.log_event(update.effective_user, "EXPENSE_INPUT", f"Amount: {amt}")

            kb = [[InlineKeyboardButton("❌ Cancel", callback_data="main_menu")]]
            update.message.reply_text("📝 **Enter Description:**\nExample: 'Office Supplies'",
                                      reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
            return E_DESC
        except ValueError:
            update.message.reply_text("❌ Invalid Amount. Enter number only.")
            return E_AMT

    def handle_desc(self, update, context):
        desc = update.message.text
        context.user_data['temp_item']['desc'] = desc
        # 📝 LOG
        self.db.log_event(update.effective_user, "EXPENSE_INPUT", f"Desc: {desc}")

        company = context.user_data['exp']['company']
        kb = [[InlineKeyboardButton("🔎 Live GL Search", switch_inline_query_current_chat=f"gl_{company} ")],
              [InlineKeyboardButton("❌ Cancel", callback_data="main_menu")]]
        update.message.reply_text("📂 **Select G/L Account:**\nType Name/ID or use Live Search:",
                                  reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        return E_GL_SRCH

    def search_gl_text(self, update: Update, context: CallbackContext):
        keyword = update.message.text.strip()
        if keyword.startswith('/sel_gl_'): return self.select_gl(update, context)

        company = context.user_data.get('exp', {}).get('company', '1000')
        msg = update.message.reply_text(f"🔍 Searching GL '{keyword}' in {company}...")

        res = self.sap.search_gl_accounts_dynamic(keyword)
        context.bot.delete_message(chat_id=msg.chat_id, message_id=msg.message_id)

        if not res:
            kb = [[InlineKeyboardButton("❌ Cancel", callback_data="main_menu")]]
            update.message.reply_text(f"❌ No GL found in {company}. Try again:", reply_markup=InlineKeyboardMarkup(kb))
            return E_GL_SRCH

        kb = [[InlineKeyboardButton(f"📂 {i['name']} ({i['id']})", callback_data=f"egl_{i['id']}")] for i in res[:5]]
        kb.append([InlineKeyboardButton("❌ Cancel", callback_data="main_menu")])

        update.message.reply_text("👇 **Select G/L Account:**", reply_markup=InlineKeyboardMarkup(kb),
                                  parse_mode=ParseMode.MARKDOWN)
        return E_GL_SRCH

    def select_gl(self, update: Update, context: CallbackContext):
        if update.callback_query:
            gl_id = update.callback_query.data.split('_')[1]
            update.callback_query.answer()
            msg_func = update.callback_query.edit_message_text
        else:
            gl_id = update.message.text.replace("/sel_gl_", "").strip()
            msg_func = update.message.reply_text

        # 📝 LOG
        self.db.log_event(update.effective_user, "EXPENSE_INPUT", f"Selected GL: {gl_id}")

        if context.user_data.get('editing_field') == 'gl':
            context.user_data['temp_item']['gl'] = gl_id
            self.db.log_event(update.effective_user, "EXPENSE_EDIT", f"✏️ Edited GL to: {gl_id}")
            return self._show_review_screen(msg_func, context)

        context.user_data['temp_item']['gl'] = gl_id
        company = context.user_data['exp']['company']

        # Auto-assign Cost Center based on Company Code
        cc_map = {'1000': '10001010', '2000': '20001010'}
        context.user_data['temp_item']['cc'] = cc_map.get(company, '10001010')

        # Skip CC selection, go directly to Tax
        kb = [[InlineKeyboardButton("🔎 Live Tax Search", switch_inline_query_current_chat=f"tax_{company} ")],
              [InlineKeyboardButton("❌ Cancel", callback_data="main_menu")]]
        msg_func(f"✅ GL: `{gl_id}`\n\n🧾 **Select Tax Code:**\nType or use Live Search:",
                 reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        return E_TAX_SEL

    def search_cc_text(self, update: Update, context: CallbackContext):
        keyword = update.message.text.strip()
        if keyword.startswith('/sel_cc_'): return self.select_cc(update, context)

        company = context.user_data['exp']['company']
        msg = update.message.reply_text(f"🔍 Searching CC '{keyword}' in {company}...")
        res = self.sap.search_cost_centers_dynamic(keyword, company)
        context.bot.delete_message(chat_id=msg.chat_id, message_id=msg.message_id)
        if not res:
            kb = [[InlineKeyboardButton("❌ Cancel", callback_data="main_menu")]]
            update.message.reply_text("❌ No Cost Center found. Try again:", reply_markup=InlineKeyboardMarkup(kb))
            return E_CC_SRCH
        kb = [[InlineKeyboardButton(f"🏢 {i['name']} ({i['id']})", callback_data=f"ecc_{i['id']}")] for i in res[:5]]
        kb.append([InlineKeyboardButton("❌ Cancel", callback_data="main_menu")])
        update.message.reply_text("👇 **Select Cost Center:**", reply_markup=InlineKeyboardMarkup(kb),
                                  parse_mode=ParseMode.MARKDOWN)
        return E_CC_SRCH

    def select_cc(self, update: Update, context: CallbackContext):
        if update.callback_query:
            cc_id = update.callback_query.data.split('_')[1]
            update.callback_query.answer()
            msg_func = update.callback_query.edit_message_text
        else:
            cc_id = update.message.text.replace("/sel_cc_", "").strip()
            msg_func = update.message.reply_text

        # 📝 LOG
        self.db.log_event(update.effective_user, "EXPENSE_INPUT", f"Selected CC: {cc_id}")

        if context.user_data.get('editing_field') == 'cc':
            context.user_data['temp_item']['cc'] = cc_id
            self.db.log_event(update.effective_user, "EXPENSE_EDIT", f"✏️ Edited CC to: {cc_id}")
            return self._show_review_screen(msg_func, context)

        context.user_data['temp_item']['cc'] = cc_id
        company = context.user_data['exp']['company']
        kb = [[InlineKeyboardButton("🔎 Live Tax Search", switch_inline_query_current_chat=f"tax_{company} ")],
              [InlineKeyboardButton("❌ Cancel", callback_data="main_menu")]]
        msg_func("🧾 **Select Tax Code:**\nType or use Live Search:", reply_markup=InlineKeyboardMarkup(kb),
                 parse_mode=ParseMode.MARKDOWN)
        return E_TAX_SEL

    def search_tax_text(self, update: Update, context: CallbackContext):
        keyword = update.message.text.strip()
        if keyword.startswith('/sel_tax_'):
            return self.select_tax(update, context)
        company = context.user_data.get('exp', {}).get('company', '1000')
        msg = update.message.reply_text(f"🔍 Searching Tax Code '{keyword}' in {company}...")
        res = self.sap.search_tax_codes_dynamic(keyword, company)
        context.bot.delete_message(chat_id=msg.chat_id, message_id=msg.message_id)
        if not res:
            kb = [[InlineKeyboardButton("❌ Cancel", callback_data="main_menu")]]
            update.message.reply_text("❌ No Tax Code found. Try again:", reply_markup=InlineKeyboardMarkup(kb))
            return E_TAX_SEL
        kb = [[InlineKeyboardButton(f"🧾 {i['name']} ({i['id']})", callback_data=f"etax_{i['id']}")] for i in res[:5]]
        kb.append([InlineKeyboardButton("❌ Cancel", callback_data="main_menu")])
        update.message.reply_text("👇 **Select Tax Code:**", reply_markup=InlineKeyboardMarkup(kb),
                                  parse_mode=ParseMode.MARKDOWN)
        return E_TAX_SEL

    def select_tax(self, update, context):
        if update.callback_query:
            tax_code = update.callback_query.data.split('_')[1]
            update.callback_query.answer()
            msg_func = update.callback_query.edit_message_text
        else:
            tax_code = update.message.text.replace("/sel_tax_", "").strip()
            msg_func = update.message.reply_text

        # 📝 LOG
        self.db.log_event(update.effective_user, "EXPENSE_INPUT", f"Selected Tax: {tax_code}")

        if context.user_data.get('editing_field') == 'tax':
            context.user_data['temp_item']['tax'] = tax_code
            self.db.log_event(update.effective_user, "EXPENSE_EDIT", f"✏️ Edited Tax to: {tax_code}")
            return self._show_review_screen(msg_func, context)

        context.user_data['temp_item']['tax'] = tax_code
        context.user_data['exp']['items'].append(context.user_data['temp_item'])
        return self._show_review_screen(msg_func, context)

    def handle_add_more(self, update, context):
        query = update.callback_query
        query.answer()
        return self.execute_posting(update, context)