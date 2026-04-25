import os
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode
from telegram.ext import CallbackContext, ConversationHandler
from datetime import datetime
from ai_handler import AIHandler
from states import (
    EW_INPUT_METHOD, EW_SCAN_PHOTO, EW_REF, EW_DATE_DOC, EW_DATE_POST,
    EW_AMT, EW_DESC, EW_GL_SRCH, EW_CC_SRCH, EW_TAX_SEL,
    EW_FINAL_REVIEW, EW_EDIT_MENU, EW_EDIT_VALUE,
)
from config import PETTY_CASH_GL
from logger_setup import get_logger

logger = get_logger(__name__)


class ExpenseWithoutVendorFeature:
    def __init__(self, sap_handler, db_handler):
        self.sap = sap_handler
        self.db = db_handler
        self.ai = AIHandler()

    def handle_input_method(self, update, context):
        query = update.callback_query
        query.answer()

        method = "Scan (AI)" if query.data == "ew_input_scan" else "Manual Entry"
        self.db.log_event(update.effective_user, "EXPENSE_WV_ACTION", f"Selected Input Method: {method}")

        if query.data == "ew_input_manual":
            kb = [[InlineKeyboardButton("❌ Cancel", callback_data="main_menu")]]
            query.edit_message_text("🆔 **Enter Reference / Title:**\nExample: `Cash Purchase`",
                                    reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
            return EW_REF
        elif query.data == "ew_input_scan":
            query.edit_message_text("📸 **Please upload the Receipt Photo.**\n\n(Send it as 'Photo', not 'File')",
                                    parse_mode=ParseMode.MARKDOWN)
            return EW_SCAN_PHOTO

    def handle_photo_scan(self, update, context):
        user = update.effective_user
        try:
            if not update.message.photo:
                update.message.reply_text("⚠️ Please send a PHOTO, not a file.")
                return EW_SCAN_PHOTO

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
                context.bot.send_message(chat_id=update.effective_chat.id, text="🆔 **Enter Reference / Title:**")
                return EW_REF

            data = result['data']
            self.db.log_event(user, "AI_SCAN_WV", f"Extracted Data: {data}")

            vendor_name = data.get('vendor_name', '')
            ref_id = vendor_name[:16].upper() if vendor_name else f"CASH-{int(datetime.now().timestamp())}"
            context.user_data['exp']['ref_id'] = ref_id

            raw_date = data.get('date', datetime.now().strftime('%Y-%m-%d'))
            context.user_data['exp']['doc_date'] = f"{raw_date}T00:00:00"
            context.user_data['exp']['post_date'] = f"{datetime.now().strftime('%Y-%m-%d')}T00:00:00"

            company = context.user_data['exp']['company']

            # Auto-set GL, Tax, and Cost Center
            selected_gl = PETTY_CASH_GL
            selected_tax = "V1" if company == '1000' else "I1"
            cc_map = {'1000': '10001010', '2000': '20001010'}
            selected_cc = cc_map.get(company, '10001010')

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
            update.message.reply_text("⚠️ System Error. Try Manual.")
            return EW_REF

    def _show_review_screen(self, msg_func, context, custom_text=None):
        header = context.user_data['exp']
        item = context.user_data.get('temp_item', {})

        d_doc = header.get('doc_date', '').split('T')[0]
        d_post = header.get('post_date', '').split('T')[0]

        intro = custom_text if custom_text else "📋 **Expense Claim Summary**"

        txt = (
            f"{intro}\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📅 **Doc Date:** `{d_doc}`\n"
            f"📅 **Post Date:** `{d_post}`\n"
            f"🆔 **Text/Ref:** `{header.get('ref_id')}`\n"
            f"────────────────\n"
            f"💰 **Gross Amount:** `{item.get('amount')} {header.get('currency')}`\n"
            f"📝 **Expense Title:** {item.get('desc')}\n"
            f"🏢 **Cost Center:** `{item.get('cc')}`\n"
            f"📂 **Expense GL:** `{item.get('gl')}` *(Auto)*\n"
            f"🧾 **Tax Code:** `{item.get('tax')}` *(Auto)*\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"✅ **Check details above.**\nClick 'Edit' to change anything."
        )

        kb = [
            [InlineKeyboardButton("✅ File Expense Claim", callback_data="ew_post_now")],
            [InlineKeyboardButton("✏️ Edit Fields", callback_data="ew_edit_menu")],
            [InlineKeyboardButton("❌ Discard", callback_data="main_menu")]
        ]
        msg_func(txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        return EW_FINAL_REVIEW

    def handle_final_review(self, update, context):
        query = update.callback_query
        query.answer()

        if query.data == "ew_post_now":
            return self.execute_posting(update, context)

        elif query.data == "ew_edit_menu":
            kb = [
                [InlineKeyboardButton("📅 Doc Date", callback_data="ew_edit_field_doc_date"),
                 InlineKeyboardButton("📅 Post Date", callback_data="ew_edit_field_post_date")],
                [InlineKeyboardButton("💰 Amount", callback_data="ew_edit_field_amount"),
                 InlineKeyboardButton("📝 Expense Title", callback_data="ew_edit_field_desc")],
                [InlineKeyboardButton("🆔 Ref ID", callback_data="ew_edit_field_ref")],
                [InlineKeyboardButton("🔙 Back to Summary", callback_data="ew_back_to_review")]
            ]
            query.edit_message_text("✏️ **Select Field to Edit:**", reply_markup=InlineKeyboardMarkup(kb),
                                    parse_mode=ParseMode.MARKDOWN)
            return EW_EDIT_MENU

    def handle_edit_menu(self, update, context):
        query = update.callback_query
        query.answer()
        data = query.data

        if data == "ew_back_to_review":
            return self._show_review_screen(query.edit_message_text, context)

        field = data.replace("ew_edit_field_", "")
        context.user_data['editing_field'] = field
        company = context.user_data['exp']['company']

        if field == 'cc':
            kb = [[InlineKeyboardButton("🔎 Live CC Search", switch_inline_query_current_chat=f"cc_{company} ")]]
            query.edit_message_text(f"🏢 **Select New Cost Center:**", reply_markup=InlineKeyboardMarkup(kb),
                                    parse_mode=ParseMode.MARKDOWN)
        elif field in ['doc_date', 'post_date']:
            query.edit_message_text(f"📅 **Enter New Date:**\nFormat: `YYYY-MM-DD`", parse_mode=ParseMode.MARKDOWN)
        else:
            query.edit_message_text(f"✏️ **Enter new value:**", parse_mode=ParseMode.MARKDOWN)

        return EW_EDIT_VALUE

    def handle_edit_value(self, update, context):
        field = context.user_data.get('editing_field')
        new_val = None
        msg_func = update.message.reply_text

        if update.message:
            new_val = update.message.text
        elif update.message and update.message.text.startswith('/sel_'):
            new_val = update.message.text.split('_')[-1].strip()

        if field and new_val:
            if field in ['doc_date', 'post_date', 'ref_id']:
                if field in ['doc_date', 'post_date']:
                    try:
                        datetime.strptime(new_val, '%Y-%m-%d')
                        context.user_data['exp'][field] = f"{new_val}T00:00:00"
                    except ValueError:
                        update.message.reply_text("❌ Invalid Date Format. Use YYYY-MM-DD")
                        return EW_EDIT_VALUE
                else:
                    context.user_data['exp']['ref_id'] = new_val
            else:
                context.user_data['temp_item'][field] = new_val

        return self._show_review_screen(msg_func, context)

    def execute_posting(self, update, context):
        query = update.callback_query
        query.edit_message_text("⏳ *Filing Expense Claim in SAP...*", parse_mode=ParseMode.MARKDOWN)

        if 'items' not in context.user_data['exp']: context.user_data['exp']['items'] = []
        if not context.user_data['exp']['items'] and 'temp_item' in context.user_data:
            context.user_data['exp']['items'].append(context.user_data['temp_item'])
        elif 'temp_item' in context.user_data and context.user_data['exp']['items']:
            context.user_data['exp']['items'][-1] = context.user_data['temp_item']

        # --- PETTY CASH LIMIT CHECK (Sprint 2) ---
        limits = self.db.get_petty_cash_limits()
        if limits:
            total_amt = sum(float(it.get('amount', 0)) for it in context.user_data['exp'].get('items', []))
            daily_used = self.db.get_daily_expense_total()
            monthly_used = self.db.get_monthly_expense_total()

            if daily_used + total_amt > limits['daily']:
                query.edit_message_text(
                    f"🚫 *Daily Petty Cash Limit Exceeded!*\n━━━━━━━━━━━━━━━━━━\n"
                    f"📅 Daily Limit: `{limits['daily']:,.2f}`\n"
                    f"📊 Used Today: `{daily_used:,.2f}`\n"
                    f"💰 This Claim: `{total_amt:,.2f}`\n\n"
                    f"Contact your manager to increase the limit.",
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]]),
                    parse_mode=ParseMode.MARKDOWN)
                return EW_FINAL_REVIEW

            if monthly_used + total_amt > limits['monthly']:
                query.edit_message_text(
                    f"🚫 *Monthly Petty Cash Limit Exceeded!*\n━━━━━━━━━━━━━━━━━━\n"
                    f"📅 Monthly Limit: `{limits['monthly']:,.2f}`\n"
                    f"📊 Used This Month: `{monthly_used:,.2f}`\n"
                    f"💰 This Claim: `{total_amt:,.2f}`\n\n"
                    f"Contact your manager to increase the limit.",
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]]),
                    parse_mode=ParseMode.MARKDOWN)
                return EW_FINAL_REVIEW

        # SAP API Hit
        res = self.sap.create_journal_entry_without_vendor(context.user_data['exp'])

        if res['success']:
            msg = f"✅ *Expense Claim Filed Successfully!*\n📄 Document: `{res['id']}`\n📅 Year: `{res['year']}`"

            # Log petty cash amount for limit tracking
            total_amt = sum(float(it.get('amount', 0)) for it in context.user_data['exp'].get('items', []))
            self.db.log_event(query.from_user, "PETTY_CASH_POSTED", str(total_amt))

            if 'photo_path' in context.user_data:
                try:
                    os.remove(context.user_data['photo_path'])
                except Exception:
                    pass

            query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]]), parse_mode=ParseMode.MARKDOWN)
            return ConversationHandler.END
        else:
            msg = f"⚠️ *SAP Error:*\n`{res['error']}`\n\n👇 **Click 'Edit Fields' to fix:**"
            context.user_data['exp']['items'] = []
            return self._show_review_screen(query.edit_message_text, context, custom_text=msg)

    # --- Manual Entry Handlers ---
    def handle_ref(self, update, context):
        context.user_data['exp']['ref_id'] = update.message.text.strip()
        today_str = datetime.now().strftime('%d-%m-%Y')
        kb = [[InlineKeyboardButton(f"📅 Today ({today_str})", callback_data=f"ew_curr_date_doc")]]
        update.message.reply_text("📅 **Enter Document Date:**\nFormat: `DD-MM-YYYY`\nOr click button:",
                                  reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        return EW_DATE_DOC

    def handle_doc_date(self, update, context):
        if update.callback_query:
            doc_date_str = datetime.now().strftime("%Y-%m-%dT00:00:00")
            update.callback_query.answer()
            msg_func = update.callback_query.edit_message_text
        else:
            try:
                doc_date_str = datetime.strptime(update.message.text.strip(), "%d-%m-%Y").strftime("%Y-%m-%dT00:00:00")
                msg_func = update.message.reply_text
            except ValueError:
                update.message.reply_text("❌ Invalid Format! Use DD-MM-YYYY")
                return EW_DATE_DOC
        context.user_data['exp']['doc_date'] = doc_date_str

        today_str = datetime.now().strftime('%d-%m-%Y')
        kb = [[InlineKeyboardButton(f"📅 Today ({today_str})", callback_data=f"ew_curr_date_post")]]
        msg_func("📅 **Enter Posting Date:**\nFormat: `DD-MM-YYYY`\nOr click button:",
                 reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        return EW_DATE_POST

    def handle_post_date(self, update, context):
        if update.callback_query:
            post_date_str = datetime.now().strftime("%Y-%m-%dT00:00:00")
            update.callback_query.answer()
            msg_func = update.callback_query.edit_message_text
        else:
            try:
                post_date_str = datetime.strptime(update.message.text.strip(), "%d-%m-%Y").strftime("%Y-%m-%dT00:00:00")
                msg_func = update.message.reply_text
            except ValueError:
                update.message.reply_text("❌ Invalid Format! Use DD-MM-YYYY")
                return EW_DATE_POST
        context.user_data['exp']['post_date'] = post_date_str

        curr = context.user_data['exp']['currency']
        msg_func(f"💰 **Enter Gross Amount ({curr}):**\nExample: 100.00", parse_mode=ParseMode.MARKDOWN)
        return EW_AMT

    def handle_amt(self, update, context):
        try:
            amt = "{:.2f}".format(float(update.message.text))
            context.user_data['temp_item'] = {'amount': amt}
            update.message.reply_text("📝 **Enter Expense Title / Text:**\nExample: 'Office Supplies'",
                                      parse_mode=ParseMode.MARKDOWN)
            return EW_DESC
        except ValueError:
            update.message.reply_text("❌ Invalid Amount. Enter number only.")
            return EW_AMT

    def handle_desc(self, update, context):
        context.user_data['temp_item']['desc'] = update.message.text
        company = context.user_data['exp']['company']

        # Auto-set GL, Tax, and Cost Center (all hardcoded)
        context.user_data['temp_item']['gl'] = PETTY_CASH_GL
        context.user_data['temp_item']['tax'] = "V1" if company == '1000' else "I1"
        cc_map = {'1000': '10001010', '2000': '20001010'}
        context.user_data['temp_item']['cc'] = cc_map.get(company, '10001010')

        # Skip CC selection, go directly to review
        return self._show_review_screen(update.message.reply_text, context)

    def search_cc_text(self, update: Update, context: CallbackContext):
        keyword = update.message.text.strip()
        if keyword.startswith('/sel_cc_'): return self.select_cc(update, context)
        company = context.user_data['exp']['company']
        msg = update.message.reply_text(f"🔍 Searching CC...")
        res = self.sap.search_cost_centers_dynamic(keyword, company)
        context.bot.delete_message(chat_id=msg.chat_id, message_id=msg.message_id)
        if not res: return EW_CC_SRCH
        kb = [[InlineKeyboardButton(f"🏢 {i['name']} ({i['id']})", callback_data=f"ew_ecc_{i['id']}")] for i in res[:5]]
        update.message.reply_text("👇 **Select Cost Center:**", reply_markup=InlineKeyboardMarkup(kb),
                                  parse_mode=ParseMode.MARKDOWN)
        return EW_CC_SRCH

    def select_cc(self, update: Update, context: CallbackContext):
        cc_id = update.callback_query.data.split('_')[2] if update.callback_query else update.message.text.replace(
            "/sel_cc_", "").strip()
        if update.callback_query: update.callback_query.answer()
        msg_func = update.callback_query.edit_message_text if update.callback_query else update.message.reply_text
        context.user_data['temp_item']['cc'] = cc_id

        # Go directly to review screen (Tax selection skipped)
        return self._show_review_screen(msg_func, context)

    # Stub functions required by bot.py handler registration
    def search_gl_text(self, update, context):
        pass

    def select_gl(self, update, context):
        pass

    def select_tax(self, update, context):
        pass