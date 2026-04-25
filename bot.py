import requests
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Updater, CommandHandler, CallbackQueryHandler, MessageHandler,
    Filters, CallbackContext, ConversationHandler, InlineQueryHandler, TypeHandler,
)
import gc
from config import SALES_BOT_TOKEN, MANAGER_BOT_TOKEN, PRIVACY_POLICY_VERSION, OTP_EXPIRY_SECONDS
from sap_handler import SAPHandler
from db_helper import DatabaseHandler
from notification_feature import NotificationFeature
from manager_feature import ManagerFeature
from approval_server import start_background_server
from material_feature import MaterialFeature
from credit_feature import CreditFeature
from customer_feature import CustomerFeature
from price_feature import PriceFeature
from sales_feature import SalesFeature
from order_feature import OrderFeature
from dashboard_feature import DashboardFeature
from expense_feature import ExpenseFeature
from expense_without_vendor_feature import ExpenseWithoutVendorFeature
from customer360_feature import Customer360Feature
from analytics_feature import AnalyticsFeature
from copilot_feature import CopilotFeature
from security_middleware import SecurityManager, mask_email
from b2b_secure_handler import B2BSecureSAPHandler
from logger_setup import setup_logging, get_logger
from states import (
    MAIN_MENU,
    CUSTOMER_MENU, CUSTOMER_SEARCH_INPUT, CUSTOMER_CREATE_CATEGORY,
    CUSTOMER_CREATE_NAME, CUSTOMER_CREATE_COUNTRY, CUSTOMER_CREATE_REGION,
    CUSTOMER_CREATE_CITY, CUSTOMER_CREATE_STREET, CUSTOMER_CREATE_POSTAL,
    CUSTOMER_CREATE_MOBILE, CUSTOMER_CREATE_EMAIL, CUSTOMER_CREATE_CONFIRM,
    DASH_SELECT_PERIOD, DASH_ASK_START, DASH_ASK_END, DASH_ASK_TARGET,
    DASH_SHOW_REPORT, DASH_SELECT_STATUS_FILTER,
    DASH_COMPARE_START, DASH_COMPARE_END,
    ORDER_ASK_DISCOUNT, ORDER_ASK_QUOTE_ID, ORDER_ASK_VALIDITY,
    ORDER_ASK_CUSTOMER, ORDER_ASK_REF, ORDER_ASK_MATERIAL, ORDER_ASK_QTY,
    ORDER_ADD_MORE, ORDER_CONFIRM, ORDER_REMOVE_ITEM,
    SALES_MENU, SALES_SEARCH_INPUT,
    MATERIAL_SEARCH_INPUT, PRODUCT_SEARCH_INPUT,
    E_CO, E_VENDOR_TYPE, E_V_SRCH, E_V_CONF, E_INPUT_METHOD, E_SCAN_PHOTO,
    E_REF, E_DATE_DOC, E_DATE_POST, E_AMT, E_DESC, E_GL_SRCH, E_CC_SRCH,
    E_TAX_SEL, E_ADD_MORE, E_FINAL_REVIEW, E_EDIT_MENU, E_EDIT_VALUE,
    EW_INPUT_METHOD, EW_SCAN_PHOTO, EW_REF, EW_DATE_DOC, EW_DATE_POST,
    EW_AMT, EW_DESC, EW_GL_SRCH, EW_CC_SRCH, EW_TAX_SEL,
    EW_FINAL_REVIEW, EW_EDIT_MENU, EW_EDIT_VALUE,
    MANAGER_REASON_INPUT, MANAGER_CONFIRM_REJECT,
    MGR_TARGET_PERIOD, MGR_TARGET_START, MGR_TARGET_END, MGR_TARGET_AMOUNT,
    MGR_USER_LIST, MGR_USER_PERMISSIONS,
    CUSTOMER_360, MGR_PETTY_CASH_MENU, MGR_PETTY_CASH_SET,
    FAVORITES_MENU, COPILOT_CONFIRM,
    OTP_ASK_BP_ID, OTP_ASK_CODE,
    GDPR_CONSENT,
)

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Default: enable protect_content on all bot replies (DLP)
# ---------------------------------------------------------------------------
PROTECT_CONTENT = True


class SAPSalesBot:
    def __init__(self):
        self.db = DatabaseHandler()
        self.sap = SAPHandler()
        self.security = SecurityManager(self.db)
        self.notify_ui = NotificationFeature(self.sap, self.db)
        self.mgr = ManagerFeature(self.db)

        self.cust = CustomerFeature(self.sap, self.db)
        self.price = PriceFeature(self.sap, self.db)
        self.sales = SalesFeature(self.sap, self.db)
        self.order = OrderFeature(self.sap, self.db)
        self.dash = DashboardFeature(self.sap, self.db)
        self.credit = CreditFeature(self.sap, self.db)
        self.material = MaterialFeature(self.sap, self.db)
        self.expense = ExpenseFeature(self.sap, self.db)
        self.expense_wv = ExpenseWithoutVendorFeature(self.sap, self.db)
        self.cust360 = Customer360Feature(self.sap, self.db)
        self.analytics = AnalyticsFeature(self.db)
        self.copilot = CopilotFeature(self.sap, self.db)

    # ------------------------------------------------------------------
    # OTP Authentication Handlers
    # ------------------------------------------------------------------
    def handle_bp_id_input(self, update: Update, context: CallbackContext):
        """Handle user entering their SAP Business Partner / Customer ID."""
        user = update.effective_user
        raw_input = update.message.text.strip()

        # Validate: must be numeric
        if not raw_input.isdigit():
            update.message.reply_text(
                "❌ Invalid ID. Please enter a *numeric* SAP Customer ID.\n\n"
                "Example: `100` or `1000025`",
                parse_mode='Markdown')
            return OTP_ASK_BP_ID

        # Auto-pad to 10 digits (SAP stores BP IDs with leading zeros)
        bp_id = raw_input.zfill(10)

        update.message.reply_text("🔄 *Verifying ID in SAP S/4HANA...*", parse_mode='Markdown')

        # Look up BP in SAP and fetch email
        bp_data = self.sap.get_customer_details(bp_id)
        if not bp_data:
            update.message.reply_text(
                f"❌ *Customer ID `{raw_input}` not found in SAP.*\n\n"
                "Please check your ID and try again.",
                parse_mode='Markdown')
            return OTP_ASK_BP_ID

        email = bp_data.get('Email')
        if not email or email == 'N/A' or '@' not in str(email):
            update.message.reply_text(
                f"⚠️ *No email address registered* for Customer `{raw_input}` in SAP.\n\n"
                "Please contact your administrator to add an email to your Business Partner record.",
                parse_mode='Markdown')
            return OTP_ASK_BP_ID

        # Generate and send OTP
        otp = self.security.create_otp_challenge(user.id, bp_id, email)
        email_sent = self.security.send_otp_email(email, otp, bp_id)

        if not email_sent:
            update.message.reply_text(
                "❌ *Failed to send OTP email.*\n\n"
                "Please try again later or contact your administrator.",
                parse_mode='Markdown')
            return OTP_ASK_BP_ID

        masked = mask_email(email)
        context.user_data['otp_bp_id'] = bp_id
        context.user_data['otp_bp_name'] = bp_data.get('Name', bp_id)

        update.message.reply_text(
            f"✅ *Customer Found:* {bp_data.get('Name', 'N/A')}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📧 A 6-digit OTP has been sent to:\n`{masked}`\n\n"
            f"⏱ The OTP expires in *{OTP_EXPIRY_SECONDS // 60} minutes*.\n\n"
            f"👇 *Enter the OTP below:*",
            parse_mode='Markdown')
        return OTP_ASK_CODE

    def handle_otp_input(self, update: Update, context: CallbackContext):
        """Handle user entering the 6-digit OTP."""
        user = update.effective_user
        entered_otp = update.message.text.strip()

        # Basic format check
        if not entered_otp.isdigit() or len(entered_otp) != 6:
            update.message.reply_text(
                "❌ Please enter a valid *6-digit* OTP.",
                parse_mode='Markdown')
            return OTP_ASK_CODE

        result = self.security.verify_otp(user.id, entered_otp)

        if result['success']:
            bp_name = context.user_data.get('otp_bp_name', result['bp_id'])
            # Register user in the system
            self.db.register_user(user.id, user.username, user.first_name)

            update.message.reply_text(
                f"✅ *Login Successful!*\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"👤 Welcome, *{bp_name}*\n"
                f"🆔 BP: `{result['bp_id']}`\n\n"
                f"Your session will expire after 8 hours of inactivity.\n"
                f"Use /logout to end your session.\n\n"
                f"Sending you to the main menu...",
                parse_mode='Markdown')

            # Clean up OTP temp data
            context.user_data.pop('otp_bp_id', None)
            context.user_data.pop('otp_bp_name', None)

            # Show main menu
            return self.start(update, context)

        # Handle failures
        reason = result.get('reason')
        if reason == 'expired':
            update.message.reply_text(
                "⏱ *OTP has expired.*\n\n"
                "Send /start to request a new one.",
                parse_mode='Markdown')
            return ConversationHandler.END
        elif reason == 'max_attempts':
            update.message.reply_text(
                "🚫 *Too many wrong attempts.* OTP has been invalidated.\n\n"
                "Send /start to request a new OTP.",
                parse_mode='Markdown')
            return ConversationHandler.END
        else:
            remaining = result.get('remaining', 0)
            update.message.reply_text(
                f"❌ *Wrong OTP.* You have *{remaining}* attempt(s) remaining.\n\n"
                f"Please try again:",
                parse_mode='Markdown')
            return OTP_ASK_CODE

    def handle_logout(self, update: Update, context: CallbackContext):
        """Handle /logout command — destroy session and flush data."""
        user = update.effective_user
        if user:
            self.security.destroy_session(user.id)
            self.security.flush_user_data(context, user.id)
        update.message.reply_text(
            "🔒 *Session Ended*\n\n"
            "All cached data has been securely cleared.\n"
            "Send /start to log in again.",
            parse_mode='Markdown')

    # ------------------------------------------------------------------
    # GDPR Consent Handler
    # ------------------------------------------------------------------
    def handle_gdpr_consent(self, update: Update, context: CallbackContext):
        """Handle privacy policy accept/decline callback."""
        query = update.callback_query
        query.answer()
        user = update.effective_user

        if query.data == 'gdpr_accept':
            self.db.record_consent(user.id, PRIVACY_POLICY_VERSION)
            self.db.log_event(user, "GDPR_CONSENT_ACCEPTED",
                              f"Policy v{PRIVACY_POLICY_VERSION}")
            self.security._log_security_event(
                user.id, "GDPR_CONSENT", f"Accepted policy v{PRIVACY_POLICY_VERSION}")

            # After accepting GDPR, route to OTP login
            if not self.security.is_session_valid(user.id):
                query.edit_message_text(
                    "✅ *Privacy Policy Accepted!*\n\n"
                    "Now let's log you in.\n"
                    "Please enter your *SAP Customer ID* (Business Partner ID).\n\n"
                    "Example: `100` or `1000025`\n\n"
                    "👇 *Type your ID below:*",
                    parse_mode='Markdown')
                return OTP_ASK_BP_ID

            # User is already authenticated — send to main menu
            query.edit_message_text(
                "✅ *Privacy Policy Accepted!*\n\n"
                "Send /start to open the main menu.",
                parse_mode='Markdown')
            return ConversationHandler.END
        else:
            query.edit_message_text(
                "⛔ *Access Denied*\n\n"
                "You must accept the Privacy Policy to use this bot.\n"
                "Send /start to try again.",
                parse_mode='Markdown')
            self.db.log_event(user, "GDPR_CONSENT_DECLINED",
                              f"Policy v{PRIVACY_POLICY_VERSION}")
            return ConversationHandler.END

    # ------------------------------------------------------------------
    # Security Gate: check auth before allowing SAP operations
    # ------------------------------------------------------------------
    def _security_gate(self, update: Update, context: CallbackContext) -> bool:
        """Returns True if user is authenticated via OTP. Sends login prompt if not."""
        user = update.effective_user
        if not user:
            return True  # System/internal calls

        # Rate limit check
        if not self.security.check_rate_limit(user.id):
            msg = "Too many requests. Please wait a moment."
            if update.callback_query:
                update.callback_query.answer(msg, show_alert=True)
            elif update.message:
                update.message.reply_text(msg)
            return False

        # Check valid session (OTP-based)
        if not self.security.is_session_valid(user.id):
            msg = ("🔒 *Session expired or not logged in.*\n\n"
                   "Send /start to authenticate with your SAP Customer ID.")
            if update.callback_query:
                update.callback_query.answer("Session expired. Send /start to login.", show_alert=True)
            elif update.message:
                update.message.reply_text(msg, parse_mode='Markdown')
            return False

        return True

    # ------------------------------------------------------------------
    # B2B Customer Handlers
    # ------------------------------------------------------------------
    def _b2b_my_orders(self, update: Update, context: CallbackContext):
        """B2B: View own orders only (via proxy)."""
        query = update.callback_query
        query.answer()
        user = update.effective_user
        sap_proxy, b2b_bp = self._get_sap_for_user(user.id)
        if not b2b_bp:
            return self.sales.view_orders(update, context)

        page = int(query.data.split('_')[-1])
        query.edit_message_text("⏳ *Fetching your orders from SAP...*", parse_mode='Markdown')

        orders = sap_proxy.get_sales_orders(skip=page * 5, top=5)
        kb = []
        txt = f"📦 *My Orders* (Page {page + 1})\n━━━━━━━━━━━━━━━━━━\n"
        if orders:
            for o in orders:
                btn_text = f"📦 {o['SalesOrder']}  |  {o['TotalNetAmount']} {o['TransactionCurrency']}"
                kb.append([InlineKeyboardButton(btn_text, callback_data=f"b2b_order_detail_{o['SalesOrder']}")])
            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"b2b_my_orders_{page - 1}"))
            if len(orders) == 5:
                nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"b2b_my_orders_{page + 1}"))
            if nav:
                kb.append(nav)
        else:
            txt += "_No orders found._\n"
        kb.append([InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")])
        try:
            query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')
        except Exception:
            pass
        return MAIN_MENU

    def _b2b_invoice_menu(self, update: Update, context: CallbackContext):
        """B2B: Invoice entry menu — no SAP fetch, just routes to Pending or Completed."""
        query = update.callback_query
        query.answer()
        txt = ("🧾 *Invoice Management*\n\n"
               "Please select the type of invoices you want to view:")
        kb = [
            [InlineKeyboardButton("⏳ Pending Invoices",
                                  callback_data="b2b_my_invoices_pending_0")],
            [InlineKeyboardButton("✅ Completed Invoices",
                                  callback_data="b2b_my_invoices_completed_0")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")],
        ]
        try:
            query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')
        except Exception:
            pass
        return MAIN_MENU

    def _b2b_my_invoices(self, update: Update, context: CallbackContext):
        """B2B: Tabbed invoice list — Pending vs Completed (via proxy)."""
        query = update.callback_query
        query.answer()
        user = update.effective_user
        sap_proxy, b2b_bp = self._get_sap_for_user(user.id)
        if not b2b_bp:
            query.edit_message_text("❌ This view is for B2B customers only.",
                                    reply_markup=InlineKeyboardMarkup(
                                        [[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]]))
            return MAIN_MENU

        # Callback shape: b2b_my_invoices_<tab>_<page>  (tab in {'pending','completed'})
        # Legacy shape b2b_my_invoices_<page> falls back to 'pending'.
        parts = query.data.split('_')
        try:
            page = int(parts[-1])
        except ValueError:
            page = 0
        tab = parts[-2] if parts[-2] in ('pending', 'completed') else 'pending'

        query.edit_message_text("⏳ *Fetching your invoices from SAP...*", parse_mode='Markdown')
        invoices = sap_proxy.get_customer_invoices(skip=page * 5, top=5, status_filter=tab)

        title_emoji, title_word = ("⏳", "Pending") if tab == 'pending' else ("✅", "Completed")
        txt = f"🧾 *{title_emoji} {title_word} Invoices* (Page {page + 1})\n━━━━━━━━━━━━━━━━━━\n"

        kb = [[
            InlineKeyboardButton(
                ("• ⏳ Pending •" if tab == 'pending' else "⏳ Pending"),
                callback_data="b2b_my_invoices_pending_0"),
            InlineKeyboardButton(
                ("• ✅ Completed •" if tab == 'completed' else "✅ Completed"),
                callback_data="b2b_my_invoices_completed_0"),
        ]]

        if invoices:
            for inv in invoices:
                date_fmt = inv.get('BillingDocumentDate_fmt', 'N/A')
                btn_text = (f"🧾 {inv['BillingDocument']}  |  "
                            f"{inv['TotalNetAmount']} {inv['TransactionCurrency']}  |  {date_fmt}")
                kb.append([InlineKeyboardButton(
                    btn_text,
                    callback_data=f"b2b_inv_detail_{tab}_{inv['BillingDocument']}")])
            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton(
                    "⬅️ Prev", callback_data=f"b2b_my_invoices_{tab}_{page - 1}"))
            if len(invoices) == 5:
                nav.append(InlineKeyboardButton(
                    "Next ➡️", callback_data=f"b2b_my_invoices_{tab}_{page + 1}"))
            if nav:
                kb.append(nav)
        else:
            txt += f"\n_No {title_word.lower()} invoices found._\n"

        kb.append([InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")])
        try:
            query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')
        except Exception:
            pass
        return MAIN_MENU

    def _b2b_invoice_detail(self, update: Update, context: CallbackContext):
        """B2B: Detailed view of a single invoice (ownership validated by proxy)."""
        query = update.callback_query
        query.answer()
        user = update.effective_user
        sap_proxy, b2b_bp = self._get_sap_for_user(user.id)
        if not b2b_bp:
            query.edit_message_text("❌ This view is for B2B customers only.",
                                    reply_markup=InlineKeyboardMarkup(
                                        [[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]]))
            return MAIN_MENU

        # Callback: b2b_inv_detail_<tab>_<invoice_no>
        parts = query.data.split('_')
        invoice_no = parts[-1]
        tab = parts[-2] if parts[-2] in ('pending', 'completed') else 'pending'

        query.edit_message_text("⏳ *Loading invoice details...*", parse_mode='Markdown')
        inv = sap_proxy.get_invoice_details(invoice_no)

        back_kb = [[InlineKeyboardButton("⬅️ Back to Invoices",
                                         callback_data=f"b2b_my_invoices_{tab}_0")]]
        if not inv:
            query.edit_message_text(
                "❌ *Invoice not found or access denied.*",
                reply_markup=InlineKeyboardMarkup(back_kb), parse_mode='Markdown')
            return MAIN_MENU

        clearing = inv.get('InvoiceClearingStatus')
        billing_status = inv.get('OverallBillingStatus')
        if clearing == 'C':
            status_line = "✅ *Paid*"
        elif billing_status == 'A':
            status_line = "🔴 *Unpaid*"
        else:
            status_line = "⏳ *Pending*"

        currency = inv.get('TransactionCurrency', '')
        txt = (
            f"🧾 *Invoice Details*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"{status_line}\n\n"
            f"📄 *Invoice No:* `{inv.get('BillingDocument', 'N/A')}`\n"
            f"📅 *Date:* {inv.get('BillingDocumentDate_fmt', 'N/A')}\n"
            f"🗂 *Doc Type:* `{inv.get('BillingDocumentType', 'N/A')}`\n\n"
            f"💰 *Base Amount:* {inv.get('TotalNetAmount', '0.00')} {currency}\n"
            f"🏛 *Tax:* {inv.get('TaxAmount', '0.00')} {currency}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"💵 *Grand Total:* *{inv.get('TotalGrossAmount', '0.00')} {currency}*\n\n"
            f"💳 *Payment Terms:* `{inv.get('CustomerPaymentTerms', 'N/A')}`\n"
        )

        kb = [
            [InlineKeyboardButton("⬇️ Download PDF",
                                  callback_data=f"b2b_inv_pdf_{invoice_no}")],
            [InlineKeyboardButton("⬅️ Back to Invoices",
                                  callback_data=f"b2b_my_invoices_{tab}_0"),
             InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")],
        ]
        try:
            query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')
        except Exception:
            pass
        return MAIN_MENU

    def _b2b_inv_pdf_mock(self, update: Update, context: CallbackContext):
        """B2B: Placeholder for invoice PDF download."""
        query = update.callback_query
        query.answer("📄 PDF download coming soon!", show_alert=True)
        return MAIN_MENU

    def _b2b_statement_of_accounts(self, update: Update, context: CallbackContext):
        """B2B: Statement of Accounts — aggregated client-side from recent invoices."""
        query = update.callback_query
        query.answer()
        user = update.effective_user
        sap_proxy, b2b_bp = self._get_sap_for_user(user.id)
        if not b2b_bp:
            query.edit_message_text("❌ This view is for B2B customers only.",
                                    reply_markup=InlineKeyboardMarkup(
                                        [[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]]))
            return MAIN_MENU

        query.edit_message_text("⏳ *Calculating your account statement...*", parse_mode='Markdown')

        invoices = sap_proxy.get_customer_invoices(skip=0, top=100)

        total_billed = 0.0
        total_outstanding = 0.0
        total_cleared = 0.0
        currency = 'EUR'

        for inv in invoices:
            gross = float(inv.get('TotalGrossAmount', '0'))
            total_billed += gross
            if inv.get('OverallBillingStatus') == 'A':
                total_outstanding += gross
            if inv.get('InvoiceClearingStatus') == 'C':
                total_cleared += gross
            cur = inv.get('TransactionCurrency')
            if cur:
                currency = cur

        bp_disp = b2b_bp.lstrip('0')
        txt = (
            f"📊 *STATEMENT OF ACCOUNTS*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"👤 Customer ID: `{bp_disp}`\n"
            f"📅 Based on last {len(invoices)} invoice(s)\n\n"
            f"🔴 *Total Outstanding:* `{total_outstanding:,.2f}` {currency}\n"
            f"🟢 *Total Cleared/Paid:* `{total_cleared:,.2f}` {currency}\n"
            f"💼 *Total Billed:* `{total_billed:,.2f}` {currency}\n"
            f"━━━━━━━━━━━━━━━━━━"
        )

        kb = [
            [InlineKeyboardButton("🧾 View Pending Invoices",
                                  callback_data="b2b_my_invoices_pending_0")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")],
        ]
        try:
            query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')
        except Exception:
            pass
        return MAIN_MENU

    def _b2b_my_quotes(self, update: Update, context: CallbackContext):
        """B2B: View own quotes only (via proxy)."""
        query = update.callback_query
        query.answer()
        user = update.effective_user
        sap_proxy, b2b_bp = self._get_sap_for_user(user.id)
        if not b2b_bp:
            return self.sales.view_quotes(update, context)

        page = int(query.data.split('_')[-1])
        query.edit_message_text("⏳ *Fetching your quotations from SAP...*", parse_mode='Markdown')

        quotes = sap_proxy.get_quotations(skip=page * 5, top=5)
        kb = []
        txt = f"📝 *My Quotes* (Page {page + 1})\n━━━━━━━━━━━━━━━━━━\n"
        if quotes:
            for q in quotes:
                kb.append([InlineKeyboardButton(
                    f"📝 {q['SalesQuotation']} | {q['TotalNetAmount']}",
                    callback_data=f"b2b_quote_detail_{q['SalesQuotation']}")])
            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"b2b_my_quotes_{page - 1}"))
            if len(quotes) == 5:
                nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"b2b_my_quotes_{page + 1}"))
            if nav:
                kb.append(nav)
        else:
            txt += "_No quotations found._\n"
        kb.append([InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")])
        try:
            query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')
        except Exception:
            pass
        return MAIN_MENU

    def _b2b_order_detail(self, update: Update, context: CallbackContext):
        """B2B: View a specific order (ownership validated by proxy)."""
        query = update.callback_query
        query.answer()
        user = update.effective_user
        order_id = query.data.replace("b2b_order_detail_", "")
        query.edit_message_text("⏳ *Loading order details...*", parse_mode='Markdown')

        sap_proxy, b2b_bp = self._get_sap_for_user(user.id)
        details = sap_proxy.get_sales_order_details(order_id)
        if not details:
            query.edit_message_text(
                "❌ *Order not found or access denied.*",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Menu", callback_data="main_menu")]]),
                parse_mode='Markdown')
            return MAIN_MENU
        self.sales._send_order_card(query, details, "Order", edit=True)
        return MAIN_MENU

    def _b2b_quote_detail(self, update: Update, context: CallbackContext):
        """B2B: View a specific quote (ownership validated by proxy)."""
        query = update.callback_query
        query.answer()
        user = update.effective_user
        quote_id = query.data.replace("b2b_quote_detail_", "")
        query.edit_message_text("⏳ *Loading quotation details...*", parse_mode='Markdown')

        sap_proxy, b2b_bp = self._get_sap_for_user(user.id)
        details = sap_proxy.get_quotation_details(quote_id)
        if not details:
            query.edit_message_text(
                "❌ *Quote not found or access denied.*",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Menu", callback_data="main_menu")]]),
                parse_mode='Markdown')
            return MAIN_MENU
        self.sales._send_order_card(query, details, "Quote", edit=True)
        return MAIN_MENU

    def _b2b_check_credit(self, update: Update, context: CallbackContext):
        """B2B: View own credit limit (via proxy)."""
        query = update.callback_query
        query.answer()
        user = update.effective_user
        query.edit_message_text("⏳ *Fetching your credit data from SAP...*", parse_mode='Markdown')

        sap_proxy, b2b_bp = self._get_sap_for_user(user.id)
        if not b2b_bp:
            query.edit_message_text("❌ Session error.", parse_mode='Markdown')
            return MAIN_MENU

        credit_data = sap_proxy.get_credit_exposure(b2b_bp)
        bp_details = sap_proxy.get_customer_details(b2b_bp)
        cust_name = bp_details.get('Name') if bp_details else b2b_bp
        bp_disp = b2b_bp.lstrip('0')

        if credit_data:
            exposure = credit_data['exposure']
            limit = credit_data['limit']
            currency = credit_data['currency']
            remaining = credit_data['remaining']
            percent = (exposure / limit * 100) if limit > 0 else 0.0

            filled = int(min(percent, 100) / 10)
            bar = "█" * filled + "░" * (10 - filled)

            txt = (
                f"💳 *MY CREDIT REPORT*\n"
                f"══════════════════════\n"
                f"👤 *{cust_name}*\n"
                f"🆔 ID: `{bp_disp}`\n\n"
                f"📊 *UTILIZATION*\n"
                f"`[{bar}]` *{percent:.1f}%*\n"
                f"📈 Used: `{exposure:,.2f}` of `{limit:,.2f}`\n\n"
                f"💰 *FINANCIAL DETAILS*\n"
                f"──────────────────────\n"
                f"🔴 *Total Exposure:* `{exposure:,.2f} {currency}`\n"
                f"📏 *Credit Limit:* `{limit:,.2f} {currency}`\n"
                f"🟢 *Available:* `{remaining:,.2f} {currency}`\n"
                f"══════════════════════"
            )
        else:
            txt = (
                f"⚠️ *Credit Data Not Available*\n\n"
                f"No credit segment maintained for your account `{bp_disp}`."
            )

        kb = [[InlineKeyboardButton("🔄 Refresh", callback_data="b2b_check_credit"),
               InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]]
        query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')
        return MAIN_MENU

    def _b2b_cust360(self, update: Update, context: CallbackContext):
        """B2B: View own 360 profile (via proxy)."""
        query = update.callback_query
        query.answer()
        user = update.effective_user
        query.edit_message_text("⏳ *Loading your 360 profile from SAP...*", parse_mode='Markdown')

        sap_proxy, b2b_bp = self._get_sap_for_user(user.id)
        if not b2b_bp:
            query.edit_message_text("❌ Session error.", parse_mode='Markdown')
            return MAIN_MENU

        details = sap_proxy.get_customer_details(b2b_bp)
        revenue = sap_proxy.get_customer_revenue_summary(b2b_bp)
        credit = sap_proxy.get_credit_exposure(b2b_bp)

        name = details.get('Name', b2b_bp) if details else b2b_bp
        bp_disp = b2b_bp.lstrip('0')

        txt = (
            f"📊 *My 360 Profile — {name}*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🆔 ID: `{bp_disp}`\n\n"
        )

        if revenue:
            txt += (
                f"💰 *Revenue Summary*\n"
                f"   Total: `{revenue['total_revenue']:,.2f} {revenue['currency']}`\n"
                f"   Invoices: `{revenue['invoice_count']}`\n"
                f"   Last Invoice: `{revenue['last_invoice_date']}`\n\n"
            )
        else:
            txt += "💰 *Revenue:* _No billing data_\n\n"

        if credit:
            txt += (
                f"💳 *Credit Status*\n"
                f"   Limit: `{credit.get('limit', 'N/A')}`\n"
                f"   Exposure: `{credit.get('exposure', 'N/A')}`\n"
                f"   Available: `{credit.get('remaining', 'N/A')}`\n\n"
            )

        if details:
            txt += (
                f"📍 *Address:* {details.get('Address', 'N/A')}\n"
                f"📧 *Email:* {details.get('Email', 'N/A')}\n"
                f"📱 *Mobile:* {details.get('Mobile', 'N/A')}\n"
            )

        kb = [
            [InlineKeyboardButton("📦 My Orders", callback_data="b2b_my_orders_0"),
             InlineKeyboardButton("📝 My Quotes", callback_data="b2b_my_quotes_0")],
            [InlineKeyboardButton("🔄 Refresh", callback_data="b2b_cust360"),
             InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")],
        ]
        try:
            query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')
        except Exception:
            pass
        return MAIN_MENU

    def _b2b_new_order(self, update: Update, context: CallbackContext):
        """B2B: Start order creation with customer auto-locked."""
        query = update.callback_query
        query.answer()
        user = update.effective_user
        b2b_bp = self.db.get_b2b_customer_id(user.id)
        if not b2b_bp:
            return self.order.start_create_order_standalone(update, context)
        query.edit_message_text("⏳ *Preparing order form...*", parse_mode='Markdown')
        # Init flow with locked customer
        self.order._init_flow(context, cust_id=b2b_bp, doc_type="ORDER")
        sa = self.sap.get_customer_sales_area(b2b_bp)
        if not sa:
            query.edit_message_text("❌ No Sales Area found for your account.",
                                    reply_markup=InlineKeyboardMarkup(
                                        [[InlineKeyboardButton("🏠 Menu", callback_data="main_menu")]]))
            return MAIN_MENU
        context.user_data['order_sa'] = sa
        self.db.log_event(user, "B2B_START_ORDER", f"B2B order for {b2b_bp}")
        query.edit_message_text(
            f"🛒 *New Order*\n━━━━━━━━━━━━━━━━━━\n"
            f"👤 Customer: `{b2b_bp}`\n"
            f"🏢 Org: `{sa['SalesOrganization']}`\n\n"
            f"🔖 **Enter your Reference / PO Number:**",
            parse_mode='Markdown')
        return ORDER_ASK_REF

    def _b2b_new_quote(self, update: Update, context: CallbackContext):
        """B2B: Start quote creation with customer auto-locked."""
        query = update.callback_query
        query.answer()
        user = update.effective_user
        b2b_bp = self.db.get_b2b_customer_id(user.id)
        if not b2b_bp:
            return self.order.start_create_quote_standalone(update, context)
        query.edit_message_text("⏳ *Preparing quotation form...*", parse_mode='Markdown')
        # Init flow with locked customer
        self.order._init_flow(context, cust_id=b2b_bp, doc_type="QUOTE")
        sa = self.sap.get_customer_sales_area(b2b_bp)
        if not sa:
            query.edit_message_text("❌ No Sales Area found for your account.",
                                    reply_markup=InlineKeyboardMarkup(
                                        [[InlineKeyboardButton("🏠 Menu", callback_data="main_menu")]]))
            return MAIN_MENU
        context.user_data['order_sa'] = sa
        self.db.log_event(user, "B2B_START_QUOTE", f"B2B quote for {b2b_bp}")
        query.edit_message_text(
            f"📝 *New Quotation*\n━━━━━━━━━━━━━━━━━━\n"
            f"👤 Customer: `{b2b_bp}`\n"
            f"🏢 Org: `{sa['SalesOrganization']}`\n\n"
            f"🔖 **Enter your Reference / PO Number:**",
            parse_mode='Markdown')
        return ORDER_ASK_REF

    def _b2b_logout(self, update: Update, context: CallbackContext):
        """B2B: Logout handler via menu button."""
        query = update.callback_query
        query.answer()
        user = update.effective_user
        if user:
            self.security.destroy_session(user.id)
            self.security.flush_user_data(context, user.id)
        query.edit_message_text(
            "🔒 *Session Ended*\n\n"
            "All cached data has been securely cleared.\n"
            "Send /start to log in again.",
            parse_mode='Markdown')
        return ConversationHandler.END

    def _get_sap_for_user(self, user_id):
        """Return the appropriate SAP handler for a user.
        B2B users get a secure proxy; internal users get the raw handler."""
        b2b_bp = self.db.get_b2b_customer_id(user_id)
        if b2b_bp:
            return B2BSecureSAPHandler(self.sap, b2b_bp), b2b_bp
        return self.sap, None

    def log_all_activity(self, update: Update, context: CallbackContext):
        user = update.effective_user
        if not user: return
        action = "UNKNOWN"
        detail = ""

        if update.message and update.message.text:
            action = "TEXT"
            detail = update.message.text
        elif update.callback_query:
            action = "CLICK"
            raw_data = update.callback_query.data
            detail = raw_data
            try:
                msg = update.callback_query.message
                if msg.reply_markup:
                    for row in msg.reply_markup.inline_keyboard:
                        for btn in row:
                            if btn.callback_data == raw_data:
                                detail = btn.text
                                break
            except Exception:
                pass

        if detail:
            self.db.log_event_and_update_active(user, action, detail)

            # Auto-track favorites from callback clicks (Sprint 4)
            if update.callback_query and action == "CLICK":
                fav_map = {
                    'view_dashboard': 'Dashboard',
                    'bp_menu': 'Customers',
                    'sales_menu': 'Sales',
                    'start_material_search': 'Materials',
                    'start_expense': 'Expenses',
                }
                cb_data = update.callback_query.data
                if cb_data in fav_map:
                    self.db.add_favorite(user.id, cb_data, fav_map[cb_data])

    def start(self, update: Update, context: CallbackContext):
        if context.args:
            payload = context.args[0]
            if "email_approve_" in payload:
                return self.execute_approval(update, context, payload.split('_')[-1])
            elif "email_reject_" in payload:
                reject_id = payload.split('_')[-1]
                context.user_data['reject_db_id'] = reject_id
                req = self.db.get_pending_order(reject_id)
                doc_label = "Quotation" if req and req['order_data'].get('doc_type') == 'QUOTE' else "Order"
                update.message.reply_text(
                    f"⚠️ **Rejecting {doc_label} #{reject_id}**\n━━━━━━━━━━━━━━━━━━\n"
                    f"Please type the rejection reason:",
                    parse_mode='Markdown')
                return MANAGER_REASON_INPUT

        # --- GDPR Consent Gate: must accept privacy policy before anything ---
        user = update.effective_user
        if user and not self.db.has_valid_consent(user.id, PRIVACY_POLICY_VERSION):
            consent_text = (
                "🔒 *Privacy Policy & Data Consent*\n"
                "━━━━━━━━━━━━━━━━━━\n\n"
                "Before using this bot, please review and accept our data processing terms:\n\n"
                "• This bot accesses *SAP S/4HANA* business data (customers, orders, financials) "
                "on your behalf.\n"
                "• Your Telegram user ID and interactions are logged for *security audit* purposes.\n"
                "• Sensitive data (PII, financials) is masked in logs and never shared with third parties.\n"
                "• Your session data is encrypted in memory and securely wiped on logout/timeout.\n"
                "• You may request *complete deletion* of your data at any time via /deletemydata.\n"
                "• Messages sent via Telegram are subject to Telegram's own privacy policy.\n\n"
                f"📋 Policy Version: *{PRIVACY_POLICY_VERSION}*\n\n"
                "By clicking below, you consent to the processing described above."
            )
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ I Agree to the Privacy Policy", callback_data="gdpr_accept")],
                [InlineKeyboardButton("❌ I Decline", callback_data="gdpr_decline")],
            ])
            if update.callback_query:
                update.callback_query.edit_message_text(consent_text, reply_markup=kb, parse_mode='Markdown')
            else:
                update.message.reply_text(consent_text, reply_markup=kb, parse_mode='Markdown')
            return GDPR_CONSENT

        # --- OTP Authentication Gate ---
        # Manager bot skips OTP (uses its own token-based security)
        is_manager = (context.bot.token == MANAGER_BOT_TOKEN)

        if not is_manager and not self.security.is_session_valid(user.id):
            # Not logged in — start the OTP login flow
            txt = (
                "🔐 *SAP S/4HANA Bot — Login*\n"
                "━━━━━━━━━━━━━━━━━━\n\n"
                "To access the bot, please enter your\n"
                "*SAP Customer ID* (Business Partner ID).\n\n"
                "Example: `100` or `1000025`\n\n"
                "👇 *Type your ID below:*"
            )
            if update.callback_query:
                try:
                    update.callback_query.edit_message_text(txt, parse_mode='Markdown')
                except Exception:
                    update.callback_query.message.reply_text(txt, parse_mode='Markdown')
            else:
                update.message.reply_text(txt, parse_mode='Markdown')
            return OTP_ASK_BP_ID

        context.user_data.clear()

        if is_manager:
            # Store manager chat_id for reliable notifications
            if update.effective_chat:
                self.db.set_manager_config('manager_chat_id', str(update.effective_chat.id))

            kb = [
                [InlineKeyboardButton("🔔 Notifications", callback_data="view_notifications")],
                [InlineKeyboardButton("🎯 Set Sales Target", callback_data="mgr_set_target")],
                [InlineKeyboardButton("📈 Team Analytics", callback_data="mgr_analytics")],
                [InlineKeyboardButton("💵 Petty Cash Settings", callback_data="mgr_petty_cash")],
                [InlineKeyboardButton("👥 Active Users", callback_data="mgr_active_users")],
                [InlineKeyboardButton("🔐 Manage User Access", callback_data="mgr_manage_users")],
            ]
            txt = "🏢 *Manager Control Panel*\n━━━━━━━━━━━━━━━━━━\n📋 Select an option:"
        else:
            # Auto-register sales user
            if user:
                self.db.register_user(user.id, user.username, user.first_name)

            # Check if B2B customer
            b2b_bp = self.db.get_b2b_customer_id(user.id) if user else None

            if b2b_bp:
                # ============================================================
                # B2B CUSTOMER — RESTRICTED MENU
                # ============================================================
                bp_data = self.db.get_user_bp(user.id)
                bp_name = bp_data.get('bp_id', b2b_bp) if bp_data else b2b_bp

                # Store B2B context for handlers
                context.user_data['b2b_bp_id'] = b2b_bp

                kb = [
                    [InlineKeyboardButton("📦 My Orders", callback_data="b2b_my_orders_0"),
                     InlineKeyboardButton("📝 My Quotes", callback_data="b2b_my_quotes_0")],
                    [InlineKeyboardButton("🧾 My Invoices", callback_data="b2b_invoice_menu")],
                    [InlineKeyboardButton("📊 Statement of Accounts", callback_data="b2b_soa")],
                    [InlineKeyboardButton("➕ New Order", callback_data="b2b_new_order"),
                     InlineKeyboardButton("➕ New Quote", callback_data="b2b_new_quote")],
                    [InlineKeyboardButton("🔍 Material Search", callback_data="start_material_search")],
                    [InlineKeyboardButton("💳 My Credit Limit", callback_data="b2b_check_credit"),
                     InlineKeyboardButton("📊 My 360 Profile", callback_data="b2b_cust360")],
                    [InlineKeyboardButton("🔒 Logout", callback_data="b2b_logout")],
                ]
                txt = (
                    f"🏢 *SAP S/4HANA Self-Service*\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"👤 Customer: `{b2b_bp}`\n\n"
                    f"📋 What would you like to do?"
                )
            else:
                # ============================================================
                # INTERNAL SALES REP — FULL MENU (unchanged)
                # ============================================================
                # Build menu based on RBAC permissions
                perms = self.db.get_user_permissions(user.id) if user else {}

                # Build RBAC-guarded module buttons (single authoritative set)
                module_buttons = [
                    ('dashboard', "📊 My Dashboard", "view_dashboard"),
                    ('customer', "👤 Customer Center", "bp_menu"),
                    ('sales', "📦 Sales & Quotes", "sales_menu"),
                    ('material', "🔍 Material Search", "start_material_search"),
                    ('expense', "🧾 Expense Claim", "start_expense"),
                ]

                # Determine which modules the user can access
                allowed_keys = set()
                for mod_key, label, cb_data in module_buttons:
                    if perms.get(mod_key, True):
                        allowed_keys.add(cb_data)

                # Quick Actions from favorites (only show if NOT already in the module list)
                favorites = self.db.get_favorites(user.id, limit=3) if user else []
                fav_row = []
                for fav in favorites:
                    if fav['action_key'] not in allowed_keys:
                        fav_row.append(InlineKeyboardButton(
                            f"⭐ {fav['action_label']}", callback_data=fav['action_key']))

                kb = []
                if perms.get('Notification', True):
                    kb.append([InlineKeyboardButton("🔔 My Notifications", callback_data="view_notifications")])
                if fav_row:
                    kb.append(fav_row)

                for mod_key, label, cb_data in module_buttons:
                    if cb_data in allowed_keys:
                        kb.append([InlineKeyboardButton(label, callback_data=cb_data)])

                txt = "🏢 *SAP S/4HANA Assistant*\n━━━━━━━━━━━━━━━━━━\n📋 Select a Module:"

        if update.callback_query:
            try:
                update.callback_query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(kb),
                                                        parse_mode='Markdown')
            except Exception:
                update.callback_query.message.reply_text(txt, reply_markup=InlineKeyboardMarkup(kb),
                                                         parse_mode='Markdown')
        else:
            update.message.reply_text(txt, reply_markup=InlineKeyboardMarkup(kb),
                                      parse_mode='Markdown')

        return MAIN_MENU

    def router_inline_query(self, update: Update, context: CallbackContext):
        query = update.inline_query.query
        if query.startswith("cust "):
            self.order.handle_customer_inline_query(update, context)
        elif query.startswith("order "):
            self.order.handle_order_inline_query(update, context)
        elif query.startswith("qt "):
            self.order.handle_quote_inline_query(update, context)
        elif query.startswith("ven_") or query.startswith("gl_") or query.startswith("cc_") or query.startswith("tax_"):
            self.expense.handle_inline_vendor_query(update, context)
        else:
            self.material.handle_inline_query(update, context)

    def execute_approval(self, update, context, db_id):
        self.db.update_status(db_id, "APPROVED")
        req = self.db.get_pending_order(db_id)
        if not req:
            txt = f"⚠️ Request #{db_id} not found or already processed."
            if update.callback_query:
                update.callback_query.edit_message_text(txt, parse_mode='Markdown')
            else:
                update.message.reply_text(txt, parse_mode='Markdown')
            return MAIN_MENU

        d = req['order_data']
        doc_type = d.get('doc_type', 'ORDER')
        doc_label = "Sales Quotation" if doc_type == "QUOTE" else "Sales Order"

        try:
            if doc_type == "QUOTE":
                res = self.sap.create_sales_quotation(
                    d['customer'], d['items'], d.get('ref', 'Approved-Quote'))
            else:
                res = self.sap.create_sales_order(
                    d['customer'], d['items'], d.get('ref', 'Approved-Order'), req['discount'])
        except Exception as e:
            logger.error("SAP creation failed after approval #%s: %s", db_id, e, exc_info=True)
            res = {'success': False, 'error': str(e)}

        if res['success']:
            msg = f"✅ Request #{db_id} Approved!\n📄 SAP {doc_label}: `{res['id']}`"
        else:
            msg = (f"⚠️ Request #{db_id} Approved but SAP {doc_label} creation failed:\n"
                   f"`{res['error']}`")

        self.db.add_notification(req['user_id'], msg)
        try:
            requests.post(f"https://api.telegram.org/bot{SALES_BOT_TOKEN}/sendMessage",
                          json={"chat_id": req['user_id'], "text": msg}, timeout=10)
        except requests.RequestException as e:
            logger.warning("Telegram notification failed: %s", e)

        txt = f"✅ **{doc_label} Request #{db_id} Approved Successfully!**"
        if update.callback_query:
            update.callback_query.edit_message_text(txt, parse_mode='Markdown')
        else:
            update.message.reply_text(txt, parse_mode='Markdown')
        return MAIN_MENU

    def handle_manager_action(self, update: Update, context: CallbackContext):
        query = update.callback_query
        if context.bot.token != MANAGER_BOT_TOKEN:
            query.answer("Access Denied", show_alert=True)
            return MAIN_MENU
        data = query.data
        db_id = data.split('_')[-1]
        if "mgr_approve_" in data:
            return self.execute_approval(update, context, db_id)
        elif "mgr_reject_ask_" in data:
            context.user_data['reject_db_id'] = db_id
            req = self.db.get_pending_order(db_id)
            doc_label = "Quotation" if req and req['order_data'].get('doc_type') == 'QUOTE' else "Order"
            query.edit_message_text(
                f"⚠️ **Rejecting {doc_label} #{db_id}**\n━━━━━━━━━━━━━━━━━━\n"
                f"Please type the rejection reason:",
                parse_mode='Markdown')
            return MANAGER_REASON_INPUT
        return MAIN_MENU

    def handle_rejection_reason(self, update: Update, context: CallbackContext):
        reason = update.message.text
        context.user_data['reject_reason'] = reason
        db_id = context.user_data.get('reject_db_id')
        req = self.db.get_pending_order(db_id)
        doc_label = "Quotation" if req and req['order_data'].get('doc_type') == 'QUOTE' else "Order"
        kb = [[InlineKeyboardButton("🚫 Yes, Reject", callback_data="confirm_reject_yes")],
              [InlineKeyboardButton("↩️ Cancel", callback_data="confirm_reject_no")]]
        update.message.reply_text(
            f"⚠️ **Confirm Rejection?**\n━━━━━━━━━━━━━━━━━━\n"
            f"📋 {doc_label}: `#{db_id}`\n📝 Reason: {reason}",
            reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')
        return MANAGER_CONFIRM_REJECT

    def finalize_rejection(self, update: Update, context: CallbackContext):
        query = update.callback_query
        if "yes" in query.data:
            db_id = context.user_data.get('reject_db_id')
            reason = context.user_data.get('reject_reason')
            self.db.update_status(db_id, "REJECTED")
            req = self.db.get_pending_order(db_id)
            if req:
                msg = f"🚫 **Request #{db_id} Rejected**\n📝 Reason: {reason}"
                self.db.add_notification(req['user_id'], msg)
                try:
                    requests.post(f"https://api.telegram.org/bot{SALES_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": req['user_id'], "text": msg}, timeout=10)
                except requests.RequestException as e:
                    logger.warning("Telegram rejection notification failed: %s", e)
            query.edit_message_text("🚫 Rejection Sent.")
        else:
            query.edit_message_text("↩️ Rejection Cancelled.")
        return MAIN_MENU

    # --- RBAC ACCESS CHECK (blocks unauthorized module access) ---
    def _check_access(self, module_key, update, context):
        """Check if a sales user has access to a module. Returns True if allowed."""
        if context.bot.token == MANAGER_BOT_TOKEN:
            return True
        user = update.effective_user
        if not user:
            return True
        perms = self.db.get_user_permissions(user.id)
        if not perms.get(module_key, True):
            if update.callback_query:
                update.callback_query.answer("🚫 Access Denied. Contact your manager.", show_alert=True)
            return False
        return True

    # Wrapped entry points for RBAC enforcement
    def _b2b_block(self, update, module_name):
        """Block B2B users from accessing internal-only modules. Returns True if blocked."""
        user = update.effective_user
        if user and self.db.get_b2b_customer_id(user.id):
            if update.callback_query:
                update.callback_query.answer(
                    f"🚫 {module_name} is not available for B2B accounts.", show_alert=True)
            return True
        return False

    def _guarded_dashboard(self, update, context):
        if self._b2b_block(update, "Dashboard"): return MAIN_MENU
        if not self._check_access('dashboard', update, context): return MAIN_MENU
        return self.dash.start_dashboard_flow(update, context)

    def _guarded_customer(self, update, context):
        if self._b2b_block(update, "Customer Center"): return MAIN_MENU
        if not self._check_access('customer', update, context): return MAIN_MENU
        return self.cust.show_customer_menu(update, context)

    def _guarded_sales(self, update, context):
        if self._b2b_block(update, "Sales & Quotes"): return MAIN_MENU
        if not self._check_access('sales', update, context): return MAIN_MENU
        return self.sales.show_sales_menu(update, context)

    def _guarded_material(self, update, context):
        if not self._check_access('material', update, context): return MAIN_MENU
        return self.material.start_material_search(update, context)

    def _guarded_expense(self, update, context):
        if self._b2b_block(update, "Expense Claims"): return MAIN_MENU
        if not self._check_access('expense', update, context): return MAIN_MENU
        return self.expense.start_flow(update, context)

    def _session_timeout_callback(self, update: Update, context: CallbackContext):
        """Called when ConversationHandler times out. Securely flush all user data from RAM."""
        user = update.effective_user
        if user:
            self.security.flush_user_data(context, user.id)
            self.security.destroy_session(user.id)
            self.db.log_event(user, "SESSION_TIMEOUT", "Conversation timed out — RAM flushed")
            logger.info("Session timed out and flushed for user_%s", user.id)
            # Best-effort notification to user
            try:
                if update.effective_chat:
                    context.bot.send_message(
                        chat_id=update.effective_chat.id,
                        text="⏱ Session timed out. Your data has been securely cleared.\n"
                             "Send /start to begin a new session.")
            except Exception:
                pass  # User may have blocked the bot
        gc.collect()
        return ConversationHandler.END

    def setup_dispatcher(self, dp):
        dp.add_handler(TypeHandler(Update, self.log_all_activity), group=-1)
        dp.add_handler(InlineQueryHandler(self.router_inline_query))

        # --- Security commands (always available, outside conversation) ---
        dp.add_handler(CommandHandler("logout", self.handle_logout))

        # --- Notification standalone handlers ---
        dp.add_handler(CallbackQueryHandler(self.notify_ui.show_notifications_menu, pattern='^view_notifications$'))
        dp.add_handler(CallbackQueryHandler(self.notify_ui.show_pending_list, pattern='^show_pending_list$'))
        dp.add_handler(CallbackQueryHandler(self.notify_ui.show_approval_detail, pattern='^mgr_review_'))
        dp.add_handler(CallbackQueryHandler(self.notify_ui.show_approval_history, pattern='^show_approval_history$'))
        dp.add_handler(CallbackQueryHandler(self.notify_ui.handle_pending_page, pattern='^pending_page_'))
        dp.add_handler(CallbackQueryHandler(self.notify_ui.handle_history_page, pattern='^history_page_'))
        dp.add_handler(CallbackQueryHandler(self.notify_ui.show_history_detail, pattern='^read_history_'))

        # Standalone handlers for PDFs & Delivery
        dp.add_handler(CallbackQueryHandler(self.order.handle_pdf_callback, pattern='^gen_pdf_'))
        dp.add_handler(CallbackQueryHandler(self.order.show_pdf_history, pattern='^view_pdf_history$'))
        dp.add_handler(CallbackQueryHandler(self.order.show_delivery_status, pattern='^track_order_'))

        # B2B customer-specific handlers
        b2b_handlers = [
            CallbackQueryHandler(self._b2b_my_orders, pattern='^b2b_my_orders_'),
            CallbackQueryHandler(self._b2b_my_quotes, pattern='^b2b_my_quotes_'),
            CallbackQueryHandler(self._b2b_invoice_menu, pattern='^b2b_invoice_menu$'),
            CallbackQueryHandler(self._b2b_my_invoices, pattern='^b2b_my_invoices_'),
            CallbackQueryHandler(self._b2b_invoice_detail, pattern='^b2b_inv_detail_'),
            CallbackQueryHandler(self._b2b_inv_pdf_mock, pattern='^b2b_inv_pdf_'),
            CallbackQueryHandler(self._b2b_statement_of_accounts, pattern='^b2b_soa$'),
            CallbackQueryHandler(self._b2b_order_detail, pattern='^b2b_order_detail_'),
            CallbackQueryHandler(self._b2b_quote_detail, pattern='^b2b_quote_detail_'),
            CallbackQueryHandler(self._b2b_new_order, pattern='^b2b_new_order$'),
            CallbackQueryHandler(self._b2b_new_quote, pattern='^b2b_new_quote$'),
            CallbackQueryHandler(self._b2b_check_credit, pattern='^b2b_check_credit$'),
            CallbackQueryHandler(self._b2b_cust360, pattern='^b2b_cust360$'),
            CallbackQueryHandler(self._b2b_logout, pattern='^b2b_logout$'),
        ]

        # RBAC-guarded navigation handlers
        nav_handlers = [CallbackQueryHandler(self._guarded_dashboard, pattern='^view_dashboard$'),
                        CallbackQueryHandler(self._guarded_customer, pattern='^bp_menu$'),
                        CallbackQueryHandler(self._guarded_sales, pattern='^sales_menu$'),
                        CallbackQueryHandler(self._guarded_material, pattern='^start_material_search$'),
                        CallbackQueryHandler(self._guarded_expense, pattern='^start_expense$'),
                        CallbackQueryHandler(self.start, pattern='^main_menu$')] + b2b_handlers

        dash_h = self.dash.get_handlers()

        c_h = self.cust.get_handlers()
        c_h.append(CallbackQueryHandler(self.price.start_price_check, pattern='^check_price_'))
        c_h.append(CallbackQueryHandler(self.credit.check_credit_limit, pattern='^check_credit_'))
        c_h.append(
            CallbackQueryHandler(self.order.start_transaction_from_customer, pattern='^create_order_|^create_quote_'))

        s_h = self.sales.get_handlers()
        s_h.append(CallbackQueryHandler(self.order.start_create_order_standalone, pattern='^start_create_order$'))
        s_h.append(CallbackQueryHandler(self.order.start_create_quote_standalone, pattern='^start_create_quote$'))
        s_h.append(CallbackQueryHandler(self.order.start_quote_conversion_flow,
                                        pattern='^start_quote_conversion$|^convert_qt_'))

        p_h = self.price.get_handlers()
        p_h.append(CallbackQueryHandler(self.cust.handle_details_callback_external, pattern='^customer_details_view_'))

        sales_menu_handlers = dash_h + s_h

        # Manager-only handlers
        mgr_handlers = [
            CallbackQueryHandler(self.mgr.show_active_users, pattern='^mgr_active_users$'),
            CallbackQueryHandler(self.mgr.show_user_list, pattern='^mgr_manage_users$'),
            CallbackQueryHandler(self.mgr.start_set_target, pattern='^mgr_set_target$'),
            CallbackQueryHandler(self.analytics.show_analytics_menu, pattern='^mgr_analytics$'),
            CallbackQueryHandler(self.mgr.show_petty_cash_menu, pattern='^mgr_petty_cash$'),
        ]

        conv_handler = ConversationHandler(
            entry_points=[
                CommandHandler("start", self.start),
                CallbackQueryHandler(self.start, pattern='^main_menu$'),
                CallbackQueryHandler(self._guarded_dashboard, pattern='^view_dashboard$'),
                CallbackQueryHandler(self._guarded_customer, pattern='^bp_menu$'),
                CallbackQueryHandler(self._guarded_sales, pattern='^sales_menu$'),
                CallbackQueryHandler(self._guarded_material, pattern='^start_material_search$'),
                CallbackQueryHandler(self._guarded_expense, pattern='^start_expense$'),
                CallbackQueryHandler(self.handle_manager_action, pattern='^mgr_approve_|^mgr_reject_ask_'),
                CallbackQueryHandler(self.mgr.start_set_target, pattern='^mgr_set_target$'),
                CallbackQueryHandler(self.mgr.show_active_users, pattern='^mgr_active_users$'),
                CallbackQueryHandler(self.mgr.show_user_list, pattern='^mgr_manage_users$'),
                CallbackQueryHandler(self.analytics.show_analytics_menu, pattern='^mgr_analytics$'),
                CallbackQueryHandler(self.mgr.show_petty_cash_menu, pattern='^mgr_petty_cash$'),
            ],
            states={
                # --- GDPR Consent (blocks all access until accepted) ---
                GDPR_CONSENT: [
                    CallbackQueryHandler(self.handle_gdpr_consent, pattern='^gdpr_'),
                ],

                # --- OTP Login Flow ---
                OTP_ASK_BP_ID: [
                    MessageHandler(Filters.text & ~Filters.command, self.handle_bp_id_input),
                ],
                OTP_ASK_CODE: [
                    MessageHandler(Filters.text & ~Filters.command, self.handle_otp_input),
                ],

                # --- Conversation Timeout: secure RAM flush ---
                ConversationHandler.TIMEOUT: [
                    TypeHandler(Update, self._session_timeout_callback),
                ],

                MAIN_MENU: nav_handlers + mgr_handlers,
                SALES_MENU: sales_menu_handlers + nav_handlers,
                MANAGER_REASON_INPUT: [MessageHandler(Filters.text & ~Filters.command, self.handle_rejection_reason)],
                MANAGER_CONFIRM_REJECT: [CallbackQueryHandler(self.finalize_rejection, pattern='^confirm_reject_')],

                MATERIAL_SEARCH_INPUT: [MessageHandler(Filters.text & ~Filters.command,
                                                       self.material.handle_search_input),
                                        CallbackQueryHandler(self.material.show_material_details_callback,
                                                             pattern='^view_material_'),
                                        CallbackQueryHandler(self.material.start_material_search,
                                                             pattern='^search_material_again$')] + nav_handlers,

                PRODUCT_SEARCH_INPUT: [
                                          MessageHandler(Filters.text & ~Filters.command,
                                                         self.price.handle_product_search),
                                          CallbackQueryHandler(self.price.start_price_check, pattern='^check_price_'),
                                          CallbackQueryHandler(self.price.handle_search_pagination,
                                                               pattern='^price_next$|^price_prev$'),
                                          CallbackQueryHandler(self.price.handle_price_selection,
                                                               pattern='^chk_price_'),
                                          CallbackQueryHandler(self.cust.handle_details_callback_external,
                                                               pattern='^customer_details_view_')
                                      ] + nav_handlers,

                ORDER_ASK_QUOTE_ID: [
                                        MessageHandler(
                                            Filters.regex(r'^/select_qt_') | (Filters.text & ~Filters.command),
                                            self.order.handle_quote_id_input)
                                    ] + nav_handlers,

                ORDER_ASK_VALIDITY: [
                                        CallbackQueryHandler(self.order.handle_validity_selection,
                                                             pattern='^valid_date_|^cal_sel_'),
                                        CallbackQueryHandler(self.order.prompt_discount, pattern='^goto_discount$'),
                                        MessageHandler(Filters.text & ~Filters.command,
                                                       self.order.handle_validity_selection)
                                    ] + nav_handlers,

                ORDER_ASK_MATERIAL: [
                                        MessageHandler(Filters.text | Filters.regex(r'^/select_mat_') | Filters.regex(
                                            r'^/mat_detail_'), self.order.handle_material_input),
                                        CallbackQueryHandler(self.order.handle_material_select_callback,
                                                             pattern='^sel_mat_'),
                                        CallbackQueryHandler(self.order.handle_search_pagination,
                                                             pattern='^search_next$|^search_prev$')
                                    ] + nav_handlers,

                ORDER_ASK_QTY: [MessageHandler(Filters.text, self.order.handle_qty_input)] + nav_handlers,
                ORDER_ADD_MORE: [CallbackQueryHandler(self.order.handle_add_more_choice)] + nav_handlers,
                ORDER_ASK_DISCOUNT: [CallbackQueryHandler(self.order.handle_discount_input, pattern='^skip_discount$'),
                                     MessageHandler(Filters.text, self.order.handle_discount_input)] + nav_handlers,
                ORDER_CONFIRM: [CallbackQueryHandler(self.order.handle_add_more_choice, pattern='^order_'),
                                CallbackQueryHandler(self.order.prompt_discount, pattern='^goto_discount$'),
                                CallbackQueryHandler(self.order.ask_validity_date, pattern='^valid_date_custom$'),
                                CallbackQueryHandler(self.order.execute_order,
                                                     pattern='^confirm_order_yes$|^cancel_order_flow$|^confirm_quote_convert$')] + nav_handlers,
                ORDER_REMOVE_ITEM: [CallbackQueryHandler(self.order.handle_remove_selection)] + nav_handlers,

                ORDER_ASK_CUSTOMER: [
                                        MessageHandler(Filters.text | Filters.regex(r'^/select_cust_'),
                                                       self.order.handle_customer_input),
                                        CallbackQueryHandler(self.order.handle_customer_select_callback,
                                                             pattern='^sel_cust_'),
                                        CallbackQueryHandler(self.order.handle_search_pagination,
                                                             pattern='^search_next$|^search_prev$')
                                    ] + nav_handlers,

                ORDER_ASK_REF: [MessageHandler(Filters.text, self.order.handle_ref_input)] + nav_handlers,

                CUSTOMER_MENU: c_h + [
                    CallbackQueryHandler(self.cust360.show_360_panel, pattern='^cust360_'),
                ] + nav_handlers,

                CUSTOMER_SEARCH_INPUT: [
                                           MessageHandler(Filters.text | Filters.regex(r'^/select_cust_'),
                                                          self.cust.handle_customer_details),
                                           CallbackQueryHandler(self.cust.handle_search_pagination,
                                                                pattern='^bp_next$|^bp_prev$'),
                                           CallbackQueryHandler(self.cust.handle_details_callback_external,
                                                                pattern='^customer_details_view_')
                                       ] + nav_handlers,

                CUSTOMER_CREATE_CATEGORY: [CallbackQueryHandler(self.cust.ask_name, pattern='^bp_cat_')] + nav_handlers,
                CUSTOMER_CREATE_NAME: [MessageHandler(Filters.text, self.cust.get_name)] + nav_handlers,
                CUSTOMER_CREATE_COUNTRY: [MessageHandler(Filters.text, self.cust.get_country)] + nav_handlers,
                CUSTOMER_CREATE_REGION: [MessageHandler(Filters.text, self.cust.get_region)] + nav_handlers,
                CUSTOMER_CREATE_CITY: [MessageHandler(Filters.text, self.cust.get_city)] + nav_handlers,
                CUSTOMER_CREATE_STREET: [MessageHandler(Filters.text, self.cust.get_street)] + nav_handlers,
                CUSTOMER_CREATE_POSTAL: [MessageHandler(Filters.text, self.cust.get_postal)] + nav_handlers,
                CUSTOMER_CREATE_MOBILE: [MessageHandler(Filters.text, self.cust.get_mobile)] + nav_handlers,
                CUSTOMER_CREATE_EMAIL: [MessageHandler(Filters.text,
                                                       self.cust.get_email_and_show_summary)] + nav_handlers,
                CUSTOMER_CREATE_CONFIRM: [CallbackQueryHandler(self.cust.finalize_creation,
                                                               pattern='^confirm_create_|^retry_create$|^edit_bp_')] + nav_handlers,

                SALES_SEARCH_INPUT: s_h + nav_handlers,
                PRODUCT_SEARCH_INPUT: p_h + nav_handlers,
                DASH_SELECT_PERIOD: [CallbackQueryHandler(self.dash.handle_period_selection,
                                                          pattern='^dash_period_')] + nav_handlers,
                DASH_ASK_START: [MessageHandler(Filters.text, self.dash.handle_custom_start)] + nav_handlers,
                DASH_ASK_END: [MessageHandler(Filters.text, self.dash.handle_custom_end)] + nav_handlers,
                DASH_ASK_TARGET: [MessageHandler(Filters.text, self.dash.handle_target_input)] + nav_handlers,
                DASH_SHOW_REPORT: [CallbackQueryHandler(self.dash.show_filter_menu, pattern='^filter_menu_'),
                                   CallbackQueryHandler(self.dash.start_comparison,
                                                        pattern='^dash_compare_start$'),
                                   CallbackQueryHandler(self.dash.start_dashboard_flow,
                                                        pattern='^view_dashboard$')] + nav_handlers,
                DASH_SELECT_STATUS_FILTER: [CallbackQueryHandler(self.dash.handle_pagination,
                                                                 pattern='^list_next$|^list_prev$'),
                                            CallbackQueryHandler(self.dash.render_list, pattern='^render_'),
                                            CallbackQueryHandler(self.dash.show_filter_menu, pattern='^filter_menu_'),
                                            CallbackQueryHandler(self.dash.back_to_report,
                                                                 pattern='^back_to_dash_report$')] + nav_handlers,

                # --- MANAGER TARGET SETTING ---
                MGR_TARGET_PERIOD: [CallbackQueryHandler(self.mgr.handle_target_period,
                                                         pattern='^tgt_period_')] + nav_handlers,
                MGR_TARGET_START: [MessageHandler(Filters.text, self.mgr.handle_target_custom_start)] + nav_handlers,
                MGR_TARGET_END: [MessageHandler(Filters.text, self.mgr.handle_target_custom_end)] + nav_handlers,
                MGR_TARGET_AMOUNT: [MessageHandler(Filters.text, self.mgr.handle_target_amount)] + nav_handlers,

                # --- MANAGER USER MANAGEMENT (RBAC) ---
                MGR_USER_LIST: [CallbackQueryHandler(self.mgr.show_user_permissions,
                                                     pattern='^mgr_user_')] + nav_handlers,
                MGR_USER_PERMISSIONS: [CallbackQueryHandler(self.mgr.toggle_permission,
                                                            pattern='^mgr_toggle_|^mgr_deactivate_|^mgr_activate_|^mgr_settype_'),
                                       CallbackQueryHandler(self.mgr.show_user_list,
                                                            pattern='^mgr_manage_users$')] + nav_handlers,

                # --- PETTY CASH SETTINGS (Sprint 2) ---
                MGR_PETTY_CASH_MENU: [CallbackQueryHandler(self.mgr.handle_petty_cash_selection,
                                                            pattern='^petty_set_')] + nav_handlers,
                MGR_PETTY_CASH_SET: [MessageHandler(Filters.text & ~Filters.command,
                                                     self.mgr.handle_petty_cash_amount)] + nav_handlers,

                # --- CUSTOMER 360 (Sprint 2) ---
                CUSTOMER_360: [
                    CallbackQueryHandler(self.cust360.show_360_panel, pattern='^cust360_'),
                    CallbackQueryHandler(self.cust360.show_order_history, pattern='^c360_orders_'),
                    CallbackQueryHandler(self.cust360.show_quote_history, pattern='^c360_quotes_'),
                    CallbackQueryHandler(self.cust.handle_details_callback_external, pattern='^customer_details_view_'),
                    CallbackQueryHandler(self.order.start_transaction_from_customer, pattern='^create_order_|^create_quote_'),
                ] + nav_handlers,

                # --- DASHBOARD COMPARISON (Sprint 4) ---
                DASH_COMPARE_START: [MessageHandler(Filters.text & ~Filters.command,
                                                     self.dash.handle_compare_start)] + nav_handlers,
                DASH_COMPARE_END: [MessageHandler(Filters.text & ~Filters.command,
                                                   self.dash.handle_compare_end)] + nav_handlers,

                # --- EXPENSE WITH VENDOR ---
                E_CO: [CallbackQueryHandler(self.expense.handle_co, pattern='^eco_')] + nav_handlers,
                E_VENDOR_TYPE: [CallbackQueryHandler(self.expense.handle_vendor_type,
                                                     pattern='^exp_type_')] + nav_handlers,
                E_V_SRCH: [MessageHandler(Filters.regex(r'^/sel_ven_'), self.expense.select_vendor),
                           CallbackQueryHandler(self.expense.select_vendor, pattern='^even_'),
                           MessageHandler(Filters.text & ~Filters.command,
                                          self.expense.search_vendor_text)] + nav_handlers,
                E_V_CONF: [CallbackQueryHandler(self.expense.handle_ven_confirm, pattern='^vconf_')] + nav_handlers,
                E_INPUT_METHOD: [CallbackQueryHandler(self.expense.handle_input_method,
                                                      pattern='^input_')] + nav_handlers,
                E_SCAN_PHOTO: [MessageHandler(Filters.photo, self.expense.handle_photo_scan)] + nav_handlers,
                E_FINAL_REVIEW: [CallbackQueryHandler(self.expense.handle_final_review,
                                                      pattern='^post_now$|^edit_menu$')] + nav_handlers,
                E_EDIT_MENU: [CallbackQueryHandler(self.expense.handle_edit_menu,
                                                   pattern='^edit_field_|^back_to_review$')] + nav_handlers,
                E_EDIT_VALUE: [MessageHandler(Filters.regex(r'^/sel_gl_'), self.expense.select_gl),
                               MessageHandler(Filters.regex(r'^/sel_cc_'), self.expense.select_cc),
                               MessageHandler(Filters.regex(r'^/sel_tax_'), self.expense.select_tax),
                               MessageHandler(Filters.text & ~Filters.command,
                                              self.expense.handle_edit_value)] + nav_handlers,
                E_REF: [MessageHandler(Filters.text, self.expense.handle_ref)] + nav_handlers,
                E_DATE_DOC: [MessageHandler(Filters.text, self.expense.handle_doc_date),
                             CallbackQueryHandler(self.expense.handle_doc_date, pattern='^curr_date_')] + nav_handlers,
                E_DATE_POST: [MessageHandler(Filters.text, self.expense.handle_post_date),
                              CallbackQueryHandler(self.expense.handle_post_date,
                                                   pattern='^curr_date_')] + nav_handlers,
                E_AMT: [MessageHandler(Filters.text, self.expense.handle_amt)] + nav_handlers,
                E_DESC: [MessageHandler(Filters.text, self.expense.handle_desc)] + nav_handlers,
                E_GL_SRCH: [MessageHandler(Filters.regex(r'^/sel_gl_'), self.expense.select_gl),
                            CallbackQueryHandler(self.expense.select_gl, pattern='^egl_'),
                            MessageHandler(Filters.text & ~Filters.command,
                                           self.expense.search_gl_text)] + nav_handlers,
                E_CC_SRCH: [MessageHandler(Filters.regex(r'^/sel_cc_'), self.expense.select_cc),
                            CallbackQueryHandler(self.expense.select_cc, pattern='^ecc_'),
                            MessageHandler(Filters.text & ~Filters.command,
                                           self.expense.search_cc_text)] + nav_handlers,
                E_TAX_SEL: [MessageHandler(Filters.regex(r'^/sel_tax_'), self.expense.select_tax),
                            CallbackQueryHandler(self.expense.select_tax, pattern='^etax_'),
                            MessageHandler(Filters.text & ~Filters.command,
                                           self.expense.search_tax_text)] + nav_handlers,
                E_ADD_MORE: [CallbackQueryHandler(self.expense.handle_add_more,
                                                  pattern='^add_more$|^post_now$')] + nav_handlers,

                # --- EXPENSE WITHOUT VENDOR ---
                EW_INPUT_METHOD: [CallbackQueryHandler(self.expense_wv.handle_input_method,
                                                       pattern='^ew_input_')] + nav_handlers,
                EW_SCAN_PHOTO: [MessageHandler(Filters.photo, self.expense_wv.handle_photo_scan)] + nav_handlers,
                EW_FINAL_REVIEW: [CallbackQueryHandler(self.expense_wv.handle_final_review,
                                                       pattern='^ew_post_now$|^ew_edit_menu$')] + nav_handlers,
                EW_EDIT_MENU: [CallbackQueryHandler(self.expense_wv.handle_edit_menu,
                                                    pattern='^ew_edit_field_|^ew_back_to_review$')] + nav_handlers,
                EW_EDIT_VALUE: [MessageHandler(Filters.regex(r'^/sel_gl_'), self.expense_wv.select_gl),
                                MessageHandler(Filters.regex(r'^/sel_cc_'), self.expense_wv.select_cc),
                                MessageHandler(Filters.regex(r'^/sel_tax_'), self.expense_wv.select_tax),
                                MessageHandler(Filters.text & ~Filters.command,
                                               self.expense_wv.handle_edit_value)] + nav_handlers,
                EW_REF: [MessageHandler(Filters.text, self.expense_wv.handle_ref)] + nav_handlers,
                EW_DATE_DOC: [MessageHandler(Filters.text, self.expense_wv.handle_doc_date),
                              CallbackQueryHandler(self.expense_wv.handle_doc_date,
                                                   pattern='^ew_curr_date_')] + nav_handlers,
                EW_DATE_POST: [MessageHandler(Filters.text, self.expense_wv.handle_post_date),
                               CallbackQueryHandler(self.expense_wv.handle_post_date,
                                                    pattern='^ew_curr_date_')] + nav_handlers,
                EW_AMT: [MessageHandler(Filters.text, self.expense_wv.handle_amt)] + nav_handlers,
                EW_DESC: [MessageHandler(Filters.text, self.expense_wv.handle_desc)] + nav_handlers,
                EW_GL_SRCH: [MessageHandler(Filters.regex(r'^/sel_gl_'), self.expense_wv.select_gl),
                             CallbackQueryHandler(self.expense_wv.select_gl, pattern='^ew_egl_'),
                             MessageHandler(Filters.text & ~Filters.command,
                                            self.expense_wv.search_gl_text)] + nav_handlers,
                EW_CC_SRCH: [MessageHandler(Filters.regex(r'^/sel_cc_'), self.expense_wv.select_cc),
                             CallbackQueryHandler(self.expense_wv.select_cc, pattern='^ew_ecc_'),
                             MessageHandler(Filters.text & ~Filters.command,
                                            self.expense_wv.search_cc_text)] + nav_handlers,
                EW_TAX_SEL: [MessageHandler(Filters.regex(r'^/sel_tax_'), self.expense_wv.select_tax),
                             CallbackQueryHandler(self.expense_wv.select_tax, pattern='^ew_etax_')] + nav_handlers,

            },
            fallbacks=[CommandHandler("start", self.start), CallbackQueryHandler(self.start, pattern='^main_menu$')],
            conversation_timeout=900,  # 15-minute session timeout with memory flush
            name="main_conversation",
            persistent=False,
        )

        conv_handler.states[MAIN_MENU].append(
            CallbackQueryHandler(self.handle_manager_action, pattern='^mgr_approve_|^mgr_reject_ask_'))

        # --- AI Copilot: catch-all text & photo handlers (MUST be last in MAIN_MENU) ---
        conv_handler.states[MAIN_MENU].append(
            MessageHandler(Filters.photo, self.copilot.handle_photo))
        conv_handler.states[MAIN_MENU].append(
            MessageHandler(Filters.text & ~Filters.command, self.copilot.handle_text))

        # --- AI Copilot: confirmation state for write operations ---
        conv_handler.states[COPILOT_CONFIRM] = [
            CallbackQueryHandler(self.copilot.handle_confirm, pattern='^copilot_'),
        ] + nav_handlers

        dp.add_handler(conv_handler)
        dp.add_handler(
            MessageHandler(Filters.regex(r'^/mat_detail_'), self.material.show_material_details_from_command))


if __name__ == '__main__':
    # Initialize structured logging FIRST, before anything else
    setup_logging()

    logger.info("Starting DUAL BOT System...")
    start_background_server()

    logger.info("Starting Sales Bot...")
    updater1 = Updater(SALES_BOT_TOKEN, workers=8, request_kwargs={'read_timeout': 30, 'connect_timeout': 30})
    app = SAPSalesBot()
    app.setup_dispatcher(updater1.dispatcher)
    # updater1.start_polling()  # COMMENTED FOR VERCEL WEBHOOK

    logger.info("Starting Manager Bot...")
    updater2 = Updater(MANAGER_BOT_TOKEN, workers=8, request_kwargs={'read_timeout': 30, 'connect_timeout': 30})
    app.setup_dispatcher(updater2.dispatcher)
    # updater2.start_polling()  # COMMENTED FOR VERCEL WEBHOOK

    # Start background scheduler for smart alerts
    try:
        from scheduler import start_scheduler
        start_scheduler(app.sap, app.db, SALES_BOT_TOKEN, MANAGER_BOT_TOKEN)
        logger.info("Smart Alert Scheduler started.")
    except Exception as e:
        logger.warning("Scheduler failed to start (non-critical): %s", e)

    # Start periodic security cleanup (expired sessions, cache eviction)
    try:
        import threading
        def _security_cleanup_loop():
            import time
            while True:
                time.sleep(300)  # Every 5 minutes
                expired = app.security.cleanup_expired_sessions()
                if expired:
                    logger.info("Security cleanup: %d expired sessions flushed", len(expired))
        cleanup_thread = threading.Thread(target=_security_cleanup_loop, daemon=True)
        cleanup_thread.start()
        logger.info("Security session cleanup thread started.")
    except Exception as e:
        logger.warning("Security cleanup thread failed (non-critical): %s", e)

    logger.info("Both bots are now online and ready for Webhook.")
    # updater1.idle()  # COMMENTED FOR VERCEL WEBHOOK