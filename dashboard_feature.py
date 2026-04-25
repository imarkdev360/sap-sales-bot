from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode
from telegram.ext import CallbackContext, CallbackQueryHandler, MessageHandler, Filters
from telegram.error import BadRequest
from config import ITEMS_PER_PAGE, MANAGER_BOT_TOKEN
from states import (
    DASH_SELECT_PERIOD, DASH_ASK_START, DASH_ASK_END, DASH_ASK_TARGET,
    DASH_SHOW_REPORT, DASH_SELECT_STATUS_FILTER,
    DASH_COMPARE_SELECT, DASH_COMPARE_START, DASH_COMPARE_END,
)
from logger_setup import get_logger

logger = get_logger(__name__)


class DashboardFeature:
    def __init__(self, sap_handler, db_handler):
        self.sap_handler = sap_handler
        self.db = db_handler

    # --- 1. ENTRY ---
    def start_dashboard_flow(self, update: Update, context: CallbackContext):
        query = update.callback_query
        query.answer()
        self.db.log_event(query.from_user, "VIEW_DASHBOARD_START", "Opened Dashboard")

        keys_to_clear = ['dash_data', 'dash_filter', 'dash_page', 'dash_category']
        for k in keys_to_clear:
            if k in context.user_data: del context.user_data[k]

        kb = [
            [InlineKeyboardButton("📅 Current Month", callback_data="dash_period_month"),
             InlineKeyboardButton("📅 Current Quarter", callback_data="dash_period_quarter")],
            [InlineKeyboardButton("📅 Current Year", callback_data="dash_period_year"),
             InlineKeyboardButton("✏️ Custom Date", callback_data="dash_period_custom")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
        ]

        try:
            query.edit_message_text(
                "📊 *Sales Analytics Dashboard*\n━━━━━━━━━━━━━━━━━━\n📅 Select reporting period:",
                reply_markup=InlineKeyboardMarkup(kb),
                parse_mode=ParseMode.MARKDOWN
            )
        except BadRequest:
            pass
        return DASH_SELECT_PERIOD

    # --- 2. PERIOD LOGIC ---
    def handle_period_selection(self, update: Update, context: CallbackContext):
        query = update.callback_query
        choice = query.data
        today = datetime.now()
        start_date = ""
        end_date = today.strftime("%d-%m-%Y")

        if choice == "dash_period_custom":
            query.edit_message_text("📅 **Start Date** (DD-MM-YYYY):", parse_mode=ParseMode.MARKDOWN)
            return DASH_ASK_START
        elif choice == "dash_period_month":
            start_date = today.replace(day=1).strftime("%d-%m-%Y")
        elif choice == "dash_period_quarter":
            q_month = ((today.month - 1) // 3) * 3 + 1
            start_date = today.replace(month=q_month, day=1).strftime("%d-%m-%Y")
        elif choice == "dash_period_year":
            start_date = today.replace(month=1, day=1).strftime("%d-%m-%Y")

        context.user_data['dash_start'] = start_date
        context.user_data['dash_end'] = end_date

        # Manager bot: ask target manually | Sales bot: auto-fetch from DB
        is_manager = (context.bot.token == MANAGER_BOT_TOKEN)
        if is_manager:
            return self._ask_target(update, context)
        else:
            return self._auto_fetch_target_and_report(update, context)

    def handle_custom_start(self, update: Update, context: CallbackContext):
        text = update.message.text.strip()
        if len(text) != 10:
            update.message.reply_text("❌ Invalid Format. Use DD-MM-YYYY.")
            return DASH_ASK_START
        context.user_data['dash_start'] = text
        update.message.reply_text("📅 **End Date** (DD-MM-YYYY):", parse_mode=ParseMode.MARKDOWN)
        return DASH_ASK_END

    def handle_custom_end(self, update: Update, context: CallbackContext):
        end = update.message.text.strip()
        context.user_data['dash_end'] = end

        is_manager = (context.bot.token == MANAGER_BOT_TOKEN)
        if is_manager:
            return self._ask_target(update, context)
        else:
            return self._auto_fetch_target_and_report(update, context)

    # --- 3. TARGET INPUT (Manager Only) ---
    def _ask_target(self, update, context):
        # Safety guard: sales users must NEVER reach the target prompt
        if context.bot.token != MANAGER_BOT_TOKEN:
            return self._auto_fetch_target_and_report(update, context)

        msg_txt = "🎯 **Enter Sales Target** (e.g. `50000`):"
        if update.callback_query:
            update.callback_query.edit_message_text(msg_txt, parse_mode=ParseMode.MARKDOWN)
        else:
            update.message.reply_text(msg_txt, parse_mode=ParseMode.MARKDOWN)
        return DASH_ASK_TARGET

    def handle_target_input(self, update: Update, context: CallbackContext):
        # Double-safety: sales users should never be in this state
        if context.bot.token != MANAGER_BOT_TOKEN:
            return self._auto_fetch_target_and_report(update, context)

        text = update.message.text.strip()
        if not text.isdigit():
            update.message.reply_text("❌ Enter numbers only.")
            return DASH_ASK_TARGET

        target = float(text)
        context.user_data['dash_target'] = target
        start = context.user_data['dash_start']
        end = context.user_data['dash_end']

        self.db.log_event(update.message.from_user, "DASH_SET_TARGET", f"Tgt: {target} | Period: {start}-{end}")

        return self._fetch_and_show_report(update, context, start, end, target)

    # --- 3b. AUTO-FETCH TARGET (Sales Users) ---
    def _auto_fetch_target_and_report(self, update, context):
        start = context.user_data['dash_start']
        end = context.user_data['dash_end']

        target = self.db.get_sales_target(start, end)

        if target is None:
            target = 0.0
            context.user_data['dash_no_target'] = True
        else:
            context.user_data['dash_no_target'] = False

        context.user_data['dash_target'] = target
        return self._fetch_and_show_report(update, context, start, end, target)

    # --- 4. REPORT DISPLAY ---
    def _fetch_and_show_report(self, update, context, start, end, target):
        if update.callback_query:
            msg = update.callback_query.edit_message_text("🔄 *Loading Data...*", parse_mode=ParseMode.MARKDOWN)
        else:
            msg = update.message.reply_text("🔄 *Loading Data...*", parse_mode=ParseMode.MARKDOWN)

        data = self.sap_handler.get_analytics_by_date(start, end)

        if not data:
            if update.callback_query:
                msg.edit_text("📭 No data found for this period.", reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Back", callback_data="view_dashboard")]]))
            else:
                msg.edit_text("📭 No data found for this period.", reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Back", callback_data="view_dashboard")]]))
            return DASH_SELECT_PERIOD

        context.user_data['dash_data'] = data
        context.user_data['dash_period'] = f"{start} - {end}"

        achieved = data['invoices']['val']
        pct = min(int((achieved / target) * 100), 100) if target > 0 else 0
        bar = "█" * (pct // 10) + "░" * (10 - (pct // 10))

        ord_status = "\n".join([f"   - {k}: {v}" for k, v in data['orders']['status'].items()])
        qt_status = "\n".join([f"   - {k}: {v}" for k, v in data['quotes']['status'].items()])

        # Target display
        no_target = context.user_data.get('dash_no_target', False)
        if no_target:
            target_line = "🎯 Target: _Not set by manager_"
            achieve_line = f"📈 Revenue: `{achieved:,.2f}`"
        else:
            target_line = f"🎯 Target: `{target:,.0f}`"
            achieve_line = f"📈 Achieved: {pct}% `[{bar}]`"

        report = (
            f"📊 *Sales Overview*\n"
            f"📅 {start} to {end}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{target_line}\n"
            f"{achieve_line}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🧾 **Invoices (Revenue):**\n"
            f"💰 `{achieved:,.2f}`  (Count: {data['invoices']['total']})\n\n"
            f"📦 **Sales Orders:**\n"
            f"💰 `{data['orders']['val']:,.2f}`  (Count: {data['orders']['total']})\n"
            f"{ord_status}\n\n"
            f"📝 **Quotations:**\n"
            f"💰 `{data['quotes']['val']:,.2f}`  (Count: {data['quotes']['total']})\n"
            f"{qt_status}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👇 *View Details:*"
        )

        kb = [
            [InlineKeyboardButton(f"🧾 Invoices ({data['invoices']['total']})", callback_data="filter_menu_invoices")],
            [InlineKeyboardButton(f"📦 Orders ({data['orders']['total']})", callback_data="filter_menu_orders")],
            [InlineKeyboardButton(f"📝 Quotes ({data['quotes']['total']})", callback_data="filter_menu_quotes")],
            [InlineKeyboardButton("📊 Compare Periods", callback_data="dash_compare_start")],
            [InlineKeyboardButton("🔄 Change Period", callback_data="view_dashboard"),
             InlineKeyboardButton("🏠 Menu", callback_data="main_menu")]
        ]

        try:
            if update.callback_query:
                msg.edit_text(report, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
            else:
                msg.edit_text(report, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        except BadRequest:
            pass

        return DASH_SHOW_REPORT

    # --- 5. FILTER MENU ---
    def show_filter_menu(self, update: Update, context: CallbackContext):
        query = update.callback_query
        category = query.data.replace("filter_menu_", "")
        context.user_data['dash_category'] = category
        context.user_data['dash_page'] = 0

        if category == "invoices":
            context.user_data['dash_filter'] = "All"
            return self.render_list(update, context)
        else:
            kb = [
                [InlineKeyboardButton("🟡 Open", callback_data="render_Open"),
                 InlineKeyboardButton("🟢 Completed", callback_data="render_Completed")],
                [InlineKeyboardButton("🔵 Processing", callback_data="render_Processing"),
                 InlineKeyboardButton("📋 All Items", callback_data="render_All")],
                [InlineKeyboardButton("🔙 Back to Report", callback_data="back_to_dash_report")]
            ]

        try:
            query.edit_message_text(f"🔎 *Filter {category.capitalize()}*\n━━━━━━━━━━━━━━━━━━", reply_markup=InlineKeyboardMarkup(kb),
                                    parse_mode=ParseMode.MARKDOWN)
        except BadRequest:
            pass
        return DASH_SELECT_STATUS_FILTER

    # --- 6. RENDER LIST ---
    def render_list(self, update: Update, context: CallbackContext):
        query = update.callback_query
        data_call = query.data

        if data_call.startswith("render_"):
            filter_type = data_call.replace("render_", "")
            context.user_data['dash_filter'] = filter_type
            context.user_data['dash_page'] = 0
        else:
            filter_type = context.user_data.get('dash_filter', 'All')

        category = context.user_data.get('dash_category')
        all_data = context.user_data.get('dash_data')

        if not all_data:
            query.answer("Session expired")
            return self.start_dashboard_flow(update, context)

        items_source = all_data[category]['list']
        filtered_items = []

        for i in items_source:
            raw_status = i.get('OverallSDProcessStatus', '')
            readable_status = "Open" if raw_status == 'A' else "Completed" if raw_status == 'C' else "Processing"
            if category == 'quotes' and raw_status == 'C': readable_status = "Completed"

            i['readable_status'] = readable_status

            if filter_type == "All" or filter_type == "all":
                filtered_items.append(i)
            elif filter_type == readable_status:
                filtered_items.append(i)

        if not filtered_items:
            query.answer(f"No items found for filter: {filter_type}", show_alert=True)
            return DASH_SELECT_STATUS_FILTER

        page = context.user_data.get('dash_page', 0)
        items_per_page = ITEMS_PER_PAGE
        start_idx = page * items_per_page
        end_idx = start_idx + items_per_page
        page_items = filtered_items[start_idx:end_idx]

        display_title = "Invoices" if category == "invoices" else f"{category.capitalize()} ({filter_type})"
        txt = f"*{display_title}* (Page {page + 1})\n{context.user_data.get('dash_period')}\n━━━━━━━━━━━━━━━━━━\n"

        status_labels = {"Open": "🟡", "Completed": "🟢", "Processing": "🔵"}

        for item in page_items:
            if category == "invoices":
                d_clean = self.sap_handler._parse_sap_date(item.get('BillingDocumentDate'))
                txt += f"🧾 `{item['BillingDocument']}`\n   💰 {item['TotalNetAmount']} {item['TransactionCurrency']} | 📅 {d_clean}\n\n"

            elif category == "orders":
                d_clean = self.sap_handler._parse_sap_date(item.get('CreationDate'))
                st = item['readable_status']
                icon = status_labels.get(st, "⚪")
                txt += f"📦 `{item['SalesOrder']}`\n   💰 {item['TotalNetAmount']} {item['TransactionCurrency']}\n   {icon} {st} | 📅 {d_clean}\n\n"

            elif category == "quotes":
                d_clean = self.sap_handler._parse_sap_date(item.get('CreationDate'))
                st = item['readable_status']
                icon = status_labels.get(st, "⚪")
                txt += f"📝 `{item['SalesQuotation']}`\n   💰 {item['TotalNetAmount']} {item['TransactionCurrency']}\n   {icon} {st} | 📅 {d_clean}\n\n"

        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data="list_prev"))
        if end_idx < len(filtered_items):
            nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data="list_next"))

        back_cb = "back_to_dash_report" if category == "invoices" else f"filter_menu_{category}"

        kb = []
        if nav_buttons: kb.append(nav_buttons)
        kb.append([InlineKeyboardButton("🔙 Back", callback_data=back_cb)])

        try:
            query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        except BadRequest:
            query.answer()

        return DASH_SELECT_STATUS_FILTER

    # --- HANDLE NEXT/PREV ---
    def handle_pagination(self, update: Update, context: CallbackContext):
        query = update.callback_query
        direction = query.data

        if direction == "list_next":
            context.user_data['dash_page'] += 1
        elif direction == "list_prev":
            context.user_data['dash_page'] = max(0, context.user_data['dash_page'] - 1)

        return self.render_list(update, context)

    def back_to_report(self, update: Update, context: CallbackContext):
        start = context.user_data.get('dash_start')
        end = context.user_data.get('dash_end')
        target = context.user_data.get('dash_target', 50000)
        return self._fetch_and_show_report(update, context, start, end, target)

    # --- COMPARISON MODE (Sprint 4) ---
    def start_comparison(self, update: Update, context: CallbackContext):
        """Ask for comparison period start date."""
        query = update.callback_query
        query.answer()

        # Store current period data for comparison
        context.user_data['compare_base_data'] = context.user_data.get('dash_data')
        context.user_data['compare_base_period'] = context.user_data.get('dash_period')

        query.edit_message_text(
            "📊 *Period Comparison*\n━━━━━━━━━━━━━━━━━━\n\n"
            "Enter **Start Date** for comparison period (DD-MM-YYYY):",
            parse_mode=ParseMode.MARKDOWN)
        return DASH_COMPARE_START

    def handle_compare_start(self, update: Update, context: CallbackContext):
        """Receive comparison start date."""
        text = update.message.text.strip()
        if len(text) != 10:
            update.message.reply_text("❌ Invalid Format. Use DD-MM-YYYY.")
            return DASH_COMPARE_START
        context.user_data['compare_start'] = text
        update.message.reply_text(
            "📅 **End Date** for comparison period (DD-MM-YYYY):",
            parse_mode=ParseMode.MARKDOWN)
        return DASH_COMPARE_END

    def handle_compare_end(self, update: Update, context: CallbackContext):
        """Fetch comparison data and show side-by-side report."""
        text = update.message.text.strip()
        if len(text) != 10:
            update.message.reply_text("❌ Invalid Format. Use DD-MM-YYYY.")
            return DASH_COMPARE_END

        compare_start = context.user_data.get('compare_start')
        compare_end = text

        msg = update.message.reply_text("🔄 *Fetching comparison data...*",
                                         parse_mode=ParseMode.MARKDOWN)

        compare_data = self.sap_handler.get_analytics_by_date(compare_start, compare_end)
        base_data = context.user_data.get('compare_base_data')
        base_period = context.user_data.get('compare_base_period', 'Current')

        if not compare_data or not base_data:
            msg.edit_text(
                "📭 No data for comparison period.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Back", callback_data="view_dashboard")]]))
            return DASH_SELECT_PERIOD

        # Calculate deltas
        base_rev = base_data['invoices']['val']
        comp_rev = compare_data['invoices']['val']
        rev_delta = base_rev - comp_rev
        rev_pct = ((rev_delta / comp_rev) * 100) if comp_rev > 0 else 0

        base_orders = base_data['orders']['total']
        comp_orders = compare_data['orders']['total']

        base_quotes = base_data['quotes']['total']
        comp_quotes = compare_data['quotes']['total']

        delta_icon = "📈" if rev_delta >= 0 else "📉"
        delta_sign = "+" if rev_delta >= 0 else ""

        report = (
            f"📊 *Period Comparison*\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
            f"📅 *Current:* {base_period}\n"
            f"📅 *Compare:* {compare_start} - {compare_end}\n\n"
            f"💰 *Revenue*\n"
            f"   Current: `{base_rev:,.2f}`\n"
            f"   Compare: `{comp_rev:,.2f}`\n"
            f"   {delta_icon} Change: `{delta_sign}{rev_delta:,.2f}` ({delta_sign}{rev_pct:.1f}%)\n\n"
            f"📦 *Orders*\n"
            f"   Current: `{base_orders}` | Compare: `{comp_orders}`\n\n"
            f"📝 *Quotes*\n"
            f"   Current: `{base_quotes}` | Compare: `{comp_quotes}`\n"
        )

        kb = [
            [InlineKeyboardButton("🔙 Back to Report", callback_data="back_to_dash_report")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
        ]

        try:
            msg.edit_text(report, reply_markup=InlineKeyboardMarkup(kb),
                          parse_mode=ParseMode.MARKDOWN)
        except BadRequest:
            pass

        return DASH_SHOW_REPORT

    def get_handlers(self):
        return [
            CallbackQueryHandler(self.start_dashboard_flow, pattern='^view_dashboard$'),
            CallbackQueryHandler(self.handle_pagination, pattern='^list_next$|^list_prev$')
        ]
