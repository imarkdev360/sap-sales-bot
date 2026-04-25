from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode
from telegram.ext import CallbackContext, CallbackQueryHandler
from telegram.error import BadRequest
from states import SALES_MENU
from logger_setup import get_logger

logger = get_logger(__name__)


class SalesFeature:
    def __init__(self, sap_handler, db_handler):
        self.sap_handler = sap_handler
        self.db = db_handler

    def show_sales_menu(self, update: Update, context: CallbackContext):
        query = update.callback_query
        try:
            query.answer()
        except Exception:
            pass

        kb = [
            [InlineKeyboardButton("📋 View Orders", callback_data="view_orders_0"),
             InlineKeyboardButton("📝 View Quotes", callback_data="view_quotes_0")],
            [InlineKeyboardButton("➕ New Order", callback_data="start_create_order"),
             InlineKeyboardButton("➕ New Quote", callback_data="start_create_quote")],
            [InlineKeyboardButton("🔄 Create Order from Quote", callback_data="start_quote_conversion")],
            [InlineKeyboardButton("📂 View Saved PDFs", callback_data="view_pdf_history")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
        ]

        try:
            query.edit_message_text(
                "📦 *Sales & Quotes Center*\n━━━━━━━━━━━━━━━━━━\n📋 Manage your Sales Pipeline",
                reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        except BadRequest:
            pass
        return SALES_MENU

    def view_orders(self, update: Update, context: CallbackContext):
        query = update.callback_query
        if query.data.startswith("view_order_detail_"): return self.show_details(
            query.data.replace("view_order_detail_", ""), update, "Order")
        page = int(query.data.split('_')[-1])
        orders = self.sap_handler.get_sales_orders(skip=page * 5, top=5)
        kb = []
        txt = f"📋 *Sales Orders* (Page {page + 1})\n━━━━━━━━━━━━━━━━━━\n👇 *Select an Order to View:*\n"
        if orders:
            for o in orders:
                btn_text = f"📦 {o['SalesOrder']}  |  {o['TotalNetAmount']} {o['TransactionCurrency']}"
                kb.append([InlineKeyboardButton(btn_text, callback_data=f"view_order_detail_{o['SalesOrder']}")])
            nav = []
            if page > 0: nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"view_orders_{page - 1}"))
            if len(orders) == 5: nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"view_orders_{page + 1}"))
            kb.append(nav)
            kb.append([InlineKeyboardButton("🔙 Back to Menu", callback_data="sales_menu")])
        else:
            txt = "📭 *No Orders Found*"
            kb = [[InlineKeyboardButton("🔙 Back to Menu", callback_data="sales_menu")]]
        try:
            query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        except BadRequest:
            pass
        return SALES_MENU

    def view_quotes(self, update: Update, context: CallbackContext):
        query = update.callback_query
        if query.data.startswith("view_quote_detail_"): return self.show_details(
            query.data.replace("view_quote_detail_", ""), update, "Quote")
        page = int(query.data.split('_')[-1])
        quotes = self.sap_handler.get_quotations(skip=page * 5, top=5)
        kb = []
        txt = f"📝 *Sales Quotations* (Page {page + 1})\n━━━━━━━━━━━━━━━━━━\n👇 *Select a Quote to View:*\n"
        if quotes:
            for q in quotes:
                kb.append([InlineKeyboardButton(f"📝 {q['SalesQuotation']} | {q['TotalNetAmount']}",
                                                callback_data=f"view_quote_detail_{q['SalesQuotation']}")])
            nav = []
            if page > 0: nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"view_quotes_{page - 1}"))
            if len(quotes) == 5: nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"view_quotes_{page + 1}"))
            kb.append(nav)
            kb.append([InlineKeyboardButton("🔙 Back", callback_data="sales_menu")])
        else:
            txt = "📭 *No Quotes Found*"
            kb = [[InlineKeyboardButton("🔙 Back", callback_data="sales_menu")]]
        try:
            query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        except BadRequest:
            pass
        return SALES_MENU

    def show_detail_router(self, update: Update, context: CallbackContext):
        data = update.callback_query.data
        if "view_order_detail_" in data: return self.show_details(data.replace("view_order_detail_", ""), update,
                                                                  "Order")
        if "view_quote_detail_" in data: return self.show_details(data.replace("view_quote_detail_", ""), update,
                                                                  "Quote")
        return SALES_MENU

    def show_details(self, oid, update, doc_type):
        d = self.sap_handler.get_sales_order_details(
            oid) if doc_type == "Order" else self.sap_handler.get_quotation_details(oid)
        if d:
            self._send_order_card(update.callback_query, d, doc_type, edit=True)
        else:
            try:
                update.callback_query.edit_message_text("❌ Error loading details.", reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Back", callback_data="sales_menu")]]))
            except Exception:
                pass
        return SALES_MENU

    def _send_order_card(self, messageable, d, title, edit=False):
        status_map = {'A': '🟡 Open', 'B': '🔵 Processing', 'C': '🟢 Completed', '': '🟡 Open'}
        status_icon = status_map.get(d.get('status'), '⚪ Unknown')
        links = ""
        if title == "Order": links = f"📝 **Quote:** `{d.get('quote_link', 'N/A')}`\n🧾 **Invoice:** `{d.get('invoice_link', 'Pending')}`\n"
        title_emoji = "📦" if title == "Order" else "📝"
        msg = (
            f"{title_emoji} *{title} Details: `{d['id']}`*\n━━━━━━━━━━━━━━━━━━\n"
            f"📅 *Date:* {d['date']}\n👤 *Customer:* `{d['customer']}`\n🔖 *Ref:* `{d['ref']}`\n"
            f"{links}📊 *Status:* {status_icon}\n━━━━━━━━━━━━━━━━━━\n🛒 *Line Items:*\n")
        for i in d['items']: msg += f"  • *{i['desc']}*\n     📦 Qty: `{i['qty']}` | 💵 Net: `{i['net']}`\n"
        msg += f"━━━━━━━━━━━━━━━━━━\n💰 **TOTAL:** `{d['total']}`"
        back_data = "view_orders_0" if title == "Order" else "view_quotes_0"

        kb = []

        if title == "Order":
            kb.append([InlineKeyboardButton("📄 Generate PDF", callback_data=f"gen_pdf_{d['id']}")])
        else:
            kb.append([InlineKeyboardButton("📦 Create Order", callback_data=f"convert_qt_{d['id']}")])

        kb.append([InlineKeyboardButton(f"🔙 Back to {title}s", callback_data=back_data),
                   InlineKeyboardButton("🏠 Menu", callback_data="main_menu")])

        try:
            if edit:
                messageable.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
            else:
                messageable.reply_text(msg, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        except BadRequest:
            pass

    def get_handlers(self):
        return [
            CallbackQueryHandler(self.show_sales_menu, pattern='^sales_menu$'),
            CallbackQueryHandler(self.view_orders, pattern='^view_orders'),
            CallbackQueryHandler(self.view_quotes, pattern='^view_quotes'),
            CallbackQueryHandler(self.show_detail_router, pattern='^view_order_detail_|^view_quote_detail_')
        ]
