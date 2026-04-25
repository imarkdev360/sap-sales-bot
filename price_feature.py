from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode
from telegram.ext import CallbackContext, MessageHandler, Filters, CallbackQueryHandler
from states import PRODUCT_SEARCH_INPUT
from logger_setup import get_logger

logger = get_logger(__name__)


class PriceFeature:
    def __init__(self, sap_handler, db_handler):
        self.sap_handler = sap_handler
        self.db = db_handler

    def escape_markdown(self, text):
        if not text: return ""
        return str(text).replace('_', '\\_').replace('*', '\\*').replace('`', '\\`').replace('[', '\\[')

    def start_price_check(self, update: Update, context: CallbackContext):
        query = update.callback_query
        query.answer()

        if query.data.startswith("check_price_"):
            bp_id = query.data.replace("check_price_", "")
            context.user_data['current_customer_id'] = bp_id
        else:
            bp_id = context.user_data.get('current_customer_id', 'Unknown')

        txt = (f"🏷️ *Price Checker*\n👤 Customer: `{bp_id}`\n━━━━━━━━━━━━━━━━━━\n🔍 *Type Product Name (e.g. Ball) or ID:*")
        keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data=f"customer_details_view_{bp_id}")]]

        query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
        return PRODUCT_SEARCH_INPUT

    def handle_product_search(self, update: Update, context: CallbackContext):
        text = update.message.text.strip()
        bp_id = context.user_data.get('current_customer_id')

        if text.isdigit():
            update.message.reply_text(f"🔄 Checking Price for ID: `{text}`...")
            price = self.sap_handler.get_product_price(text, bp_id)
            if "0.00 EUR" not in price:
                return self._show_price_result(text, f"Item {text}", update, context)

        msg = update.message.reply_text("🔍 Searching SAP...")
        prods = self.sap_handler.search_products(text)

        if not prods:
            msg.edit_text("❌ Product Not Found. Try a different name or ID.",
                          reply_markup=InlineKeyboardMarkup(
                              [[InlineKeyboardButton("🔙 Back", callback_data=f"customer_details_view_{bp_id}")]]))
            return PRODUCT_SEARCH_INPUT

        context.user_data['price_search_results'] = prods
        context.user_data['price_search_page'] = 0

        msg.delete()
        return self.render_search_page(update, context)

    def render_search_page(self, update, context):
        results = context.user_data.get('price_search_results', [])
        page = context.user_data.get('price_search_page', 0)
        bp_id = context.user_data.get('current_customer_id')

        start = page * 5
        end = start + 5
        chunk = results[start:end]

        kb = []
        for item in chunk:
            safe_name = item['name'][:30]
            btn_text = f"📦 {safe_name} ({item['id']})"
            kb.append([InlineKeyboardButton(btn_text, callback_data=f"chk_price_{item['id']}")])

        nav = []
        if page > 0: nav.append(InlineKeyboardButton("⬅️ Prev", callback_data="price_prev"))
        if end < len(results): nav.append(InlineKeyboardButton("Next ➡️", callback_data="price_next"))
        if nav: kb.append(nav)

        kb.append([InlineKeyboardButton("❌ Cancel", callback_data=f"customer_details_view_{bp_id}")])

        msg = f"🔍 Found {len(results)} products. Page {page + 1}:"

        if update.callback_query:
            update.callback_query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(kb))
        else:
            update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(kb))

        return PRODUCT_SEARCH_INPUT

    def handle_search_pagination(self, update: Update, context: CallbackContext):
        query = update.callback_query
        page = context.user_data.get('price_search_page', 0)
        if query.data == "price_next":
            page += 1
        elif query.data == "price_prev":
            page -= 1
        context.user_data['price_search_page'] = page
        return self.render_search_page(update, context)

    def handle_price_selection(self, update: Update, context: CallbackContext):
        query = update.callback_query
        mat_id = query.data.split('_')[-1]

        results = context.user_data.get('price_search_results', [])
        name = next((i['name'] for i in results if str(i['id']) == str(mat_id)), f"Item {mat_id}")

        return self._show_price_result(mat_id, name, update, context, is_callback=True)

    def _show_price_result(self, mat_id, name, update, context, is_callback=False):
        bp_id = context.user_data.get('current_customer_id')
        safe_name = self.escape_markdown(name)

        if is_callback:
            update.callback_query.answer("Fetching price...")
            msg_obj = update.callback_query.message
            msg_obj.edit_text(f"🔄 Checking price for *{safe_name}*...", parse_mode=ParseMode.MARKDOWN)
        else:
            msg_obj = update.message

        price = self.sap_handler.get_product_price(mat_id, bp_id)
        self.db.log_event(update.effective_user, "PRICE_CHECK", f"Cust: {bp_id} | Prod: {mat_id} | Price: {price}")

        msg = (f"🏷️ *Price Result*\n━━━━━━━━━━━━━━━━━━\n"
               f"📦 Product: *{safe_name}*\n"
               f"🆔 Material: `{mat_id}`\n━━━━━━━━━━━━━━━━━━\n"
               f"💰 **Rate: `{price}`**")

        keyboard = [
            [InlineKeyboardButton("🔄 Search Again", callback_data=f"check_price_{bp_id}")],
            [InlineKeyboardButton("🔙 Back to Customer", callback_data=f"customer_details_view_{bp_id}")]
        ]

        if is_callback:
            msg_obj.edit_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
        else:
            msg_obj.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

        return PRODUCT_SEARCH_INPUT

    def get_handlers(self):
        return [
            MessageHandler(Filters.text & ~Filters.command, self.handle_product_search),
            CallbackQueryHandler(self.handle_search_pagination, pattern='^price_next$|^price_prev$'),
            CallbackQueryHandler(self.handle_price_selection, pattern='^chk_price_'),
            CallbackQueryHandler(self.start_price_check, pattern='^check_price_')
        ]
