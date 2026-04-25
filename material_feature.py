from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode, InlineQueryResultArticle, \
    InputTextMessageContent
from telegram.ext import CallbackContext
from telegram.error import BadRequest
from uuid import uuid4
from config import STOCK_LOW_THRESHOLD
from states import MATERIAL_SEARCH_INPUT
from logger_setup import get_logger

logger = get_logger(__name__)


class MaterialFeature:
    def __init__(self, sap_handler, db_handler):
        self.sap = sap_handler
        self.db = db_handler

    def start_material_search(self, update: Update, context: CallbackContext):
        if update.callback_query:
            query = update.callback_query
            query.answer()
            msg_func = query.edit_message_text
        else:
            msg_func = update.message.reply_text

        txt = (
            "🔍 *Material Search*\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "Choose a search method:\n\n"
            "🔎 *Live Search:* Click the button below for auto-suggestions.\n"
            "⌨️ *Manual Search:* Type the **Name** or **ID** directly here."
        )

        kb = [
            [InlineKeyboardButton("🔎 Start Live Search", switch_inline_query_current_chat="")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
        ]

        try:
            msg_func(txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        except BadRequest:
            pass

        return MATERIAL_SEARCH_INPUT

    def handle_search_input(self, update: Update, context: CallbackContext):
        keyword = update.message.text.strip()
        user = update.message.from_user

        msg = update.message.reply_text(f"🔍 Searching for _'{keyword}'_ ...", parse_mode=ParseMode.MARKDOWN)

        results = self.sap.search_products(keyword)
        self.db.log_event(user, "MATERIAL_SEARCH_MANUAL", f"Keyword: {keyword} | Found: {len(results)}")

        if not results:
            msg.edit_text(
                f"❌ *No matches found for '{keyword}'*\n"
                "Please check the ID or Description.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Search Again", callback_data="start_material_search")],
                    [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return MATERIAL_SEARCH_INPUT

        kb = []
        for item in results[:8]:
            btn_text = f"📦 {item['name']} ({item['id']})"
            kb.append([InlineKeyboardButton(btn_text, callback_data=f"view_material_{item['id']}")])

        kb.append([
            InlineKeyboardButton("🔄 Search Again", callback_data="start_material_search"),
            InlineKeyboardButton("❌ Cancel", callback_data="main_menu")
        ])

        msg.edit_text(
            f"🔍 *Search Results ({len(results)} found)*\n"
            f"━━━━━━━━━━━━━━━━━━\n👇 Select item to view details:",
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode=ParseMode.MARKDOWN
        )
        return MATERIAL_SEARCH_INPUT

    def show_material_details_callback(self, update: Update, context: CallbackContext):
        query = update.callback_query
        material_id = query.data.split('_')[-1]
        self._send_material_card(update, context, material_id, is_callback=True)
        return MATERIAL_SEARCH_INPUT

    def handle_inline_query(self, update: Update, context: CallbackContext):
        query = update.inline_query.query.strip()
        if not query: return

        results = self.sap.search_products(query)
        articles = []

        for item in results[:50]:
            articles.append(
                InlineQueryResultArticle(
                    id=str(uuid4()),
                    title=f"{item['name']}",
                    description=f"ID: {item['id']} | Tap to View",
                    input_message_content=InputTextMessageContent(f"/mat_detail_{item['id']}")
                )
            )
        update.inline_query.answer(articles, cache_time=0)

    def show_material_details_from_command(self, update: Update, context: CallbackContext):
        material_id = update.message.text.split('_')[-1]
        self._send_material_card(update, context, material_id, is_callback=False)

    def _send_material_card(self, update, context, material_id, is_callback):
        if is_callback:
            update.callback_query.answer("Retrieving data...")
            message_obj = update.callback_query
        else:
            message_obj = update.message

        stock_data = self.sap.get_stock_overview(material_id)
        # Use customer-specific pricing for B2B users
        b2b_bp = context.user_data.get('b2b_bp_id')
        price_txt = self.sap.get_product_price(material_id, b2b_bp)

        qty = stock_data['total'] if stock_data else 0.0
        unit = stock_data['unit'] if stock_data else "PC"

        if qty <= 0:
            status_icon = "🔴"
            status_text = "OUT OF STOCK"
            stock_bar = "░░░░░░░░░░"
        elif qty < STOCK_LOW_THRESHOLD:
            status_icon = "🟡"
            status_text = "LOW STOCK"
            stock_bar = "███░░░░░░░"
        else:
            status_icon = "🟢"
            status_text = "IN STOCK"
            stock_bar = "██████████"

        loc_text = ""
        if stock_data and stock_data['breakdown']:
            for plant, locations in stock_data['breakdown'].items():
                p_total = sum(locations.values())
                loc_text += f"*Plant {plant}* `(Total: {p_total:,.0f} {unit})`\n"

                sorted_locs = sorted(locations.items())
                for i, (loc, q) in enumerate(sorted_locs):
                    is_last = (i == len(sorted_locs) - 1)
                    branch = "   L" if is_last else "   +"
                    loc_text += f"{branch} *{loc}:* `{q:,.0f}`\n"
                loc_text += "\n"
        else:
            loc_text = "   _No inventory data available._"

        now = datetime.now().strftime("%d-%b-%Y %H:%M")

        msg = (
            f"📦 *MATERIAL DATA REPORT*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🆔 *Material ID:* `{material_id}`\n"
            f"🏷️ *{'Your Price' if b2b_bp else 'Std. Price'}:* `{price_txt}`\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 *GLOBAL STOCK STATUS*\n"
            f"{status_icon} *{status_text}* |  Total: `{qty:,.0f} {unit}`\n"
            f"`[{stock_bar}]`\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🏭 *WAREHOUSE DISTRIBUTION*\n"
            f"{loc_text}"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🕐 _Generated: {now}_"
        )

        kb = [
            [InlineKeyboardButton("🔄 Search Again", callback_data="start_material_search")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
        ]

        if is_callback:
            message_obj.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        else:
            message_obj.reply_text(msg, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)

        user = update.effective_user
        self.db.log_event(user, "STOCK_VIEW", f"ID: {material_id}")
