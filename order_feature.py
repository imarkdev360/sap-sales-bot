from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode, InlineQueryResultArticle, \
    InputTextMessageContent
from telegram.ext import CallbackContext, ConversationHandler
from config import DISCOUNT_THRESHOLD
from notification_service import NotificationService
from pdf_manager import PDFManager
from uuid import uuid4
from states import (
    CUSTOMER_MENU, ORDER_ASK_CUSTOMER, ORDER_ASK_REF, ORDER_ASK_MATERIAL,
    ORDER_ASK_QTY, ORDER_ADD_MORE, ORDER_CONFIRM, ORDER_REMOVE_ITEM,
    ORDER_ASK_DISCOUNT, ORDER_ASK_QUOTE_ID, ORDER_ASK_VALIDITY, MAIN_MENU,
)
from logger_setup import get_logger

logger = get_logger(__name__)


class OrderFeature:
    def __init__(self, sap_handler, db_handler):
        self.sap_handler = sap_handler
        self.db = db_handler
        self.pdf_manager = PDFManager(self.sap_handler, self.db)
        self.notifier = NotificationService(self.db)

    def _get_sap(self, context):
        """Return the proxy-aware SAP handler for the current user.
        If b2b_bp_id is set in context, returns the B2B proxy."""
        b2b_bp = context.user_data.get('b2b_bp_id')
        if b2b_bp:
            from b2b_secure_handler import B2BSecureSAPHandler
            return B2BSecureSAPHandler(self.sap_handler, b2b_bp)
        return self.sap_handler

    def _init_flow(self, context, cust_id=None, doc_type="ORDER"):
        context.user_data['order_cart'] = []
        context.user_data['doc_type'] = doc_type
        context.user_data['order_discount'] = 0.0
        context.user_data['quote_valid_to'] = None
        if cust_id: context.user_data['order_cust_id'] = cust_id

    # --- START FUNCTIONS ---
    def start_create_order_standalone(self, update: Update, context: CallbackContext):
        query = update.callback_query;
        query.answer()
        self._init_flow(context, doc_type="ORDER")
        self.db.log_event(query.from_user, "START_ORDER", "Started Order Flow")
        # Live search for Customer
        kb = [[InlineKeyboardButton("🔎 Live Search", switch_inline_query_current_chat="cust ")],
              [InlineKeyboardButton("🔙 Cancel", callback_data="sales_menu")]]
        query.edit_message_text("🆕 *New Sales Order*\n━━━━━━━━━━━━━━━━━━\n\n🔢 Enter **Customer ID** or **Name**:",
                                reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        return ORDER_ASK_CUSTOMER

    def start_create_quote_standalone(self, update: Update, context: CallbackContext):
        query = update.callback_query;
        query.answer()
        self._init_flow(context, doc_type="QUOTE")
        self.db.log_event(query.from_user, "START_QUOTE", "Started Quote Flow")
        # Live search for Customer (Works for Quotes too)
        kb = [[InlineKeyboardButton("🔎 Live Search", switch_inline_query_current_chat="cust ")],
              [InlineKeyboardButton("🔙 Cancel", callback_data="sales_menu")]]
        query.edit_message_text("🆕 *New Quotation*\n━━━━━━━━━━━━━━━━━━\n\n🔢 Enter **Customer ID** or **Name**:",
                                reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        return ORDER_ASK_CUSTOMER

    def start_transaction_from_customer(self, update: Update, context: CallbackContext):
        query = update.callback_query;
        query.answer()
        parts = query.data.split('_');
        action = parts[1];
        cust_id = parts[2]
        doc_type = "QUOTE" if action == "quote" else "ORDER"
        self._init_flow(context, cust_id, doc_type)
        sa = self.sap_handler.get_customer_sales_area(cust_id)
        if not sa:
            query.edit_message_text("❌ No Sales Area found.", reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔙 Back", callback_data=f"customer_details_view_{cust_id}")]]))
            return CUSTOMER_MENU
        context.user_data['order_sa'] = sa
        query.edit_message_text(
            f"🛒 *Create {doc_type}: {cust_id}*\n🏢 Org: `{sa['SalesOrganization']}`\n━━━━━━━━━━━━━━━━━━\n🔖 **Enter Reference (PO):**",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("Cancel", callback_data=f"customer_details_view_{cust_id}")]]),
            parse_mode=ParseMode.MARKDOWN)
        return ORDER_ASK_REF

    # --- INPUT HANDLERS ---
    def handle_customer_input(self, update: Update, context: CallbackContext):
        text = update.message.text.strip()
        if text.startswith('/select_cust_'):
            cust_id = text.split('_')[-1]
            return self._select_customer(cust_id, update, context)
        results = self.sap_handler.search_customers(text)
        if not results:
            update.message.reply_text(f"❌ Customer '{text}' not found.\nTry Live Search:",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔎 Live Search",
                                                                                               switch_inline_query_current_chat="cust ")]]))
            return ORDER_ASK_CUSTOMER
        context.user_data['search_results'] = results
        context.user_data['search_page'] = 0
        context.user_data['search_context'] = 'CUSTOMER'
        return self.render_search_page(update, context)

    def handle_material_input(self, update: Update, context: CallbackContext):
        text = update.message.text.strip()
        if text.startswith('/select_mat_'):
            return self._select_material(text.split('_')[-1], "Selected Item", update, context)
        prods = self.sap_handler.search_products(text)
        if not prods:
            update.message.reply_text("❌ Product not found.")
            return ORDER_ASK_MATERIAL
        context.user_data['search_results'] = prods
        context.user_data['search_page'] = 0
        context.user_data['search_context'] = 'MATERIAL'
        return self.render_search_page(update, context)

    def handle_search_pagination(self, update: Update, context: CallbackContext):
        query = update.callback_query
        page = context.user_data.get('search_page', 0)
        if query.data == "search_next":
            page += 1
        elif query.data == "search_prev":
            page -= 1
        context.user_data['search_page'] = page
        return self.render_search_page(update, context)

    def render_search_page(self, update, context):
        results = context.user_data.get('search_results', [])
        page = context.user_data.get('search_page', 0)
        stype = context.user_data.get('search_context', 'CUSTOMER')
        start = page * 5;
        end = start + 5
        chunk = results[start:end]
        kb = []
        for item in chunk:
            if stype == 'CUSTOMER':
                btn = InlineKeyboardButton(f"👤 {item['CustomerName']} ({item['Customer']})",
                                           callback_data=f"sel_cust_{item['Customer']}")
            else:
                btn = InlineKeyboardButton(f"📦 {item['name']} ({item['id']})", callback_data=f"sel_mat_{item['id']}")
            kb.append([btn])
        nav = []
        if page > 0: nav.append(InlineKeyboardButton("⬅️ Prev", callback_data="search_prev"))
        if end < len(results): nav.append(InlineKeyboardButton("Next ➡️", callback_data="search_next"))
        if nav: kb.append(nav)
        kb.append(
            [InlineKeyboardButton("❌ Cancel", callback_data="sales_menu" if stype == 'CUSTOMER' else "main_menu")])
        msg = f"🔍 Found {len(results)} matches. Showing {start + 1}-{min(end, len(results))}:"
        if update.callback_query:
            update.callback_query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(kb))
        else:
            update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(kb))
        return ORDER_ASK_CUSTOMER if stype == 'CUSTOMER' else ORDER_ASK_MATERIAL

    def handle_customer_select_callback(self, update: Update, context: CallbackContext):
        return self._select_customer(update.callback_query.data.split('_')[-1], update, context, True)

    def handle_material_select_callback(self, update: Update, context: CallbackContext):
        mat_id = update.callback_query.data.split('_')[-1]
        return self._select_material(mat_id, "Selected Item", update, context, True)

    def _select_customer(self, cust_id, update, context, is_callback=False):
        sa = self.sap_handler.get_customer_sales_area(cust_id)
        if not sa:
            msg = "❌ Sales Area not found."
            if is_callback:
                update.callback_query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Back", callback_data="sales_menu")]]))
            else:
                update.message.reply_text(msg)
            return ORDER_ASK_CUSTOMER
        context.user_data['order_cust_id'] = cust_id
        context.user_data['order_sa'] = sa
        txt = f"✅ Customer: `{cust_id}`\n🔖 **Enter Reference (PO Number):**"
        if is_callback:
            update.callback_query.edit_message_text(txt, parse_mode=ParseMode.MARKDOWN)
        else:
            update.message.reply_text(txt, parse_mode=ParseMode.MARKDOWN)
        return ORDER_ASK_REF

    def handle_ref_input(self, update: Update, context: CallbackContext):
        context.user_data['order_ref'] = update.message.text.strip()
        # Live search for Material (Works for Quote and Order)
        kb = [[InlineKeyboardButton("🔎 Live Search Material", switch_inline_query_current_chat="order ")]]
        update.message.reply_text("👇 **Select Material**\nEnter **ID** or **Name**:",
                                  reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        return ORDER_ASK_MATERIAL

    def _select_material(self, mat_id, desc, update, context, is_callback=False):
        price = self.sap_handler.get_product_price(mat_id, context.user_data.get('order_cust_id'))
        try:
            unit_p, curr = float(price.split(' ')[0].replace(',', '')), price.split(' ')[1]
        except (ValueError, IndexError):
            unit_p, curr = 0.0, "EUR"
        context.user_data['current_item'] = {'Material': mat_id, 'Desc': desc, 'UnitPrice': unit_p, 'Currency': curr}
        txt = f"📦 *{desc}*\n🆔 `{mat_id}`\n🏷️ Price: `{unit_p} {curr}`\n\n🔢 **Enter Quantity:**"
        if is_callback:
            update.callback_query.edit_message_text(txt, parse_mode=ParseMode.MARKDOWN)
        else:
            update.message.reply_text(txt, parse_mode=ParseMode.MARKDOWN)
        return ORDER_ASK_QTY

    def handle_qty_input(self, update: Update, context: CallbackContext):
        if not update.message.text.isdigit(): update.message.reply_text("⚠️ Enter number."); return ORDER_ASK_QTY
        qty = int(update.message.text)
        self.db.log_event(update.effective_user, "INPUT_QUANTITY", f"Qty: {qty}")
        stock = self.sap_handler.get_stock_overview(context.user_data['current_item']['Material'])
        avail = stock['total'] if stock else 0.0
        if qty > avail:
            context.user_data['temp_qty'] = qty
            kb = [[InlineKeyboardButton("✅ Proceed", callback_data="confirm_qty_yes"),
                   InlineKeyboardButton("🔄 Change", callback_data="confirm_qty_no")]]
            update.message.reply_text(f"⚠️ *LOW STOCK*\nReq: {qty} | Avail: {avail}\nProceed?",
                                      reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
            return ORDER_ADD_MORE
        return self._add_item_to_cart(update, context, qty)

    def _add_item_to_cart(self, update, context, qty):
        item = context.user_data['current_item']
        item['Quantity'] = qty
        item['LineTotal'] = item['UnitPrice'] * qty
        context.user_data['order_cart'].append(item)
        return self.ask_add_more(update, context)

    def ask_add_more(self, update, context):
        cart = context.user_data.get('order_cart', [])
        subtotal = sum(i['LineTotal'] for i in cart)
        kb = [[InlineKeyboardButton("➕ Add Another Item", callback_data="order_add_more")],
              [InlineKeyboardButton("➡️ Finish & Next", callback_data="goto_discount")]]
        msg = f"🛒 **Items:** {len(cart)} | **Subtotal:** {subtotal:.2f}\nAdd more items or proceed?"
        if update.callback_query:
            update.callback_query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(kb),
                                                    parse_mode=ParseMode.MARKDOWN)
        else:
            update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        return ORDER_ADD_MORE

    def handle_add_more_choice(self, update: Update, context: CallbackContext):
        query = update.callback_query;
        data = query.data
        if data == "confirm_qty_yes": return self._add_item_to_cart(update, context, context.user_data.pop('temp_qty'))
        if data == "confirm_qty_no": query.edit_message_text("🔢 **Enter New Quantity:**",
                                                             parse_mode=ParseMode.MARKDOWN); return ORDER_ASK_QTY
        if data == "order_add_more":
            kb = [[InlineKeyboardButton("🔎 Live Search", switch_inline_query_current_chat="order ")]]
            query.edit_message_text("👇 **Select Next Material:**", reply_markup=InlineKeyboardMarkup(kb))
            return ORDER_ASK_MATERIAL
        if data == "goto_discount": return self.prompt_discount(update, context)
        if data == "order_remove_item": return self.start_remove_flow(update, context)
        return ORDER_ADD_MORE

    def start_remove_flow(self, update: Update, context: CallbackContext):
        cart = context.user_data.get('order_cart', [])
        if not cart: return self.show_final_review(update, context)
        kb = [[InlineKeyboardButton(f"🗑 {i['Desc']} (x{i['Quantity']})", callback_data=f"del_cart_idx_{idx}") for idx, i
               in enumerate(cart)]]
        kb.append([InlineKeyboardButton("🔙 Back", callback_data="back_to_review")])
        update.callback_query.edit_message_text("🗑 *Remove Item*", reply_markup=InlineKeyboardMarkup(kb),
                                                parse_mode=ParseMode.MARKDOWN)
        return ORDER_REMOVE_ITEM

    def handle_remove_selection(self, update: Update, context: CallbackContext):
        query = update.callback_query;
        data = query.data
        if data == "back_to_review": return self.show_final_review(update, context)
        if data.startswith("del_cart_idx_"):
            try:
                context.user_data['order_cart'].pop(int(data.split('_')[-1]))
            except (IndexError, ValueError):
                pass
            return self.show_final_review(update, context)

    def prompt_discount(self, update: Update, context: CallbackContext):
        query = update.callback_query
        kb = [[InlineKeyboardButton("⏭️ Skip (0%)", callback_data="skip_discount")]]
        query.edit_message_text("📉 **Enter Header Discount %**", reply_markup=InlineKeyboardMarkup(kb),
                                parse_mode=ParseMode.MARKDOWN)
        return ORDER_ASK_DISCOUNT

    def handle_discount_input(self, update: Update, context: CallbackContext):
        if update.callback_query and update.callback_query.data == "skip_discount":
            disc = 0.0
        else:
            try:
                disc = float(update.message.text.strip().replace('%', ''))
            except ValueError:
                update.message.reply_text("❌ Invalid.");
                return ORDER_ASK_DISCOUNT
        context.user_data['order_discount'] = disc

        # ✅ If Quote, Ask Date. If Order, Show Summary.
        if context.user_data.get('doc_type') == "QUOTE":
            return self.ask_validity_date(update, context)
        return self.show_final_review(update, context)

    def ask_validity_date(self, update: Update, context: CallbackContext):
        msg_obj = update.callback_query.message if update.callback_query else update.message
        kb = [
            [InlineKeyboardButton("📅 Today", callback_data="valid_date_0"),
             InlineKeyboardButton("📅 Tomorrow", callback_data="valid_date_1")],
            [InlineKeyboardButton("🔙 Back", callback_data="goto_discount")]
        ]
        txt = "📅 *Set Quotation Validity*\n━━━━━━━━━━━━━━━━━━\nValid From: *Today*\n\n👇 Select Option or Type Date (DD.MM.YYYY):"
        if update.callback_query:
            msg_obj.edit_text(txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        else:
            msg_obj.reply_text(txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        return ORDER_ASK_VALIDITY

    def handle_validity_selection(self, update: Update, context: CallbackContext):
        if update.callback_query:
            query = update.callback_query
            if query.data == "goto_discount": return self.prompt_discount(update, context)
            if query.data.startswith("valid_date_"):
                days = int(query.data.split("_")[-1])
                target = datetime.now() + timedelta(days=days)
                context.user_data['quote_valid_to'] = target.strftime("%Y-%m-%d")
                return self.show_final_review(update, context)

        if update.message and update.message.text:
            text = update.message.text.strip()
            for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
                try:
                    dt = datetime.strptime(text, fmt)
                    context.user_data['quote_valid_to'] = dt.strftime("%Y-%m-%d")
                    return self.show_final_review(update, context)
                except ValueError:
                    continue
            update.message.reply_text("❌ Invalid Date Format.")
            return ORDER_ASK_VALIDITY

    def show_final_review(self, update: Update, context: CallbackContext):
        doc_type = context.user_data.get('doc_type', 'ORDER')
        cart = context.user_data.get('order_cart', [])
        cust = context.user_data.get('order_cust_id')
        discount = context.user_data.get('order_discount', 0.0)
        valid_to = context.user_data.get('quote_valid_to', 'N/A')

        subtotal = sum(i['LineTotal'] for i in cart)
        discount_val = (subtotal * discount) / 100
        net_total = subtotal - discount_val
        curr = cart[0]['Currency'] if cart else "EUR"

        txt = f"📝 *Summary ({doc_type})*\n👤 Cust: `{cust}`\n"
        if doc_type == "QUOTE": txt += f"📅 Valid To: `{valid_to}`\n"
        txt += "━━━━━━━━━━━━━━━━━━\n"
        for i, item in enumerate(cart, 1): txt += f"• {item['Desc']} x {item['Quantity']} = {item['LineTotal']:.2f}\n"
        txt += f"━━━━━━━━━━━━━━━━━━\n💵 Subtotal: {subtotal:.2f}\n📉 Disc ({discount}%): -{discount_val:.2f}\n💰 **NET: {net_total:.2f} {curr}**"

        kb = [[InlineKeyboardButton("✅ Confirm & Create", callback_data="confirm_order_yes")],
              [InlineKeyboardButton("❌ Cancel", callback_data="cancel_order_flow")]]

        edit_row = [InlineKeyboardButton("🗑 Remove Item", callback_data="order_remove_item")]
        if doc_type == "QUOTE": edit_row.append(InlineKeyboardButton("📅 Change Date", callback_data="goto_discount"))
        kb.insert(0, edit_row)

        if update.callback_query:
            update.callback_query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(kb),
                                                    parse_mode=ParseMode.MARKDOWN)
        else:
            update.message.reply_text(txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        return ORDER_CONFIRM

    def execute_order(self, update: Update, context: CallbackContext):
        query = update.callback_query

        if query.data == "cancel_order_flow":
            query.edit_message_text("🚫 Cancelled.", reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🏠 Menu", callback_data="main_menu")]]))
            return MAIN_MENU

        if query.data == "confirm_quote_convert":
            return self._execute_quote_conversion(update, context)

        doc_type = context.user_data.get('doc_type', 'ORDER')
        data = {'customer': context.user_data['order_cust_id'], 'items': context.user_data['order_cart'],
                'ref': context.user_data.get('order_ref'), 'discount': context.user_data.get('order_discount', 0.0),
                'doc_type': doc_type}

        # --- RBAC-GATED DISCOUNT APPROVAL WORKFLOW ---
        # Check if approval is required based on:
        #   1. Discount exceeds threshold (>5%)
        #   2. The RBAC module (Order_Approval / Quote_Approval) is ENABLED for this user
        user = update.effective_user
        approval_module = 'Order_Approval' if doc_type == 'ORDER' else 'Quote_Approval'
        requires_approval = (
            data['discount'] > DISCOUNT_THRESHOLD
            and self.db.check_access(user.id, approval_module)
        )

        if requires_approval:
            token = str(uuid4())
            subtotal = sum(i['LineTotal'] for i in data['items'])
            discount_val = (subtotal * data['discount']) / 100
            net_val = subtotal - discount_val
            doc_label = "Sales Order" if doc_type == "ORDER" else "Sales Quotation"
            cust_id = data['customer']
            curr = data['items'][0].get('Currency', 'EUR') if data['items'] else 'EUR'

            # Fetch customer name for the approval summary
            try:
                cust_results = self.sap_handler.search_customers(cust_id)
                cust_name = cust_results[0]['CustomerName'] if cust_results else cust_id
            except Exception:
                cust_name = cust_id

            db_id = self.db.save_pending_order(
                user_id=user.id,
                user_name=user.username or user.first_name or str(user.id),
                order_data=data,
                discount=data['discount'],
                token=token
            )

            self.notifier.send_approval_email(
                db_id=db_id,
                requester_name=user.username or user.first_name or str(user.id),
                customer_id=cust_id,
                total_val=subtotal,
                discount=data['discount'],
                items=data['items'],
                token=token,
                doc_type=doc_type
            )

            # Build detailed Telegram notification for Manager Bot
            items_detail = ""
            for idx, itm in enumerate(data['items'], 1):
                mat_id = itm.get('Material', 'N/A')
                mat_name = itm.get('Desc', mat_id)
                qty = itm.get('Quantity', 0)
                line_val = itm.get('LineTotal', 0)
                items_detail += f"  {idx}. `{mat_id}` {mat_name}\n     Qty: `{qty}` | Value: `{line_val:,.2f}`\n"

            mgr_txt = (
                f"🔔 *{doc_label} Approval Request #{db_id}*\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"👤 *Rep:* {user.username or user.first_name or user.id}\n"
                f"🏢 *Customer:* `{cust_id}` ({cust_name})\n\n"
                f"🛒 *Line Items:*\n{items_detail}\n"
                f"💵 *Gross Value:* `{subtotal:,.2f} {curr}`\n"
                f"📉 *Discount:* `{data['discount']}%` (-{discount_val:,.2f})\n"
                f"💰 *Net Value:* `{net_val:,.2f} {curr}`\n"
                f"━━━━━━━━━━━━━━━━━━"
            )

            self.notifier.send_detailed_approval_telegram(db_id, mgr_txt)

            self.db.add_notification(
                user.id,
                f"⏳ {doc_label} Request #{db_id} sent for manager approval (Discount: {data['discount']}%)"
            )

            query.edit_message_text(
                f"⏳ *Sent for Approval*\n━━━━━━━━━━━━━━━━━━\n"
                f"📄 Type: `{doc_label}`\n"
                f"📋 Request: `#{db_id}`\n"
                f"👤 Customer: `{cust_id}` ({cust_name})\n"
                f"📉 Discount: `{data['discount']}%`\n"
                f"💵 Gross: `{subtotal:,.2f} {curr}`\n"
                f"💰 Net: `{net_val:,.2f} {curr}`\n\n"
                f"Your manager will be notified via Email and Telegram.\n"
                f"You'll receive a notification once approved/rejected.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]]),
                parse_mode=ParseMode.MARKDOWN)
            return MAIN_MENU

        query.edit_message_text(f"⏳ Creating {doc_type} in SAP...", parse_mode=ParseMode.MARKDOWN)

        sap = self._get_sap(context)
        if doc_type == "QUOTE":
            valid_to = context.user_data.get('quote_valid_to')
            res = sap.create_sales_quotation(data['customer'], data['items'], data['ref'],
                                             valid_to_date=valid_to)
        else:
            res = sap.create_sales_order(data['customer'], data['items'], data['ref'], data['discount'])

        if res['success']:
            kb = []
            if doc_type == "ORDER":
                kb.append([InlineKeyboardButton("📄 PDF", callback_data=f"gen_pdf_{res['id']}"),
                           InlineKeyboardButton("🚚 Track", callback_data=f"track_order_{res['id']}")])
            kb.append([InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")])
            query.edit_message_text(f"✅ *Success!*\n📄 Doc: `{res['id']}`", reply_markup=InlineKeyboardMarkup(kb),
                                    parse_mode=ParseMode.MARKDOWN)
        else:
            query.edit_message_text(f"❌ *Error:* `{res['error']}`", parse_mode=ParseMode.MARKDOWN,
                                    reply_markup=InlineKeyboardMarkup(
                                        [[InlineKeyboardButton("🏠 Menu", callback_data="main_menu")]]))

        return MAIN_MENU

    def _execute_quote_conversion(self, update, context):
        query = update.callback_query
        try:
            q_data = context.user_data.get('convert_quote')
            query.edit_message_text(f"⏳ Converting Quote `{q_data['id']}`...", parse_mode=ParseMode.MARKDOWN)
            items_payload = []
            for item in q_data['items']:
                try:
                    qty_val = float(item['qty'].split(' ')[0])
                except (ValueError, IndexError):
                    qty_val = 1.0
                items_payload.append(
                    {'Material': item['material'], 'Quantity': qty_val, 'Ref_Item': item.get('item_no', '10')})
            res = self.sap_handler.create_sales_order(customer_id=q_data['customer'], items_list=items_payload,
                                                      customer_ref=f"Ref-Q{q_data['id']}", ref_doc=q_data['id'])
            if res['success']:
                kb = [[InlineKeyboardButton("📄 PDF", callback_data=f"gen_pdf_{res['id']}")],
                      [InlineKeyboardButton("🏠 Menu", callback_data="main_menu")]]
                query.edit_message_text(f"✅ *Conversion Successful!*\n📦 Order: `{res['id']}`",
                                        reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
            else:
                query.edit_message_text(f"❌ *Error:* `{res['error']}`", parse_mode=ParseMode.MARKDOWN,
                                        reply_markup=InlineKeyboardMarkup(
                                            [[InlineKeyboardButton("🏠 Menu", callback_data="main_menu")]]))
        except Exception as e:
            query.edit_message_text(f"❌ Critical Error: {str(e)}", reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🏠 Menu", callback_data="main_menu")]]))
        return MAIN_MENU

    # --- UPDATED: QUOTE CONVERSION START WITH LIVE SEARCH ---
    def start_quote_conversion_flow(self, update: Update, context: CallbackContext):
        query = update.callback_query;
        query.answer()

        # 1. Handle Click from "View Quotes" -> "Create Order" Button
        if query.data.startswith("convert_qt_"):
            quote_id = query.data.split('_')[-1]
            return self._fetch_and_show_quote_summary(quote_id, query.message, context, is_edit=True)

        # 2. Normal Flow from Menu (Added Live Search Button)
        kb = [[InlineKeyboardButton("🔎 Live Search", switch_inline_query_current_chat="qt ")],
              [InlineKeyboardButton("🔙 Cancel", callback_data="sales_menu")]]
        query.edit_message_text(
            "📝 *Convert Quote to Order*\n━━━━━━━━━━━━━━━━━━\n\n🔢 Enter **Quotation ID**:",
            reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        return ORDER_ASK_QUOTE_ID

    def handle_quote_id_input(self, update: Update, context: CallbackContext):
        text = update.message.text.strip()

        # ✅ Handle Live Search Selection (/select_qt_20001)
        if text.startswith('/select_qt_'):
            quote_id = text.split('_')[-1]
            return self._fetch_and_show_quote_summary(quote_id, update.message, context, is_edit=False)

        # Normal Text Input
        return self._fetch_and_show_quote_summary(text, update.message, context, is_edit=False)

    # ✅ FIXED: Now accepts is_edit parameter to choose between edit_text and reply_text
    def _fetch_and_show_quote_summary(self, quote_id, messageable, context, is_edit=False):
        if is_edit:
            messageable.edit_text("🔎 Fetching Quotation Details...")
        else:
            messageable.reply_text("🔎 Fetching Quotation Details...")

        quote_data = self.sap_handler.get_quotation_details(quote_id)
        if not quote_data:
            kb = [[InlineKeyboardButton("🔙 Cancel", callback_data="sales_menu")]]
            if is_edit:
                messageable.edit_text("❌ Quotation Not Found.", reply_markup=InlineKeyboardMarkup(kb))
            else:
                messageable.reply_text("❌ Quotation Not Found.", reply_markup=InlineKeyboardMarkup(kb))
            return ORDER_ASK_QUOTE_ID

        context.user_data['convert_quote'] = quote_data
        summary = (
            f"📄 *QUOTATION SUMMARY: {quote_id}*\n━━━━━━━━━━━━━━━━━━\n👤 **Customer:** `{quote_data['customer']}`\n📅 **Date:** {quote_data['date']}\n🔖 **Ref/PO:** `{quote_data['ref']}`\n━━━━━━━━━━━━━━━━━━\n🛒 **LINE ITEMS:**\n")
        for i, item in enumerate(quote_data['items'], 1):
            summary += f"**{i}. {item['desc']}**\n   🆔 Mat: `{item['material']}`\n   📦 Qty: `{item['qty']}`\n   💵 Net: `{item['net']}`\n"
        summary += f"━━━━━━━━━━━━━━━━━━\n💰 **TOTAL VALUE: {quote_data['total']}**\n\n✅ *Proceed to create Sales Order?*"

        kb = [[InlineKeyboardButton("✅ Yes, Create Order", callback_data="confirm_quote_convert")],
              [InlineKeyboardButton("❌ Cancel", callback_data="sales_menu")]]

        # ✅ FIX: Use correct method based on is_edit
        if is_edit:
            messageable.edit_text(summary, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        else:
            messageable.reply_text(summary, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)

        return ORDER_CONFIRM

    # --- INLINE HANDLERS ---
    def handle_quote_inline_query(self, update: Update, context: CallbackContext):
        query = update.inline_query.query.replace("qt ", "")
        # Using get_quotations as search buffer since live search API is complex
        results = self.sap_handler.get_quotations(top=20)
        # Simple local filter
        filtered = [q for q in results if query in str(q['SalesQuotation'])] if query else results

        articles = [InlineQueryResultArticle(id=str(uuid4()), title=f"📄 Quote {q['SalesQuotation']}",
                                             description=f"Val: {q['TotalNetAmount']}",
                                             input_message_content=InputTextMessageContent(
                                                 f"/select_qt_{q['SalesQuotation']}")) for q in filtered]
        update.inline_query.answer(articles, cache_time=0)

    def handle_order_inline_query(self, update: Update, context: CallbackContext):
        query = update.inline_query.query.replace("order ", "")
        if not query: return
        results = self.sap_handler.search_products(query)
        articles = [InlineQueryResultArticle(id=str(uuid4()), title=f"➕ {i['name']}", description=f"ID: {i['id']}",
                                             input_message_content=InputTextMessageContent(f"/select_mat_{i['id']}"))
                    for i in results[:50]]
        update.inline_query.answer(articles, cache_time=0)

    def handle_customer_inline_query(self, update: Update, context: CallbackContext):
        query = update.inline_query.query.replace("cust ", "").strip()
        if not query: return
        results = self.sap_handler.search_customers(query)
        articles = [InlineQueryResultArticle(id=str(uuid4()), title=f"👤 {c['CustomerName']}",
                                             description=f"ID: {c['Customer']}",
                                             input_message_content=InputTextMessageContent(
                                                 f"/select_cust_{c['Customer']}")) for c in results[:50]]
        update.inline_query.answer(articles, cache_time=0)

    # --- DELIVERY TRACKER (Sprint 2) ---
    def show_delivery_status(self, update: Update, context: CallbackContext):
        """Show fulfillment status for a sales order."""
        query = update.callback_query
        query.answer()
        order_id = query.data.replace("track_order_", "")

        query.edit_message_text("🔄 *Checking fulfillment status...*", parse_mode=ParseMode.MARKDOWN)

        status = self.sap_handler.get_order_fulfillment_status(order_id)
        if status:
            delivery_icon = {"Not Started": "⚪", "Open": "🟡", "Partial": "🔵", "Complete": "🟢"}.get(
                status['delivery'], "⚪")
            billing_icon = {"Not Started": "⚪", "Open": "🟡", "Partial": "🔵", "Complete": "🟢"}.get(
                status['billing'], "⚪")

            txt = (
                f"🚚 *Delivery Tracker*\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📦 Order: `{order_id}`\n"
                f"👤 Customer: `{status['customer']}`\n"
                f"💰 Value: `{status['amount']} {status['currency']}`\n"
                f"📅 Created: `{status['date']}`\n\n"
                f"📊 *Fulfillment Status:*\n"
                f"   {delivery_icon} Delivery: *{status['delivery']}*\n"
                f"   {billing_icon} Billing: *{status['billing']}*\n"
                f"   📋 Overall: *{status['overall']}*"
            )
        else:
            txt = f"❌ *Could not fetch status for order {order_id}*"

        kb = [
            [InlineKeyboardButton("🔄 Refresh", callback_data=f"track_order_{order_id}"),
             InlineKeyboardButton("📄 PDF", callback_data=f"gen_pdf_{order_id}")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
        ]
        try:
            query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(kb),
                                    parse_mode=ParseMode.MARKDOWN)
        except Exception:
            pass

    def handle_pdf_callback(self, update: Update, context: CallbackContext):
        query = update.callback_query
        try:
            order_id = query.data.split('_')[-1]
            query.answer("Syncing with SAP...")
            query.edit_message_text(f"⏳ *Generating PDF for Order {order_id}...*\nChecking Database & SAP Queue...",
                                    parse_mode=ParseMode.MARKDOWN)
            pdf_bytes = self.pdf_manager.get_sales_order_pdf(order_id)
            if pdf_bytes:
                query.delete_message()
                context.bot.send_document(chat_id=query.message.chat_id, document=pdf_bytes,
                                          filename=f"SalesOrder_{order_id}.pdf",
                                          caption=f"✅ **Sales Order #{order_id}**", parse_mode=ParseMode.MARKDOWN)
                kb = [[InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")]]
                context.bot.send_message(chat_id=query.message.chat_id, text="Done.",
                                         reply_markup=InlineKeyboardMarkup(kb))
            else:
                kb = [[InlineKeyboardButton("🔄 Retry", callback_data=f"gen_pdf_{order_id}"),
                       InlineKeyboardButton("❌ Cancel", callback_data="main_menu")]]
                query.edit_message_text(
                    f"⚠️ *PDF Not Found*\nSAP hasn't generated the file yet.\nPlease try again in 10s.",
                    reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        except Exception as e:
            logger.error("PDF processing error: %s", e, exc_info=True)
            query.edit_message_text("❌ Error processing PDF.")

    def show_pdf_history(self, update: Update, context: CallbackContext):
        query = update.callback_query;
        query.answer()
        pdfs = self.db.get_recent_pdfs()
        if not pdfs:
            query.edit_message_text("📂 **No Saved PDFs found.**", reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔙 Back", callback_data="sales_menu")]]), parse_mode=ParseMode.MARKDOWN)
            return
        kb = []
        for row in pdfs: kb.append([InlineKeyboardButton(f"📄 Order {row[0]}", callback_data=f"gen_pdf_{row[0]}")])
        kb.append([InlineKeyboardButton("🔙 Back", callback_data="sales_menu")])
        query.edit_message_text("📂 **Saved Order PDFs**\nTap to download:", reply_markup=InlineKeyboardMarkup(kb),
                                parse_mode=ParseMode.MARKDOWN)