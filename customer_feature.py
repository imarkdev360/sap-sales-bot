from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode
from telegram.ext import CallbackContext, MessageHandler, Filters, CallbackQueryHandler
from telegram.error import BadRequest
from config import CUSTOMER_MASTER_LOGIC
from states import (
    CUSTOMER_MENU, CUSTOMER_SEARCH_INPUT, CUSTOMER_CREATE_CATEGORY,
    CUSTOMER_CREATE_NAME, CUSTOMER_CREATE_COUNTRY, CUSTOMER_CREATE_REGION,
    CUSTOMER_CREATE_CITY, CUSTOMER_CREATE_STREET, CUSTOMER_CREATE_POSTAL,
    CUSTOMER_CREATE_MOBILE, CUSTOMER_CREATE_EMAIL, CUSTOMER_CREATE_CONFIRM,
)
from logger_setup import get_logger

logger = get_logger(__name__)


class CustomerFeature:
    def __init__(self, sap_handler, db_handler):
        self.sap_handler = sap_handler
        self.db = db_handler

    def get_customer_menu_keyboard(self):
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("📂 Master Directory", callback_data="master_directory")],
            [InlineKeyboardButton("📋 View All", callback_data="view_customers_0")],
            [InlineKeyboardButton("➕ New Customer", callback_data="create_bp_category"),
             InlineKeyboardButton("🔍 Search Customer", callback_data="customer_details_input")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
        ])

    def show_customer_menu(self, update: Update, context: CallbackContext):
        query = update.callback_query
        if query: query.answer()

        txt = "👤 *Customer Center*\n━━━━━━━━━━━━━━━━━━\n📋 Manage Business Partners"
        kb = self.get_customer_menu_keyboard()

        if query:
            try:
                query.edit_message_text(txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
            except BadRequest:
                pass
        else:
            update.message.reply_text(txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

        return CUSTOMER_MENU

    # === MASTER DIRECTORY ===

    def start_master_directory(self, update: Update, context: CallbackContext):
        query = update.callback_query
        query.answer()
        kb = [[InlineKeyboardButton("🏠 Domestic", callback_data="cust_cat_domestic"),
               InlineKeyboardButton("🌍 Export", callback_data="cust_cat_export")],
              [InlineKeyboardButton("🔙 Back", callback_data="bp_menu")]]
        query.edit_message_text("📂 *Master Directory*\n━━━━━━━━━━━━━━━━━━\n📋 Select Classification:",
                                reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')
        return CUSTOMER_MENU

    def handle_category_selection(self, update: Update, context: CallbackContext):
        query = update.callback_query
        query.answer()
        cat = query.data.split('_')[-1]
        context.user_data['master_filter_cat'] = cat
        kb = [[InlineKeyboardButton("🏢 Company 1000", callback_data="cust_co_1000")],
              [InlineKeyboardButton("🏢 Company 2000", callback_data="cust_co_2000")],
              [InlineKeyboardButton("🏢 Company 5000", callback_data="cust_co_5000")],
              [InlineKeyboardButton("🔙 Back", callback_data="master_directory")]]
        query.edit_message_text(f"📋 Category: *{cat.upper()}*\n━━━━━━━━━━━━━━━━━━\n🏢 Select Company Code:",
                                reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')
        return CUSTOMER_MENU

    def handle_company_selection(self, update: Update, context: CallbackContext):
        query = update.callback_query
        query.answer()
        co = query.data.split('_')[-1]
        context.user_data['master_filter_co'] = co
        return self.view_filtered_customers(update, context, page=0)

    def view_filtered_customers(self, update: Update, context: CallbackContext, page=None):
        query = update.callback_query
        if page is None:
            page = int(query.data.split('_')[-1])

        co = context.user_data.get('master_filter_co')
        cat = context.user_data.get('master_filter_cat')

        all_data = self.sap_handler.get_all_customers_with_expansion()

        target_channels = CUSTOMER_MASTER_LOGIC[co][cat]

        filtered_list = []
        for item in all_data:
            comp_list = item.get('to_CustomerCompany', {}).get('results', [])
            companies = [x['CompanyCode'] for x in comp_list]

            sales_list = item.get('to_CustomerSalesArea', {}).get('results', [])
            channels = [x['DistributionChannel'] for x in sales_list]

            if co in companies:
                if any(ch in target_channels for ch in channels):
                    filtered_list.append({
                        'Customer': item['Customer'],
                        'CustomerName': item.get('CustomerName') or "Unknown"
                    })

        start = page * 5
        end = start + 5
        chunk = filtered_list[start:end]

        kb = []
        if chunk:
            for c in chunk:
                kb.append([InlineKeyboardButton(f"👤 {c['CustomerName']} ({c['Customer']})",
                                                callback_data=f"customer_details_view_{c['Customer']}")])

            nav = []
            if page > 0: nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"filter_page_{page - 1}"))
            if end < len(filtered_list): nav.append(
                InlineKeyboardButton("Next ➡️", callback_data=f"filter_page_{page + 1}"))
            if nav: kb.append(nav)
            kb.append([InlineKeyboardButton("🔙 Back to Companies", callback_data=f"cust_cat_{cat}")])

            txt = f"📋 *{cat.upper()} - Co `{co}`* (Page {page + 1})\n━━━━━━━━━━━━━━━━━━\n🔢 Found: {len(filtered_list)} Customers"
            query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')
        else:
            txt = f"⚠️ *No {cat} customers found* for Company `{co}`."
            kb = [[InlineKeyboardButton("🔙 Back", callback_data="master_directory")]]
            query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

        return CUSTOMER_MENU

    # === VIEW CUSTOMERS ===

    def view_customers(self, update: Update, context: CallbackContext):
        query = update.callback_query
        page = int(query.data.split('_')[-1])
        customers = self.sap_handler.get_customers(skip=page * 5, top=5)

        if page == 0: self.db.log_event(query.from_user, "VIEW_CUST_LIST", "Viewed Customer Directory")

        kb = []
        txt = f"📋 *Customer Directory* (Page {page + 1})\n━━━━━━━━━━━━━━━━━━\n👇 *Select a Customer:*\n"
        if customers:
            for c in customers:
                name = c.get('CustomerName') or c.get('CustomerFullName') or "Unknown"
                kb.append([InlineKeyboardButton(f"👤 {name} ({c['Customer']})",
                                                callback_data=f"customer_details_view_{c['Customer']}")])
            nav = []
            if page > 0: nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"view_customers_{page - 1}"))
            if len(customers) == 5: nav.append(
                InlineKeyboardButton("Next ➡️", callback_data=f"view_customers_{page + 1}"))
            kb.append(nav)
            kb.append([InlineKeyboardButton("🔙 Back to Menu", callback_data="bp_menu")])
        else:
            txt = "📭 *No More Customers*"
            kb = [[InlineKeyboardButton("🔙 Back to Menu", callback_data="bp_menu")]]
        try:
            query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        except BadRequest:
            query.answer("List updated")
        return CUSTOMER_MENU

    # --- CREATE CUSTOMER FLOW ---
    def create_bp_category_start(self, update: Update, context: CallbackContext):
        if 'new_bp' in context.user_data: del context.user_data['new_bp']
        update.callback_query.edit_message_text("➕ *New Customer Wizard*\n━━━━━━━━━━━━━━━━━━\n📋 Select Type:",
                                                reply_markup=InlineKeyboardMarkup(
                                                    [[InlineKeyboardButton("👤 Person", callback_data="bp_cat_1"),
                                                      InlineKeyboardButton("🏢 Organization", callback_data="bp_cat_2")],
                                                     [InlineKeyboardButton("❌ Cancel", callback_data="bp_menu")]]),
                                                parse_mode=ParseMode.MARKDOWN)
        return CUSTOMER_CREATE_CATEGORY

    def ask_name(self, update: Update, context: CallbackContext):
        cat = update.callback_query.data.replace("bp_cat_", "")
        context.user_data['new_bp'] = {'address_data': {}, 'contact_data': {}, 'category': cat}
        prompt = "Format: `First, Last`" if cat == '1' else "Enter Company Name:"
        update.callback_query.edit_message_text(f"✏️ *Step 2/9: Name*\n━━━━━━━━━━━━━━━━━━\n{prompt}",
                                                parse_mode=ParseMode.MARKDOWN)
        return CUSTOMER_CREATE_NAME

    def get_name(self, update: Update, context: CallbackContext):
        text = update.message.text.strip()
        if context.user_data['new_bp']['category'] == '1':
            if ',' not in text:
                update.message.reply_text("❌ *Invalid Format*\nPlease use: `FirstName, LastName`",
                                          parse_mode=ParseMode.MARKDOWN)
                return CUSTOMER_CREATE_NAME
            parts = text.split(',')
            context.user_data['new_bp']['name_fields'] = {"FirstName": parts[0].strip(), "LastName": parts[1].strip()}
            context.user_data['new_bp']['display_name'] = text
        else:
            context.user_data['new_bp']['name_fields'] = {"OrganizationBPName1": text}
            context.user_data['new_bp']['display_name'] = text
        update.message.reply_text("🌍 *Step 3/9: Country*\n━━━━━━━━━━━━━━━━━━\nEnter 2-letter Code (e.g. `IN`, `US`)",
                                  parse_mode=ParseMode.MARKDOWN)
        return CUSTOMER_CREATE_COUNTRY

    def get_country(self, update: Update, context: CallbackContext):
        context.user_data['new_bp']['address_data']['country'] = update.message.text.strip().upper()
        update.message.reply_text("📍 *Step 4/9: Region*\n━━━━━━━━━━━━━━━━━━\nEnter Code (e.g. `MH`):", parse_mode=ParseMode.MARKDOWN)
        return CUSTOMER_CREATE_REGION

    def get_region(self, update: Update, context: CallbackContext):
        context.user_data['new_bp']['address_data']['region'] = update.message.text.strip().upper()
        update.message.reply_text("🏙️ *Step 5/9: City*\n━━━━━━━━━━━━━━━━━━\nEnter City Name:", parse_mode=ParseMode.MARKDOWN)
        return CUSTOMER_CREATE_CITY

    def get_city(self, update: Update, context: CallbackContext):
        context.user_data['new_bp']['address_data']['city'] = update.message.text.strip()
        update.message.reply_text("🏠 *Step 6/9: Street*\n━━━━━━━━━━━━━━━━━━\nType `SKIP` to omit:", parse_mode=ParseMode.MARKDOWN)
        return CUSTOMER_CREATE_STREET

    def get_street(self, update: Update, context: CallbackContext):
        context.user_data['new_bp']['address_data']['street'] = update.message.text.strip()
        update.message.reply_text("📮 *Step 7/9: Postal Code*\n━━━━━━━━━━━━━━━━━━\nType `SKIP` to omit:", parse_mode=ParseMode.MARKDOWN)
        return CUSTOMER_CREATE_POSTAL

    def get_postal(self, update: Update, context: CallbackContext):
        context.user_data['new_bp']['address_data']['postal_code'] = update.message.text.strip()
        update.message.reply_text("📱 *Step 8/9: Mobile*\n━━━━━━━━━━━━━━━━━━\nType `SKIP` to omit:", parse_mode=ParseMode.MARKDOWN)
        return CUSTOMER_CREATE_MOBILE

    def get_mobile(self, update: Update, context: CallbackContext):
        context.user_data['new_bp']['contact_data']['mobile'] = update.message.text.strip()
        update.message.reply_text("📧 *Step 9/9: Email*\n━━━━━━━━━━━━━━━━━━\nType `SKIP` to omit:", parse_mode=ParseMode.MARKDOWN)
        return CUSTOMER_CREATE_EMAIL

    def get_email_and_show_summary(self, update: Update, context: CallbackContext):
        context.user_data['new_bp']['contact_data']['email'] = update.message.text.strip()
        d = context.user_data['new_bp']
        addr = d['address_data']
        cont = d['contact_data']
        summary = (
            f"📝 *Review Registration*\n━━━━━━━━━━━━━━━━━━\n"
            f"👤 *{d.get('display_name')}*\n"
            f"🏙️ {addr.get('city')}, {addr.get('country')}\n"
            f"📱 {cont.get('mobile')} | 📧 {cont.get('email')}\n"
            f"━━━━━━━━━━━━━━━━━━\n✅ *Create this Customer?*")
        keyboard = [[InlineKeyboardButton("✅ Yes, Create", callback_data="confirm_create_yes"),
                     InlineKeyboardButton("❌ No, Cancel", callback_data="bp_menu")]]
        update.message.reply_text(summary, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
        return CUSTOMER_CREATE_CONFIRM

    def finalize_creation(self, update: Update, context: CallbackContext):
        query = update.callback_query
        query.answer()
        if query.data == "confirm_create_yes":
            query.edit_message_text("🔄 *Syncing with SAP S/4HANA...*", parse_mode=ParseMode.MARKDOWN)
            res = self.sap_handler.create_business_partner_customer(context.user_data['new_bp'],
                                                                    context.user_data['new_bp']['address_data'],
                                                                    context.user_data['new_bp']['contact_data'])
            status = "SUCCESS" if res['success'] else "FAILED"
            self.db.log_event(query.from_user, "CREATE_CUSTOMER", f"ID: {res.get('bp_id')} | {status}")
            if res['success']:
                msg = f"✅ *Customer Created Successfully!*\n━━━━━━━━━━━━━━━━━━\n👤 *Name:* {res['name']}\n🆔 *SAP ID:* `{res['bp_id']}`"
                keyboard = [
                    [InlineKeyboardButton("👤 View Profile", callback_data=f"customer_details_view_{res['bp_id']}"),
                     InlineKeyboardButton("🔙 Menu", callback_data="bp_menu")]]
                query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
            else:
                query.edit_message_text(f"❌ *Creation Failed*\n━━━━━━━━━━━━━━━━━━\n⚠️ Reason: `{res['error']}`",
                                        parse_mode=ParseMode.MARKDOWN)
            context.user_data.clear()
            return CUSTOMER_MENU
        return self.show_customer_menu(update, context)

    # --- SEARCH & DETAILS ---
    def show_customer_details_input(self, update: Update, context: CallbackContext):
        query = update.callback_query
        query.answer()
        if query.data.startswith("customer_details_view_"):
            return self._fetch_and_show_details(query.data.replace("customer_details_view_", ""),
                                                update.effective_message, update.effective_user)

        kb = [[InlineKeyboardButton("🔎 Live Search", switch_inline_query_current_chat="cust ")],
              [InlineKeyboardButton("❌ Cancel", callback_data="bp_menu")]]
        query.edit_message_text("🔍 *Search Customer*\n━━━━━━━━━━━━━━━━━━\n\nEnter **ID** or **Name**:",
                                reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        context.user_data['expecting_search'] = True
        return CUSTOMER_SEARCH_INPUT

    def handle_customer_details(self, update: Update, context: CallbackContext):
        text = update.message.text.strip()

        if text.startswith('/select_cust_'):
            return self._fetch_and_show_details(text.split('_')[-1], update.message, update.effective_user)

        results = self.sap_handler.search_customers(text)

        if not results:
            update.message.reply_text(f"❌ Customer '{text}' not found.\nTry Live Search:",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔎 Live Search",
                                                                                               switch_inline_query_current_chat="cust ")]]))
            return CUSTOMER_SEARCH_INPUT

        context.user_data['search_results'] = results
        context.user_data['search_page'] = 0
        context.user_data['search_context'] = 'BP_SEARCH'

        return self.render_search_page(update, context)

    def handle_search_pagination(self, update: Update, context: CallbackContext):
        query = update.callback_query
        page = context.user_data.get('search_page', 0)
        if query.data == "bp_next":
            page += 1
        elif query.data == "bp_prev":
            page -= 1
        context.user_data['search_page'] = page
        return self.render_search_page(update, context)

    def render_search_page(self, update, context):
        results = context.user_data.get('search_results', [])
        page = context.user_data.get('search_page', 0)

        start = page * 5
        end = start + 5
        chunk = results[start:end]

        kb = []
        for item in chunk:
            btn = InlineKeyboardButton(f"{item['CustomerName']} ({item['Customer']})",
                                       callback_data=f"customer_details_view_{item['Customer']}")
            kb.append([btn])

        nav = []
        if page > 0: nav.append(InlineKeyboardButton("⬅️ Prev", callback_data="bp_prev"))
        if end < len(results): nav.append(InlineKeyboardButton("Next ➡️", callback_data="bp_next"))
        if nav: kb.append(nav)
        kb.append([InlineKeyboardButton("❌ Cancel", callback_data="bp_menu")])

        msg = f"🔍 Found {len(results)} matches. Showing {start + 1}-{min(end, len(results))}:"
        if update.callback_query:
            update.callback_query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(kb))
        else:
            update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(kb))

        return CUSTOMER_SEARCH_INPUT

    def handle_details_callback_external(self, update: Update, context: CallbackContext):
        bp_id = update.callback_query.data.replace("customer_details_view_", "")
        return self._fetch_and_show_details(bp_id, update.callback_query, update.effective_user)

    def _fetch_and_show_details(self, bp_id, messageable, user):
        self.db.log_event(user, "VIEW_CUST_DETAIL", f"ID: {bp_id}")
        is_edit = hasattr(messageable, 'edit_message_text')
        loading_txt = "🔄 *Retrieving Profile...*"
        if is_edit:
            messageable.edit_message_text(loading_txt, parse_mode=ParseMode.MARKDOWN)
        else:
            messageable.reply_text(loading_txt, parse_mode=ParseMode.MARKDOWN)

        data = self.sap_handler.get_customer_details(bp_id)

        if data:
            cat_emoji = "🏢" if data['Category'] == 'Org' else "👤"

            status = "Unknown"
            chn = data.get('DistributionChannel', '')

            if chn in ['DO', 'DC', 'DR']:
                status = "Domestic"
            elif chn in ['EX', 'ET', 'IE']:
                status = "Export"

            msg = (
                f"{cat_emoji} *BUSINESS PARTNER PROFILE*\n══════════════════════\n"
                f"**{data['Name']}**\n🆔 Customer ID: `{data['BusinessPartner']}`\n"
                f"📋 **Classification:** {status}\n"
                f"🏢 **Sales Org:** `{data.get('SalesOrganization', 'N/A')}` | **Channel:** `{chn}`\n"
                f"──────────────────────\n"
                f"📍 *ADDRESS*\n{data['Address']}\n\n"
                f"📞 *CONTACT INFO*\n📱 Mobile: `{data.get('Mobile', 'Not Listed')}`\n"
                f"📧 Email: {data.get('Email', 'Not Listed')}\n"
                f"══════════════════════\n⚡ *Quick Actions:*"
            )

            keyboard = [
                [InlineKeyboardButton("📊 360 View", callback_data=f"cust360_{bp_id}"),
                 InlineKeyboardButton("💳 Credit Limit", callback_data=f"check_credit_{bp_id}")],
                [InlineKeyboardButton("🏷️ Price Check", callback_data=f"check_price_{bp_id}")],
                [InlineKeyboardButton("📝 New Quote", callback_data=f"create_quote_{bp_id}"),
                 InlineKeyboardButton("📦 New Order", callback_data=f"create_order_{bp_id}")],
                [InlineKeyboardButton("🔙 Back to Directory", callback_data="master_directory")]
            ]

            if is_edit:
                messageable.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard),
                                              parse_mode=ParseMode.MARKDOWN)
            else:
                messageable.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
        else:
            kb = [[InlineKeyboardButton("🔙 Return to Menu", callback_data="bp_menu")]]
            err_msg = "❌ *Customer Not Found*\nThe ID might be incorrect or archived."
            if is_edit:
                messageable.edit_message_text(err_msg, reply_markup=InlineKeyboardMarkup(kb),
                                              parse_mode=ParseMode.MARKDOWN)
            else:
                messageable.reply_text(err_msg, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        return CUSTOMER_MENU

    def get_handlers(self):
        return [
            CallbackQueryHandler(self.show_customer_menu, pattern='^bp_menu$'),
            CallbackQueryHandler(self.view_customers, pattern='^view_customers'),
            CallbackQueryHandler(self.start_master_directory, pattern='^master_directory$'),
            CallbackQueryHandler(self.handle_category_selection, pattern='^cust_cat_'),
            CallbackQueryHandler(self.handle_company_selection, pattern='^cust_co_'),
            CallbackQueryHandler(self.view_filtered_customers, pattern='^filter_page_'),
            CallbackQueryHandler(self.create_bp_category_start, pattern='^create_bp_category$'),
            CallbackQueryHandler(self.show_customer_details_input, pattern='^customer_details_input$'),
            CallbackQueryHandler(self.handle_search_pagination, pattern='^bp_next$|^bp_prev$'),
            CallbackQueryHandler(self.handle_details_callback_external, pattern='^customer_details_view_'),
            MessageHandler(Filters.text | Filters.regex(r'^/select_cust_'), self.handle_customer_details)
        ]
