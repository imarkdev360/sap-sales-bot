"""
Customer 360 Panel — Unified customer intelligence view.

Sprint 2: Customer order history, revenue summary, quote history, reorder suggestions.
"""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode
from telegram.ext import CallbackContext
from telegram.error import BadRequest
from states import CUSTOMER_360, CUSTOMER_MENU
from logger_setup import get_logger

logger = get_logger(__name__)


class Customer360Feature:
    def __init__(self, sap_handler, db_handler):
        self.sap = sap_handler
        self.db = db_handler

    def show_360_panel(self, update: Update, context: CallbackContext):
        """Main entry: Show customer 360 overview."""
        query = update.callback_query
        query.answer()

        bp_id = query.data.replace("cust360_", "")
        context.user_data['c360_customer'] = bp_id

        query.edit_message_text("🔄 *Loading Customer 360...*", parse_mode=ParseMode.MARKDOWN)

        # Fetch data from SAP
        details = self.sap.get_customer_details(bp_id)
        revenue = self.sap.get_customer_revenue_summary(bp_id)
        credit = self.sap.get_credit_exposure(bp_id)

        name = details.get('Name', bp_id) if details else bp_id

        txt = (
            f"📊 *Customer 360 — {name}*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🆔 ID: `{bp_id}`\n\n"
        )

        # Revenue Section
        if revenue:
            txt += (
                f"💰 *Revenue Summary*\n"
                f"   Total: `{revenue['total_revenue']:,.2f} {revenue['currency']}`\n"
                f"   Invoices: `{revenue['invoice_count']}`\n"
                f"   Last Invoice: `{revenue['last_invoice_date']}`\n\n"
            )
        else:
            txt += "💰 *Revenue:* _No billing data_\n\n"

        # Credit Section
        if credit:
            txt += (
                f"💳 *Credit Status*\n"
                f"   Limit: `{credit.get('limit', 'N/A')}`\n"
                f"   Exposure: `{credit.get('exposure', 'N/A')}`\n"
                f"   Available: `{credit.get('available', 'N/A')}`\n\n"
            )

        txt += "👇 *Explore Details:*"

        kb = [
            [InlineKeyboardButton("📦 Order History", callback_data=f"c360_orders_{bp_id}"),
             InlineKeyboardButton("📝 Quote History", callback_data=f"c360_quotes_{bp_id}")],
            [InlineKeyboardButton("🔙 Customer Profile", callback_data=f"customer_details_view_{bp_id}")],
        ]

        try:
            query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(kb),
                                    parse_mode=ParseMode.MARKDOWN)
        except BadRequest:
            pass

        return CUSTOMER_360

    def show_order_history(self, update: Update, context: CallbackContext):
        """Show recent orders for this customer."""
        query = update.callback_query
        query.answer()

        bp_id = query.data.replace("c360_orders_", "")
        orders = self.sap.get_customer_order_history(bp_id)

        status_map = {'A': '🟡 Open', 'B': '🔵 Partial', 'C': '🟢 Complete'}

        if orders:
            txt = f"📦 *Recent Orders — {bp_id}*\n━━━━━━━━━━━━━━━━━━\n\n"
            for o in orders[:8]:
                st = status_map.get(o.get('OverallSDProcessStatus', ''), '⚪ Unknown')
                d = self.sap._parse_sap_date(o.get('CreationDate'))
                txt += (
                    f"📋 `{o['SalesOrder']}` | {st}\n"
                    f"   💰 {o['TotalNetAmount']} {o['TransactionCurrency']} | 📅 {d}\n\n"
                )
        else:
            txt = f"📦 *No orders found for {bp_id}*"

        kb = [[InlineKeyboardButton("🔙 Back to 360", callback_data=f"cust360_{bp_id}")]]
        try:
            query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(kb),
                                    parse_mode=ParseMode.MARKDOWN)
        except BadRequest:
            pass
        return CUSTOMER_360

    def show_quote_history(self, update: Update, context: CallbackContext):
        """Show recent quotes for this customer."""
        query = update.callback_query
        query.answer()

        bp_id = query.data.replace("c360_quotes_", "")
        quotes = self.sap.get_customer_quote_history(bp_id)

        status_map = {'A': '🟡 Open', 'C': '🟢 Completed'}

        if quotes:
            txt = f"📝 *Recent Quotes — {bp_id}*\n━━━━━━━━━━━━━━━━━━\n\n"
            for q in quotes[:8]:
                st = status_map.get(q.get('OverallSDProcessStatus', ''), '⚪ Unknown')
                d = self.sap._parse_sap_date(q.get('CreationDate'))
                txt += (
                    f"📋 `{q['SalesQuotation']}` | {st}\n"
                    f"   💰 {q['TotalNetAmount']} {q['TransactionCurrency']} | 📅 {d}\n\n"
                )
        else:
            txt = f"📝 *No quotes found for {bp_id}*"

        kb = [[InlineKeyboardButton("🔙 Back to 360", callback_data=f"cust360_{bp_id}")]]
        try:
            query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(kb),
                                    parse_mode=ParseMode.MARKDOWN)
        except BadRequest:
            pass
        return CUSTOMER_360

