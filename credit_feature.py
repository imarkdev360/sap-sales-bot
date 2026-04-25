from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext
from logger_setup import get_logger

logger = get_logger(__name__)


class CreditFeature:
    def __init__(self, sap_handler, db_handler):
        self.sap = sap_handler
        self.db = db_handler

    def _create_progress_bar(self, percentage):
        """Creates a visual progress bar using block characters"""
        visual_percent = min(percentage, 100)
        total_blocks = 10
        filled_blocks = int((visual_percent / 100) * total_blocks)
        empty_blocks = total_blocks - filled_blocks
        bar = "█" * filled_blocks + "░" * empty_blocks
        return f"[{bar}]"

    def check_credit_limit(self, update: Update, context: CallbackContext):
        query = update.callback_query
        query.answer("Analyzing Credit Data...")

        try:
            customer_id = query.data.split('_')[-1]

            credit_data = self.sap.get_credit_exposure(customer_id)

            if credit_data:
                bp_details = self.sap.get_customer_details(customer_id)
                cust_name = bp_details.get('Name') if bp_details else customer_id

                exposure = credit_data['exposure']
                limit = credit_data['limit']
                currency = credit_data['currency']
                remaining = credit_data['remaining']

                if limit > 0:
                    percent = (exposure / limit) * 100
                else:
                    percent = 0.0

                progress_bar = self._create_progress_bar(percent)

                msg = (
                    f"💳 *CREDIT REPORT*\n"
                    f"══════════════════════\n"
                    f"👤 *{cust_name}*\n"
                    f"🆔 ID: `{customer_id}`\n\n"

                    f"📊 *UTILIZATION*\n"
                    f"`{progress_bar}` *{percent:.2f}%*\n"
                    f"📈 Used: `{exposure:,.0f}` of `{limit:,.0f}`\n\n"

                    f"💰 *FINANCIAL DETAILS*\n"
                    f"──────────────────────\n"
                    f"🔴 *Total Exposure:* `{exposure:,.2f} {currency}`\n"
                    f"📏 *Credit Limit:* `{limit:,.2f} {currency}`\n"
                    f"🟢 *Available:* `{remaining:,.2f} {currency}`\n"
                    f"══════════════════════"
                )

                kb = [[InlineKeyboardButton("🔙 Back to Profile", callback_data=f"customer_details_view_{customer_id}")]]
                query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

                self.db.log_event(query.from_user, "CREDIT_CHECK", f"Viewed credit for {customer_id}")

            else:
                kb = [[InlineKeyboardButton("🔙 Back", callback_data=f"customer_details_view_{customer_id}")]]
                query.edit_message_text(
                    f"⚠️ *Data Not Available*\nCredit segment might not be maintained for Customer `{customer_id}`.",
                    reply_markup=InlineKeyboardMarkup(kb),
                    parse_mode='Markdown'
                )

        except Exception as e:
            logger.error("Credit check error for customer: %s", e, exc_info=True)
            query.edit_message_text("❌ *System Error*\nUnable to fetch financial data.", parse_mode='Markdown')
