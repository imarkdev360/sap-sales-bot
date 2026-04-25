import smtplib
import json
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from config import (
    SALES_BOT_TOKEN, MANAGER_BOT_TOKEN, SMTP_SERVER, SMTP_PORT,
    SMTP_EMAIL, SMTP_PASSWORD, MANAGER_EMAIL, MANAGER_BOT_USERNAME,
)
from logger_setup import get_logger

logger = get_logger(__name__)


class NotificationService:

    def __init__(self, db_handler):
        self.db = db_handler

    def add_to_history(self, user_id, message):
        try:
            self.db.add_notification(user_id, message)
        except Exception as e:
            logger.error("Failed to add notification to history: %s", e)

    def send_approval_email(self, db_id, requester_name, customer_id, total_val, discount, items, token, doc_type="ORDER"):
        # Telegram Deep Links — works from any device (phone, desktop, web)
        approve_link = f"https://t.me/{MANAGER_BOT_USERNAME}?start=email_approve_{db_id}"
        reject_link = f"https://t.me/{MANAGER_BOT_USERNAME}?start=email_reject_{db_id}"

        doc_label = "Sales Order" if doc_type == "ORDER" else "Sales Quotation"

        rows = ""
        for item in items:
            rows += f"<tr><td style='padding:8px;border:1px solid #ddd'>{item['Desc']}</td><td style='padding:8px;border:1px solid #ddd'>{item['Quantity']}</td><td style='padding:8px;border:1px solid #ddd'>{item['LineTotal']:.2f}</td></tr>"

        body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; color: #333;">
            <h2 style="color: #2c3e50;">{doc_label} Approval Request: #{db_id}</h2>
            <p><b>Sales Rep:</b> {requester_name}</p>
            <p><b>Customer:</b> {customer_id}</p>

            <table style="border-collapse: collapse; width: 100%; margin-top: 10px; margin-bottom: 20px;">
                <tr style="background-color: #f2f2f2;">
                    <th style="padding:8px;border:1px solid #ddd;text-align:left">Material</th>
                    <th style="padding:8px;border:1px solid #ddd;text-align:left">Qty</th>
                    <th style="padding:8px;border:1px solid #ddd;text-align:left">Value</th>
                </tr>
                {rows}
            </table>

            <h3 style="color: #e74c3c;">Requested Discount: {discount}%</h3>
            <h3>Total Value: {total_val:.2f}</h3>

            <hr>
            <p><b>Action Required:</b></p>
            <a href="{approve_link}" style="background-color: #27ae60; color: white; padding: 12px 24px; text-decoration: none; border-radius: 5px; font-weight: bold; margin-right: 15px;">
               APPROVE
            </a>
            <a href="{reject_link}" style="background-color: #c0392b; color: white; padding: 12px 24px; text-decoration: none; border-radius: 5px; font-weight: bold;">
               REJECT
            </a>
            <p style="font-size: 12px; color: #777; margin-top: 20px;">*Buttons open the Manager Bot in Telegram to process this request.</p>
        </body>
        </html>
        """

        try:
            msg = MIMEMultipart()
            msg['From'] = SMTP_EMAIL
            msg['To'] = MANAGER_EMAIL
            msg['Subject'] = f"Approval Needed: #{db_id} ({requester_name})"
            msg.attach(MIMEText(body, 'html'))

            server = smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT)
            try:
                server.login(SMTP_EMAIL, SMTP_PASSWORD)
                server.sendmail(SMTP_EMAIL, MANAGER_EMAIL, msg.as_string())
            finally:
                server.quit()
            logger.info("Approval email sent for request #%s", db_id)
        except smtplib.SMTPException as e:
            logger.error("Email send failed for request #%s: %s", db_id, e)
        except Exception as e:
            logger.error("Unexpected email error for request #%s: %s", db_id, e)

        # Telegram notification to Manager Bot (using stored chat_id)
        try:
            chat_id = self.db.get_manager_config('manager_chat_id')
            if not chat_id:
                logger.warning("Manager chat_id not found in DB. Manager must /start the bot first.")
                return True

            kb = {"inline_keyboard": [
                [{"text": "✅ Approve", "callback_data": f"mgr_approve_{db_id}"},
                 {"text": "🚫 Reject", "callback_data": f"mgr_reject_ask_{db_id}"}]
            ]}
            txt = (
                f"🔔 *Approval Request #{db_id}*\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"👤 Rep: {requester_name}\n"
                f"🏢 Customer: {customer_id}\n"
                f"📉 Discount: {discount}%\n"
                f"💰 Value: {total_val:,.2f}"
            )
            requests.post(
                f"https://api.telegram.org/bot{MANAGER_BOT_TOKEN}/sendMessage",
                json={"chat_id": int(chat_id), "text": txt, "parse_mode": "Markdown",
                      "reply_markup": json.dumps(kb)},
                timeout=10)
            logger.info("Manager Telegram notification sent for #%s", db_id)
        except requests.RequestException as e:
            logger.warning("Telegram manager notification failed for #%s: %s", db_id, e)
        except Exception as e:
            logger.warning("Manager notification unexpected error: %s", e)

        return True

    def send_detailed_approval_telegram(self, db_id, formatted_text):
        """Send a pre-formatted detailed approval message to the Manager Bot via Telegram."""
        try:
            chat_id = self.db.get_manager_config('manager_chat_id')
            if not chat_id:
                logger.warning("Manager chat_id not found. Manager must /start the bot first.")
                return

            kb = {"inline_keyboard": [
                [{"text": "✅ Approve", "callback_data": f"mgr_approve_{db_id}"},
                 {"text": "🚫 Reject", "callback_data": f"mgr_reject_ask_{db_id}"}]
            ]}

            requests.post(
                f"https://api.telegram.org/bot{MANAGER_BOT_TOKEN}/sendMessage",
                json={"chat_id": int(chat_id), "text": formatted_text,
                      "parse_mode": "Markdown", "reply_markup": json.dumps(kb)},
                timeout=10)
            logger.info("Detailed approval notification sent for #%s", db_id)
        except requests.RequestException as e:
            logger.warning("Detailed approval Telegram failed for #%s: %s", db_id, e)
        except Exception as e:
            logger.warning("Detailed approval unexpected error for #%s: %s", db_id, e)
