"""
Smart Alert Engine — Background-triggered alerts for proactive notifications.

Sprint 1: Quote Expiry, Approval Escalation
Sprint 4: Price Condition Expiry Monitoring
"""

import requests
from datetime import datetime, timedelta
from logger_setup import get_logger

logger = get_logger(__name__)

# Escalation threshold in hours
ESCALATION_HOURS = 24


class SmartAlertEngine:
    def __init__(self, sap_handler, db_handler, sales_bot_token, manager_bot_token):
        self.sap = sap_handler
        self.db = db_handler
        self.sales_token = sales_bot_token
        self.mgr_token = manager_bot_token

    # ================================================================
    # QUOTE EXPIRY ENGINE (Sprint 1)
    # ================================================================
    def check_expiring_quotes(self):
        """Find quotes expiring within 3 days and notify the sales rep."""
        try:
            expiring = self.sap.get_expiring_quotations(days_ahead=3)
            if not expiring:
                logger.debug("No expiring quotes found.")
                return

            for qt in expiring:
                quote_id = qt.get('SalesQuotation', '')
                valid_to = qt.get('ValidToDate', '')
                customer = qt.get('SoldToParty', 'Unknown')
                amount = qt.get('TotalNetAmount', '0')

                alert_key = f"quote_expiry_{quote_id}"
                if self.db.is_alert_sent(alert_key):
                    continue

                msg = (
                    f"⚠️ *Quote Expiry Alert*\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"📝 Quote: `{quote_id}`\n"
                    f"👤 Customer: `{customer}`\n"
                    f"💰 Value: `{amount}`\n"
                    f"📅 Expires: `{valid_to}`\n\n"
                    f"⏰ This quote is expiring soon!"
                )

                # Notify all registered active users
                users = self.db.get_all_registered_users()
                for u in users:
                    if u['is_active']:
                        self._send_telegram(self.sales_token, u['user_id'], msg)
                        self.db.add_notification(u['user_id'], msg)

                self.db.mark_alert_sent(alert_key)
                logger.info("Quote expiry alert sent for %s", quote_id)

        except Exception as e:
            logger.error("Quote expiry check failed: %s", e, exc_info=True)

    # ================================================================
    # APPROVAL ESCALATION TIMER (Sprint 1)
    # ================================================================
    def check_stale_approvals(self):
        """Find pending approvals older than ESCALATION_HOURS and escalate."""
        try:
            stale = self.db.get_stale_pending_approvals(hours=ESCALATION_HOURS)
            if not stale:
                logger.debug("No stale approvals to escalate.")
                return

            mgr_chat_id = self.db.get_manager_config('manager_chat_id')
            if not mgr_chat_id:
                logger.warning("No manager_chat_id configured. Skipping escalation.")
                return

            for req in stale:
                alert_key = f"escalation_{req['id']}"
                if self.db.is_alert_sent(alert_key):
                    continue

                hours_pending = req.get('hours_pending', ESCALATION_HOURS)
                msg = (
                    f"🚨 *Escalation Alert*\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"📋 Request: `#{req['id']}`\n"
                    f"👤 Rep: {req['user_name']}\n"
                    f"📉 Discount: `{req['discount']}%`\n"
                    f"⏰ Pending for: `{hours_pending:.0f}h`\n\n"
                    f"⚠️ This request needs your attention!"
                )

                self._send_telegram(self.mgr_token, mgr_chat_id, msg)
                self.db.mark_alert_sent(alert_key)
                logger.info("Escalation alert sent for request #%s", req['id'])

        except Exception as e:
            logger.error("Approval escalation check failed: %s", e, exc_info=True)

    # ================================================================
    # PRICE CONDITION EXPIRY MONITOR (Sprint 4)
    # ================================================================
    def check_expiring_prices(self):
        """Find pricing conditions expiring within 7 days and alert manager."""
        try:
            expiring = self.sap.get_expiring_price_conditions(days_ahead=7)
            if not expiring:
                logger.debug("No expiring price conditions found.")
                return

            mgr_chat_id = self.db.get_manager_config('manager_chat_id')
            if not mgr_chat_id:
                return

            batch_msg = "🏷️ *Price Conditions Expiring Soon*\n━━━━━━━━━━━━━━━━━━\n\n"
            count = 0
            for pc in expiring[:10]:
                cond_type = pc.get('ConditionType', '')
                material = pc.get('Material', 'N/A')
                customer = pc.get('Customer', 'N/A')
                valid_to = pc.get('ConditionValidityEndDate', '')
                rate = pc.get('ConditionRateValue', '0')

                alert_key = f"price_expiry_{cond_type}_{material}_{customer}"
                if self.db.is_alert_sent(alert_key):
                    continue

                batch_msg += (
                    f"• `{cond_type}` | Mat: `{material}` | Cust: `{customer}`\n"
                    f"  Rate: `{rate}` | Expires: `{valid_to}`\n\n"
                )
                self.db.mark_alert_sent(alert_key)
                count += 1

            if count > 0:
                self._send_telegram(self.mgr_token, mgr_chat_id, batch_msg)
                logger.info("Price expiry alerts sent: %d conditions", count)

        except Exception as e:
            logger.error("Price expiry check failed: %s", e, exc_info=True)

    # ================================================================
    # TELEGRAM SENDER
    # ================================================================
    def _send_telegram(self, bot_token, chat_id, text):
        """Send a Telegram message using the Bot API."""
        try:
            requests.post(
                f"https://api.telegram.org/bot{bot_token}/sendMessage",
                json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
                timeout=10
            )
        except requests.RequestException as e:
            logger.error("Telegram alert send failed: %s", e)
