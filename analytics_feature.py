"""
Manager Analytics Dashboard — Team performance and activity insights.

Sprint 3: Activity summary, user rankings, approval stats.
"""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode
from telegram.ext import CallbackContext
from telegram.error import BadRequest
from states import MGR_ANALYTICS, MAIN_MENU
from config import MANAGER_BOT_TOKEN
from logger_setup import get_logger

logger = get_logger(__name__)


class AnalyticsFeature:
    def __init__(self, db_handler):
        self.db = db_handler

    def show_analytics_menu(self, update: Update, context: CallbackContext):
        """Main analytics dashboard entry."""
        query = update.callback_query
        query.answer()

        if context.bot.token != MANAGER_BOT_TOKEN:
            query.answer("Manager only.", show_alert=True)
            return MAIN_MENU

        # Fetch all analytics data
        activity = self.db.get_activity_summary(days=30)
        ranking = self.db.get_user_activity_ranking(days=30)
        approval = self.db.get_approval_stats()
        user_count = len(self.db.get_all_registered_users())

        txt = (
            "📊 *Manager Analytics Dashboard*\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
        )

        # User Overview
        txt += (
            f"👥 *Team Overview*\n"
            f"   Registered Users: `{user_count}`\n\n"
        )

        # Approval Stats
        txt += (
            f"📋 *Approval Summary*\n"
            f"   ⏳ Pending: `{approval.get('pending', 0)}`\n"
            f"   ✅ Approved: `{approval.get('approved', 0)}`\n"
            f"   🚫 Rejected: `{approval.get('rejected', 0)}`\n\n"
        )

        # Top Users (last 30 days)
        if ranking:
            txt += "🏆 *Top Active Users (30d)*\n"
            for i, u in enumerate(ranking[:5], 1):
                medal = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"][i - 1]
                txt += f"   {medal} {u['username']} — `{u['actions']}` actions\n"
            txt += "\n"

        # Activity Breakdown
        if activity:
            txt += "📈 *Activity Breakdown (30d)*\n"
            for a in activity[:8]:
                txt += f"   • {a['action']}: `{a['cnt']}`\n"

        kb = [
            [InlineKeyboardButton("🔄 Refresh", callback_data="mgr_analytics")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
        ]

        try:
            query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(kb),
                                    parse_mode=ParseMode.MARKDOWN)
        except BadRequest:
            pass

        return MAIN_MENU
