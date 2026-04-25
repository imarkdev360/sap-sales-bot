import re
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode
from telegram.ext import CallbackContext
from telegram.error import BadRequest
from config import MANAGER_BOT_TOKEN
from logger_setup import get_logger

logger = get_logger(__name__)

NOTIF_PAGE_SIZE = 5


class NotificationFeature:
    def __init__(self, sap_handler, db_handler):
        self.sap = sap_handler
        self.db = db_handler

    # ================================================================
    # MAIN NOTIFICATION MENU (Clean 2-Button Layout)
    # ================================================================
    def show_notifications_menu(self, update: Update, context: CallbackContext):
        query = update.callback_query

        pending_count = self.db.count_pending_approvals()
        history_count = self.db.count_approval_history()

        kb = [
            [InlineKeyboardButton(
                f"⏳ Pending Requests ({pending_count})",
                callback_data="show_pending_list")],
            [InlineKeyboardButton(
                f"📜 History ({history_count})",
                callback_data="show_approval_history")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")],
        ]

        text = (
            "🔔 *Notification Center*\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "📋 Select a category below:"
        )

        if query:
            try:
                query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb),
                                        parse_mode=ParseMode.MARKDOWN)
            except BadRequest:
                query.answer("Updated")
            except Exception as e:
                logger.debug("Notification menu edit error: %s", e)
        else:
            update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(kb),
                                      parse_mode=ParseMode.MARKDOWN)

    # ================================================================
    # PENDING REQUESTS (Paginated, 5 per page)
    # ================================================================
    def show_pending_list(self, update: Update, context: CallbackContext):
        self._render_pending_page(update, context, 0)

    def handle_pending_page(self, update: Update, context: CallbackContext):
        page = int(update.callback_query.data.split('_')[-1])
        self._render_pending_page(update, context, page)

    def _render_pending_page(self, update, context, page):
        total = self.db.count_pending_approvals()
        offset = page * NOTIF_PAGE_SIZE
        pending = self.db.get_pending_approvals_paginated(limit=NOTIF_PAGE_SIZE, offset=offset)

        kb = []
        if pending:
            for p in pending:
                kb.append([InlineKeyboardButton(
                    f"📋 Review #{p['id']} — {p['user_name']} ({p['discount']}%)",
                    callback_data=f"mgr_review_{p['id']}")])
        else:
            kb.append([InlineKeyboardButton("✅ All Caught Up!", callback_data="ignore")])

        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"pending_page_{page - 1}"))
        if offset + NOTIF_PAGE_SIZE < total:
            nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"pending_page_{page + 1}"))
        if nav:
            kb.append(nav)

        kb.append([InlineKeyboardButton("🔙 Back", callback_data="view_notifications")])

        total_pages = max(1, (total + NOTIF_PAGE_SIZE - 1) // NOTIF_PAGE_SIZE)
        text = (
            f"⏳ *Pending Requests* (Page {page + 1}/{total_pages})\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📋 {total} item{'s' if total != 1 else ''} awaiting review"
        )

        try:
            update.callback_query.edit_message_text(
                text, reply_markup=InlineKeyboardMarkup(kb),
                parse_mode=ParseMode.MARKDOWN)
        except BadRequest:
            update.callback_query.answer()

    # ================================================================
    # APPROVAL DETAIL (Pending Review — Approve/Reject Actions)
    # ================================================================
    def show_approval_detail(self, update: Update, context: CallbackContext):
        if context.bot.token != MANAGER_BOT_TOKEN:
            return
        query = update.callback_query
        db_id = query.data.split('_')[-1]
        req = self.db.get_pending_order(db_id)
        if not req or req['status'] != 'PENDING':
            query.edit_message_text(
                "✅ No longer pending.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Back", callback_data="show_pending_list")]]))
            return
        d = req['order_data']
        total = sum(i['LineTotal'] for i in d['items'])
        text = (
            f"📋 *APPROVAL REQUEST #{db_id}*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"👤 *Rep:* {req['user_name']}\n"
            f"🏢 *Customer:* `{d['customer']}`\n"
            f"💰 *Value:* `{total:.2f}`\n"
            f"📉 *Discount:* `{req['discount']}%`"
        )
        kb = [[InlineKeyboardButton("✅ Approve", callback_data=f"mgr_approve_{db_id}"),
               InlineKeyboardButton("🚫 Reject", callback_data=f"mgr_reject_ask_{db_id}")],
              [InlineKeyboardButton("🔙 Back", callback_data="show_pending_list")]]
        query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb),
                                parse_mode=ParseMode.MARKDOWN)

    # ================================================================
    # HISTORY (Paginated, 5 per page — APPROVED + REJECTED)
    # ================================================================
    def show_approval_history(self, update: Update, context: CallbackContext):
        self._render_history_page(update, context, 0)

    def handle_history_page(self, update: Update, context: CallbackContext):
        page = int(update.callback_query.data.split('_')[-1])
        self._render_history_page(update, context, page)

    def _render_history_page(self, update, context, page):
        total = self.db.count_approval_history()
        offset = page * NOTIF_PAGE_SIZE
        history = self.db.get_approval_history_paginated(limit=NOTIF_PAGE_SIZE, offset=offset)

        kb = []
        if history:
            for h in history:
                status_icon = "✅" if h['status'] == 'APPROVED' else "🚫"
                kb.append([InlineKeyboardButton(
                    f"{status_icon} #{h['id']} — {h['user_name']} ({h['status']})",
                    callback_data=f"read_history_{h['id']}")])
        else:
            kb.append([InlineKeyboardButton("📭 No History Yet", callback_data="ignore")])

        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"history_page_{page - 1}"))
        if offset + NOTIF_PAGE_SIZE < total:
            nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"history_page_{page + 1}"))
        if nav:
            kb.append(nav)

        kb.append([InlineKeyboardButton("🔙 Back", callback_data="view_notifications")])

        total_pages = max(1, (total + NOTIF_PAGE_SIZE - 1) // NOTIF_PAGE_SIZE)
        text = (
            f"📜 *Approval History* (Page {page + 1}/{total_pages})\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📊 {total} completed request{'s' if total != 1 else ''}"
        )

        try:
            update.callback_query.edit_message_text(
                text, reply_markup=InlineKeyboardMarkup(kb),
                parse_mode=ParseMode.MARKDOWN)
        except BadRequest:
            update.callback_query.answer()

    # ================================================================
    # HISTORY DETAIL (Read-only view of completed request)
    # ================================================================
    def show_history_detail(self, update: Update, context: CallbackContext):
        query = update.callback_query
        db_id = query.data.split('_')[-1]
        req = self.db.get_pending_order(db_id)
        if not req:
            query.edit_message_text(
                "📭 Record not found.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Back", callback_data="show_approval_history")]]),
                parse_mode=ParseMode.MARKDOWN)
            return

        d = req['order_data']
        total = sum(i['LineTotal'] for i in d['items'])
        status_icon = "✅" if req['status'] == 'APPROVED' else "🚫"

        text = (
            f"{status_icon} *Request #{db_id} — {req['status']}*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"👤 *Rep:* {req['user_name']}\n"
            f"🏢 *Customer:* `{d['customer']}`\n"
            f"💰 *Value:* `{total:.2f}`\n"
            f"📉 *Discount:* `{req['discount']}%`"
        )

        kb = [[InlineKeyboardButton("🔙 Back", callback_data="show_approval_history")]]
        query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb),
                                parse_mode=ParseMode.MARKDOWN)
