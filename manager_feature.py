from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode
from telegram.error import BadRequest
from telegram.ext import CallbackContext
from db_helper import ALL_MODULES
from states import (
    MAIN_MENU, MGR_TARGET_PERIOD, MGR_TARGET_START, MGR_TARGET_END,
    MGR_TARGET_AMOUNT, MGR_USER_LIST, MGR_USER_PERMISSIONS,
    MGR_PETTY_CASH_MENU, MGR_PETTY_CASH_SET,
)
from logger_setup import get_logger

logger = get_logger(__name__)

MODULE_LABELS = {
    'dashboard': '📊 My Dashboard',
    'customer': '👤 Customer Center',
    'sales': '📦 Sales & Quotes',
    'material': '🔍 Material Search',
    'expense': '🧾 Expense Claim',
    'Notification': '🔔 Notification Center',
    'Order_Approval': '📋 Order Discount Approval (>5%)',
    'Quote_Approval': '📝 Quote Discount Approval (>5%)',
}


class ManagerFeature:
    def __init__(self, db_handler):
        self.db = db_handler

    # ================================================================
    # ACTIVE USERS TRACKING
    # ================================================================
    def show_active_users(self, update: Update, context: CallbackContext):
        query = update.callback_query
        query.answer()

        users = self.db.get_all_registered_users()
        if not users:
            query.edit_message_text(
                "👥 *Active Users*\n━━━━━━━━━━━━━━━━━━\n\n_No users registered yet._",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]]),
                parse_mode=ParseMode.MARKDOWN)
            return MAIN_MENU

        txt = "👥 *Active Sales Users*\n━━━━━━━━━━━━━━━━━━\n\n"
        for u in users:
            name = u['first_name'] or u['username'] or str(u['user_id'])
            username = f"@{u['username']}" if u['username'] else "N/A"
            last = u['last_active'] or 'Never'
            status = "🟢" if u['is_active'] else "🔴"
            txt += (
                f"{status} *{name}*\n"
                f"   🆔 `{u['user_id']}` | {username}\n"
                f"   🕐 Last Active: `{last}`\n\n"
            )

        kb = [
            [InlineKeyboardButton("🔄 Refresh", callback_data="mgr_active_users")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
        ]
        query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        return MAIN_MENU

    # ================================================================
    # SET SALES TARGET (Manager Only)
    # ================================================================
    def start_set_target(self, update: Update, context: CallbackContext):
        query = update.callback_query
        query.answer()

        today = datetime.now()
        kb = [
            [InlineKeyboardButton("📅 Current Month", callback_data="tgt_period_month"),
             InlineKeyboardButton("📅 Current Quarter", callback_data="tgt_period_quarter")],
            [InlineKeyboardButton("📅 Current Year", callback_data="tgt_period_year"),
             InlineKeyboardButton("✏️ Custom Date", callback_data="tgt_period_custom")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
        ]
        query.edit_message_text(
            "🎯 *Set Sales Target*\n━━━━━━━━━━━━━━━━━━\n\n📅 Select the target period:",
            reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        return MGR_TARGET_PERIOD

    def handle_target_period(self, update: Update, context: CallbackContext):
        query = update.callback_query
        query.answer()
        choice = query.data
        today = datetime.now()

        if choice == "tgt_period_custom":
            query.edit_message_text("📅 *Start Date* (DD-MM-YYYY):", parse_mode=ParseMode.MARKDOWN)
            return MGR_TARGET_START
        elif choice == "tgt_period_month":
            start = today.replace(day=1).strftime("%d-%m-%Y")
        elif choice == "tgt_period_quarter":
            q_month = ((today.month - 1) // 3) * 3 + 1
            start = today.replace(month=q_month, day=1).strftime("%d-%m-%Y")
        elif choice == "tgt_period_year":
            start = today.replace(month=1, day=1).strftime("%d-%m-%Y")
        else:
            return MGR_TARGET_PERIOD

        end = today.strftime("%d-%m-%Y")
        context.user_data['tgt_start'] = start
        context.user_data['tgt_end'] = end
        return self._ask_target_amount(query.edit_message_text, context)

    def handle_target_custom_start(self, update: Update, context: CallbackContext):
        text = update.message.text.strip()
        if len(text) != 10:
            update.message.reply_text("❌ Invalid Format. Use DD-MM-YYYY.")
            return MGR_TARGET_START
        context.user_data['tgt_start'] = text
        update.message.reply_text("📅 *End Date* (DD-MM-YYYY):", parse_mode=ParseMode.MARKDOWN)
        return MGR_TARGET_END

    def handle_target_custom_end(self, update: Update, context: CallbackContext):
        text = update.message.text.strip()
        if len(text) != 10:
            update.message.reply_text("❌ Invalid Format. Use DD-MM-YYYY.")
            return MGR_TARGET_END
        context.user_data['tgt_end'] = text
        return self._ask_target_amount(update.message.reply_text, context)

    def _ask_target_amount(self, msg_func, context):
        start = context.user_data['tgt_start']
        end = context.user_data['tgt_end']
        msg_func(
            f"🎯 *Enter Sales Target Amount*\n━━━━━━━━━━━━━━━━━━\n"
            f"📅 Period: `{start}` to `{end}`\n\n"
            f"Enter target value (e.g. `50000`):",
            parse_mode=ParseMode.MARKDOWN)
        return MGR_TARGET_AMOUNT

    def handle_target_amount(self, update: Update, context: CallbackContext):
        text = update.message.text.strip().replace(',', '')
        try:
            amount = float(text)
        except ValueError:
            update.message.reply_text("❌ Enter numbers only (e.g. 50000).")
            return MGR_TARGET_AMOUNT

        start = context.user_data['tgt_start']
        end = context.user_data['tgt_end']
        user = update.effective_user

        self.db.set_sales_target(amount, start, end, user.id)
        self.db.log_event(user, "SET_TARGET", f"Target: {amount:,.2f} for {start} to {end}")

        kb = [[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]]
        update.message.reply_text(
            f"✅ *Sales Target Set!*\n━━━━━━━━━━━━━━━━━━\n"
            f"🎯 Target: `{amount:,.2f}`\n"
            f"📅 Period: `{start}` to `{end}`\n\n"
            f"All sales users will see this target in their dashboards.",
            reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        return MAIN_MENU

    # ================================================================
    # USER PERMISSION MANAGEMENT (RBAC)
    # ================================================================
    def show_user_list(self, update: Update, context: CallbackContext):
        query = update.callback_query
        query.answer()

        users = self.db.get_all_registered_users()
        if not users:
            query.edit_message_text(
                "🔐 *Manage User Access*\n━━━━━━━━━━━━━━━━━━\n\n_No users registered yet._",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]]),
                parse_mode=ParseMode.MARKDOWN)
            return MAIN_MENU

        kb = []
        for u in users:
            name = u['first_name'] or u['username'] or str(u['user_id'])
            status = "🟢" if u['is_active'] else "🔴"
            kb.append([InlineKeyboardButton(
                f"{status} {name} ({u['user_id']})",
                callback_data=f"mgr_user_{u['user_id']}")])

        kb.append([InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")])

        query.edit_message_text(
            "🔐 *Manage User Access*\n━━━━━━━━━━━━━━━━━━\n\n👇 Select a user to manage permissions:",
            reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        return MGR_USER_LIST

    def show_user_permissions(self, update: Update, context: CallbackContext):
        query = update.callback_query
        query.answer()

        user_id = int(query.data.split('_')[-1])
        context.user_data['mgr_editing_user'] = user_id

        return self._render_permission_screen(query.edit_message_text, user_id)

    def _render_permission_screen(self, msg_func, user_id):
        perms = self.db.get_user_permissions(user_id)

        # Get user info
        users = self.db.get_all_registered_users()
        user_info = None
        for u in users:
            if u['user_id'] == user_id:
                user_info = u
                break
        name = (user_info['first_name'] or user_info['username'] or str(user_id)) if user_info else str(user_id)

        # Get B2B/Internal status
        bp_link = self.db.get_user_bp(user_id)
        user_type = bp_link['user_type'] if bp_link else 'b2b'
        bp_id = bp_link['bp_id'] if bp_link else 'N/A'
        type_icon = "🏢" if user_type == 'internal' else "🌐"
        type_label = "Internal Sales Rep" if user_type == 'internal' else "B2B Customer"

        txt = (
            f"🔐 *Permissions: {name}*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🆔 User ID: `{user_id}`\n"
            f"🏷 SAP BP: `{bp_id}`\n"
            f"{type_icon} Role: *{type_label}*\n\n"
            f"Toggle modules ON/OFF:\n"
        )

        kb = []

        # User type toggle (B2B <-> Internal)
        if user_type == 'b2b':
            kb.append([InlineKeyboardButton(
                "🏢 Promote to Internal Sales Rep",
                callback_data=f"mgr_settype_{user_id}_internal")])
        else:
            kb.append([InlineKeyboardButton(
                "🌐 Demote to B2B Customer",
                callback_data=f"mgr_settype_{user_id}_b2b")])

        for mod_key in ALL_MODULES:
            allowed = perms.get(mod_key, True)
            icon = "✅" if allowed else "🚫"
            label = MODULE_LABELS.get(mod_key, mod_key)
            kb.append([InlineKeyboardButton(
                f"{icon} {label}",
                callback_data=f"mgr_toggle_{user_id}_{mod_key}")])

        # Active/Deactive user toggle
        if user_info and user_info['is_active']:
            kb.append([InlineKeyboardButton("🔴 Deactivate User", callback_data=f"mgr_deactivate_{user_id}")])
        else:
            kb.append([InlineKeyboardButton("🟢 Activate User", callback_data=f"mgr_activate_{user_id}")])

        kb.append([InlineKeyboardButton("🔙 Back to Users", callback_data="mgr_manage_users")])

        try:
            msg_func(txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        except BadRequest as e:
            if "Message is not modified" in str(e):
                logger.debug("Permission screen unchanged (duplicate click), ignoring.")
            else:
                raise
        return MGR_USER_PERMISSIONS

    def toggle_permission(self, update: Update, context: CallbackContext):
        query = update.callback_query
        query.answer()
        data = query.data

        if data.startswith("mgr_settype_"):
            # Format: mgr_settype_{uid}_{type}
            parts = data.split('_')
            uid = int(parts[2])
            new_type = parts[3]  # 'b2b' or 'internal'
            self.db.set_user_type(uid, new_type)
            label = "Internal Sales Rep" if new_type == 'internal' else "B2B Customer"
            self.db.log_event(update.effective_user, "USER_TYPE_CHANGE",
                              f"Set user {uid} to {label}")
            return self._render_permission_screen(query.edit_message_text, uid)

        if data.startswith("mgr_deactivate_"):
            uid = int(data.split('_')[-1])
            self.db.deactivate_user(uid)
            self.db.log_event(update.effective_user, "USER_DEACTIVATED", f"Deactivated user {uid}")
            return self._render_permission_screen(query.edit_message_text, uid)

        if data.startswith("mgr_activate_"):
            uid = int(data.split('_')[-1])
            self.db.activate_user(uid)
            self.db.log_event(update.effective_user, "USER_ACTIVATED", f"Activated user {uid}")
            return self._render_permission_screen(query.edit_message_text, uid)

        if data.startswith("mgr_toggle_"):
            # Format: mgr_toggle_{uid}_{mod_key}
            # mod_key may contain underscores (e.g. "Order_Approval"),
            # so rejoin everything after the 3rd segment
            parts = data.split('_')
            uid = int(parts[2])
            mod_key = '_'.join(parts[3:])

            # Get current permission and flip it
            perms = self.db.get_user_permissions(uid)
            current = perms.get(mod_key, True)
            self.db.set_user_permission(uid, mod_key, not current)

            action = "GRANTED" if not current else "REVOKED"
            self.db.log_event(update.effective_user, "PERMISSION_CHANGE",
                              f"{action} '{mod_key}' for user {uid}")

            return self._render_permission_screen(query.edit_message_text, uid)

        return MGR_USER_PERMISSIONS

    # ================================================================
    # PETTY CASH LIMITS MANAGEMENT (Sprint 2)
    # ================================================================
    def show_petty_cash_menu(self, update: Update, context: CallbackContext):
        """Show current petty cash limits with option to change."""
        query = update.callback_query
        query.answer()

        limits = self.db.get_petty_cash_limits()
        daily_total = self.db.get_daily_expense_total()
        monthly_total = self.db.get_monthly_expense_total()

        txt = "💵 *Petty Cash Settings*\n━━━━━━━━━━━━━━━━━━\n\n"

        if limits:
            daily_pct = min(100, int((daily_total / limits['daily']) * 100)) if limits['daily'] > 0 else 0
            monthly_pct = min(100, int((monthly_total / limits['monthly']) * 100)) if limits['monthly'] > 0 else 0

            txt += (
                f"📅 *Daily Limit:* `{limits['daily']:,.2f}`\n"
                f"   Used Today: `{daily_total:,.2f}` ({daily_pct}%)\n\n"
                f"📅 *Monthly Limit:* `{limits['monthly']:,.2f}`\n"
                f"   Used This Month: `{monthly_total:,.2f}` ({monthly_pct}%)\n"
            )
        else:
            txt += "_No limits configured yet._\n"

        kb = [
            [InlineKeyboardButton("✏️ Set Daily Limit", callback_data="petty_set_daily"),
             InlineKeyboardButton("✏️ Set Monthly Limit", callback_data="petty_set_monthly")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
        ]

        try:
            query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(kb),
                                    parse_mode=ParseMode.MARKDOWN)
        except Exception:
            pass
        return MGR_PETTY_CASH_MENU

    def handle_petty_cash_selection(self, update: Update, context: CallbackContext):
        """Handle daily/monthly limit selection."""
        query = update.callback_query
        query.answer()

        if query.data == "petty_set_daily":
            context.user_data['petty_setting'] = 'daily'
            query.edit_message_text("💵 *Enter Daily Limit* (e.g. `5000`):",
                                    parse_mode=ParseMode.MARKDOWN)
        else:
            context.user_data['petty_setting'] = 'monthly'
            query.edit_message_text("💵 *Enter Monthly Limit* (e.g. `50000`):",
                                    parse_mode=ParseMode.MARKDOWN)
        return MGR_PETTY_CASH_SET

    def handle_petty_cash_amount(self, update: Update, context: CallbackContext):
        """Save the entered petty cash limit."""
        text = update.message.text.strip().replace(',', '')
        try:
            amount = float(text)
        except ValueError:
            update.message.reply_text("❌ Enter numbers only.")
            return MGR_PETTY_CASH_SET

        setting = context.user_data.get('petty_setting', 'daily')
        user = update.effective_user
        limits = self.db.get_petty_cash_limits() or {'daily': 5000, 'monthly': 50000}

        if setting == 'daily':
            self.db.set_petty_cash_limits(amount, limits['monthly'], user.id)
        else:
            self.db.set_petty_cash_limits(limits['daily'], amount, user.id)

        self.db.log_event(user, "PETTY_CASH_LIMIT_SET", f"{setting}: {amount}")

        kb = [[InlineKeyboardButton("🔙 Back", callback_data="mgr_petty_cash"),
               InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]]
        update.message.reply_text(
            f"✅ *{setting.capitalize()} Limit Updated!*\n"
            f"New Limit: `{amount:,.2f}`",
            reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        return MAIN_MENU
