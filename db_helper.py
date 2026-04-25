import sqlite3
import json
from datetime import datetime
from config import DB_NAME
from logger_setup import get_logger
from pii_masker import mask_pii
import os

logger = get_logger(__name__)

# Default module keys for RBAC
ALL_MODULES = ['dashboard', 'customer', 'sales', 'material', 'expense', 'Notification', 'Order_Approval', 'Quote_Approval']


class DatabaseHandler:
    def __init__(self):
        db_path = '/tmp/sap_bot.db' if os.environ.get('VERCEL') == '1' else '/tmp/sap_bot.db'
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        # Enable WAL mode for better concurrent read/write performance
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.create_all_tables()
        logger.info("Database online: %s", DB_NAME)

    def create_all_tables(self):
        self.conn.execute('''CREATE TABLE IF NOT EXISTS activity_logs (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                user_id INTEGER, username TEXT, action TEXT, detail TEXT,
                                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)''')
        self.conn.execute('''CREATE TABLE IF NOT EXISTS pending_approvals (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                user_id INTEGER, user_name TEXT, order_data TEXT, discount REAL,
                                token TEXT, status TEXT DEFAULT 'PENDING',
                                created_at DATETIME DEFAULT CURRENT_TIMESTAMP)''')
        self.conn.execute('''CREATE TABLE IF NOT EXISTS notifications (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                user_id INTEGER, message TEXT,
                                is_read BOOLEAN DEFAULT 0,
                                created_at DATETIME DEFAULT CURRENT_TIMESTAMP)''')
        self.conn.execute('''CREATE TABLE IF NOT EXISTS pdf_cache (
                                order_id TEXT PRIMARY KEY,
                                pdf_blob TEXT,
                                created_at DATETIME DEFAULT CURRENT_TIMESTAMP)''')

        # --- Enterprise Tables ---
        self.conn.execute('''CREATE TABLE IF NOT EXISTS sales_targets (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                target_amount REAL NOT NULL,
                                period_start TEXT NOT NULL,
                                period_end TEXT NOT NULL,
                                set_by INTEGER NOT NULL,
                                created_at DATETIME DEFAULT CURRENT_TIMESTAMP)''')

        self.conn.execute('''CREATE TABLE IF NOT EXISTS registered_users (
                                user_id INTEGER PRIMARY KEY,
                                username TEXT,
                                first_name TEXT,
                                is_active BOOLEAN DEFAULT 1,
                                last_active DATETIME DEFAULT CURRENT_TIMESTAMP,
                                registered_at DATETIME DEFAULT CURRENT_TIMESTAMP)''')

        self.conn.execute('''CREATE TABLE IF NOT EXISTS user_permissions (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                user_id INTEGER NOT NULL,
                                module_key TEXT NOT NULL,
                                is_allowed BOOLEAN DEFAULT 1,
                                UNIQUE(user_id, module_key),
                                FOREIGN KEY (user_id) REFERENCES registered_users(user_id))''')

        self.conn.execute('''CREATE TABLE IF NOT EXISTS manager_config (
                                key TEXT PRIMARY KEY,
                                value TEXT NOT NULL,
                                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP)''')

        # --- Innovation v2.0 Tables ---
        self.conn.execute('''CREATE TABLE IF NOT EXISTS petty_cash_limits (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                daily_limit REAL NOT NULL DEFAULT 5000,
                                monthly_limit REAL NOT NULL DEFAULT 50000,
                                set_by INTEGER NOT NULL,
                                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP)''')

        self.conn.execute('''CREATE TABLE IF NOT EXISTS user_favorites (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                user_id INTEGER NOT NULL,
                                action_key TEXT NOT NULL,
                                action_label TEXT NOT NULL,
                                usage_count INTEGER DEFAULT 1,
                                UNIQUE(user_id, action_key))''')

        self.conn.execute('''CREATE TABLE IF NOT EXISTS alert_tracking (
                                alert_key TEXT PRIMARY KEY,
                                sent_at DATETIME DEFAULT CURRENT_TIMESTAMP)''')

        # --- GDPR / Privacy Consent ---
        self.conn.execute('''CREATE TABLE IF NOT EXISTS gdpr_consent (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                user_id INTEGER NOT NULL,
                                policy_version TEXT NOT NULL,
                                action TEXT NOT NULL DEFAULT 'ACCEPTED',
                                created_at DATETIME DEFAULT CURRENT_TIMESTAMP)''')

        # --- OTP Authentication ---
        self.conn.execute('''CREATE TABLE IF NOT EXISTS otp_sessions (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                user_id INTEGER NOT NULL,
                                bp_id TEXT NOT NULL,
                                email TEXT NOT NULL,
                                otp_hash TEXT NOT NULL,
                                otp_salt BLOB NOT NULL,
                                attempts INTEGER DEFAULT 0,
                                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                                expires_at DATETIME NOT NULL,
                                used BOOLEAN DEFAULT 0)''')

        self.conn.execute('''CREATE TABLE IF NOT EXISTS user_bp_link (
                                user_id INTEGER PRIMARY KEY,
                                bp_id TEXT NOT NULL,
                                email TEXT NOT NULL,
                                user_type TEXT NOT NULL DEFAULT 'b2b',
                                last_login DATETIME DEFAULT CURRENT_TIMESTAMP,
                                created_at DATETIME DEFAULT CURRENT_TIMESTAMP)''')

        # Migration: add user_type column if missing (existing DBs)
        try:
            self.conn.execute("SELECT user_type FROM user_bp_link LIMIT 1")
        except sqlite3.OperationalError:
            self.conn.execute("ALTER TABLE user_bp_link ADD COLUMN user_type TEXT NOT NULL DEFAULT 'b2b'")

        self.conn.commit()

        # Performance indexes
        self.conn.execute('CREATE INDEX IF NOT EXISTS idx_activity_user ON activity_logs(user_id)')
        self.conn.execute('CREATE INDEX IF NOT EXISTS idx_approvals_status ON pending_approvals(status)')
        self.conn.execute('CREATE INDEX IF NOT EXISTS idx_approvals_token ON pending_approvals(token)')
        self.conn.execute('CREATE INDEX IF NOT EXISTS idx_notifications_user ON notifications(user_id)')
        self.conn.execute('CREATE INDEX IF NOT EXISTS idx_targets_period ON sales_targets(period_start, period_end)')
        self.conn.execute('CREATE INDEX IF NOT EXISTS idx_permissions_user ON user_permissions(user_id)')
        self.conn.execute('CREATE INDEX IF NOT EXISTS idx_registered_active ON registered_users(last_active)')
        self.conn.execute('CREATE INDEX IF NOT EXISTS idx_favorites_user ON user_favorites(user_id)')
        self.conn.execute('CREATE INDEX IF NOT EXISTS idx_alert_key ON alert_tracking(alert_key)')
        self.conn.execute('CREATE INDEX IF NOT EXISTS idx_gdpr_consent_user ON gdpr_consent(user_id, policy_version)')
        self.conn.execute('CREATE INDEX IF NOT EXISTS idx_otp_sessions_user ON otp_sessions(user_id, used)')
        self.conn.execute('CREATE INDEX IF NOT EXISTS idx_user_bp_link_bp ON user_bp_link(bp_id)')
        self.conn.commit()

    # --- ACTIVITY LOGGING (PII-masked) ---
    def log_event(self, user_obj, action, detail):
        try:
            uid = user_obj.id if hasattr(user_obj, 'id') else 0
            uname = (user_obj.username or user_obj.first_name) if hasattr(user_obj, 'id') else "SYSTEM"
            safe_detail = mask_pii(str(detail)) if detail else ""
            self.conn.execute("INSERT INTO activity_logs (user_id, username, action, detail) VALUES (?, ?, ?, ?)",
                              (uid, uname, action, safe_detail))
            self.conn.commit()
            logger.debug("Activity logged: [user_%s] %s", uid, action)
        except sqlite3.Error as e:
            logger.error("Failed to log event: %s", e)

    def log_event_and_update_active(self, user_obj, action, detail):
        """Combined log + last_active update in a single transaction."""
        try:
            uid = user_obj.id if hasattr(user_obj, 'id') else 0
            uname = (user_obj.username or user_obj.first_name) if hasattr(user_obj, 'id') else "SYSTEM"
            safe_detail = mask_pii(str(detail)) if detail else ""
            self.conn.execute("INSERT INTO activity_logs (user_id, username, action, detail) VALUES (?, ?, ?, ?)",
                              (uid, uname, action, safe_detail))
            self.conn.execute(
                "UPDATE registered_users SET last_active = CURRENT_TIMESTAMP WHERE user_id = ?",
                (uid,))
            self.conn.commit()
            logger.debug("Activity logged + active updated: [user_%s] %s", uid, action)
        except sqlite3.Error as e:
            logger.error("Failed to log event: %s", e)

    # --- APPROVALS ---
    def save_pending_order(self, user_id, user_name, order_data, discount, token):
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO pending_approvals (user_id, user_name, order_data, discount, token) VALUES (?, ?, ?, ?, ?)",
            (user_id, user_name, json.dumps(order_data), discount, token))
        self.conn.commit()
        logger.info("Pending order saved: user=%s, discount=%.1f%%", user_name, discount)
        return cursor.lastrowid

    def get_pending_order(self, db_id):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM pending_approvals WHERE id = ?", (db_id,))
        row = cursor.fetchone()
        if row:
            return {
                "id": row["id"], "user_id": row["user_id"], "user_name": row["user_name"],
                "order_data": json.loads(row["order_data"]), "discount": row["discount"],
                "token": row["token"], "status": row["status"]
            }
        return None

    def get_all_pending_approvals(self):
        cursor = self.conn.execute("SELECT * FROM pending_approvals WHERE status = 'PENDING' ORDER BY id DESC")
        return cursor.fetchall()

    def get_pending_approvals_paginated(self, limit=5, offset=0):
        cursor = self.conn.execute(
            "SELECT * FROM pending_approvals WHERE status = 'PENDING' ORDER BY id DESC LIMIT ? OFFSET ?",
            (limit, offset))
        return cursor.fetchall()

    def count_pending_approvals(self):
        row = self.conn.execute(
            "SELECT COUNT(*) as cnt FROM pending_approvals WHERE status = 'PENDING'").fetchone()
        return row['cnt']

    def get_approval_history_paginated(self, limit=5, offset=0):
        cursor = self.conn.execute(
            "SELECT * FROM pending_approvals WHERE status IN ('APPROVED', 'REJECTED') ORDER BY id DESC LIMIT ? OFFSET ?",
            (limit, offset))
        return cursor.fetchall()

    def count_approval_history(self):
        row = self.conn.execute(
            "SELECT COUNT(*) as cnt FROM pending_approvals WHERE status IN ('APPROVED', 'REJECTED')").fetchone()
        return row['cnt']

    def get_user_notifications_paginated(self, user_id, limit=5, offset=0):
        return self.conn.execute(
            "SELECT id, message, created_at FROM notifications WHERE user_id = ? ORDER BY id DESC LIMIT ? OFFSET ?",
            (user_id, limit, offset)).fetchall()

    def count_user_notifications(self, user_id):
        row = self.conn.execute(
            "SELECT COUNT(*) as cnt FROM notifications WHERE user_id = ?", (user_id,)).fetchone()
        return row['cnt']

    def update_status(self, db_id, status):
        self.conn.execute("UPDATE pending_approvals SET status = ? WHERE id = ?", (status, db_id))
        self.conn.commit()
        logger.info("Approval status updated: id=%s -> %s", db_id, status)

    # --- NOTIFICATIONS ---
    def add_notification(self, user_id, message):
        self.conn.execute("INSERT INTO notifications (user_id, message) VALUES (?, ?)", (user_id, message))
        self.conn.commit()

    def get_user_notifications(self, user_id, limit=5):
        return self.conn.execute(
            "SELECT message, created_at FROM notifications WHERE user_id = ? ORDER BY id DESC LIMIT ?",
            (user_id, limit)).fetchall()

    # --- PDF ---
    def save_pdf_to_cache(self, order_id, base64_data):
        try:
            clean_id = str(int(order_id))
            self.conn.execute("INSERT OR REPLACE INTO pdf_cache (order_id, pdf_blob) VALUES (?, ?)",
                              (clean_id, base64_data))
            self.conn.commit()
        except (sqlite3.Error, ValueError) as e:
            logger.error("PDF cache save error for order %s: %s", order_id, e)

    def get_pdf_from_cache(self, order_id):
        try:
            clean_id = str(int(order_id))
            row = self.conn.execute("SELECT pdf_blob FROM pdf_cache WHERE order_id = ?", (clean_id,)).fetchone()
            return row["pdf_blob"] if row else None
        except (sqlite3.Error, ValueError) as e:
            logger.error("PDF cache read error for order %s: %s", order_id, e)
            return None

    def get_recent_pdfs(self, limit=10):
        try:
            return self.conn.execute("SELECT order_id, created_at FROM pdf_cache ORDER BY created_at DESC LIMIT ?",
                                     (limit,)).fetchall()
        except sqlite3.Error as e:
            logger.error("Recent PDFs fetch error: %s", e)
            return []

    # --- SALES TARGETS (Feature 1) ---
    def set_sales_target(self, amount, period_start, period_end, set_by):
        """Save a new sales target for a given period. Replaces any existing target for the same period."""
        self.conn.execute(
            "DELETE FROM sales_targets WHERE period_start = ? AND period_end = ?",
            (period_start, period_end))
        self.conn.execute(
            "INSERT INTO sales_targets (target_amount, period_start, period_end, set_by) VALUES (?, ?, ?, ?)",
            (amount, period_start, period_end, set_by))
        self.conn.commit()
        logger.info("Sales target set: %.2f for %s to %s by user %s", amount, period_start, period_end, set_by)

    def get_sales_target(self, period_start, period_end):
        """Get the most recent target that matches or overlaps the requested period."""
        row = self.conn.execute(
            "SELECT target_amount FROM sales_targets WHERE period_start = ? AND period_end = ? "
            "ORDER BY created_at DESC LIMIT 1",
            (period_start, period_end)).fetchone()
        if row:
            return row['target_amount']
        # Fallback: find any target whose period overlaps
        row = self.conn.execute(
            "SELECT target_amount FROM sales_targets WHERE period_start <= ? AND period_end >= ? "
            "ORDER BY created_at DESC LIMIT 1",
            (period_end, period_start)).fetchone()
        return row['target_amount'] if row else None

    # --- USER REGISTRATION & TRACKING (Feature 3) ---
    def register_user(self, user_id, username, first_name):
        """Register a user or update their info if already exists."""
        self.conn.execute(
            "INSERT INTO registered_users (user_id, username, first_name) VALUES (?, ?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET username=excluded.username, first_name=excluded.first_name",
            (user_id, username or '', first_name or ''))
        self.conn.commit()

    def update_last_active(self, user_id):
        """Update last active timestamp for a user."""
        self.conn.execute(
            "UPDATE registered_users SET last_active = CURRENT_TIMESTAMP WHERE user_id = ?",
            (user_id,))
        self.conn.commit()

    def get_all_registered_users(self):
        """Get all registered users ordered by last active."""
        return self.conn.execute(
            "SELECT user_id, username, first_name, is_active, last_active, registered_at "
            "FROM registered_users ORDER BY last_active DESC").fetchall()

    def deactivate_user(self, user_id):
        self.conn.execute("UPDATE registered_users SET is_active = 0 WHERE user_id = ?", (user_id,))
        self.conn.commit()

    def activate_user(self, user_id):
        self.conn.execute("UPDATE registered_users SET is_active = 1 WHERE user_id = ?", (user_id,))
        self.conn.commit()

    # --- RBAC PERMISSIONS (Feature 2) ---
    def get_user_permissions(self, user_id):
        """Get module permissions for a user. Returns dict {module_key: is_allowed}.
        Defaults to all allowed if no permissions are set."""
        rows = self.conn.execute(
            "SELECT module_key, is_allowed FROM user_permissions WHERE user_id = ?",
            (user_id,)).fetchall()
        if not rows:
            return {m: True for m in ALL_MODULES}
        perms = {m: True for m in ALL_MODULES}
        for row in rows:
            perms[row['module_key']] = bool(row['is_allowed'])
        return perms

    def check_access(self, user_id, module_key):
        """Check if a user has access to a specific module. Returns True if allowed (default)."""
        perms = self.get_user_permissions(user_id)
        return perms.get(module_key, True)

    def set_user_permission(self, user_id, module_key, is_allowed):
        """Set a single module permission for a user."""
        self.conn.execute(
            "INSERT INTO user_permissions (user_id, module_key, is_allowed) VALUES (?, ?, ?) "
            "ON CONFLICT(user_id, module_key) DO UPDATE SET is_allowed=excluded.is_allowed",
            (user_id, module_key, 1 if is_allowed else 0))
        self.conn.commit()
        logger.info("Permission updated: user=%s, module=%s, allowed=%s", user_id, module_key, is_allowed)

    # --- MANAGER CONFIG (Feature 4) ---
    def set_manager_config(self, key, value):
        """Store a key-value config for manager operations."""
        self.conn.execute(
            "INSERT INTO manager_config (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=CURRENT_TIMESTAMP",
            (key, str(value)))
        self.conn.commit()

    def get_manager_config(self, key):
        """Retrieve a manager config value."""
        row = self.conn.execute("SELECT value FROM manager_config WHERE key = ?", (key,)).fetchone()
        return row['value'] if row else None

    # --- PETTY CASH LIMITS (Sprint 2) ---
    def set_petty_cash_limits(self, daily, monthly, set_by):
        """Set petty cash limits. Only keeps latest record."""
        self.conn.execute("DELETE FROM petty_cash_limits")
        self.conn.execute(
            "INSERT INTO petty_cash_limits (daily_limit, monthly_limit, set_by) VALUES (?, ?, ?)",
            (daily, monthly, set_by))
        self.conn.commit()
        logger.info("Petty cash limits set: daily=%.2f, monthly=%.2f by %s", daily, monthly, set_by)

    def get_petty_cash_limits(self):
        """Get current petty cash limits."""
        row = self.conn.execute(
            "SELECT daily_limit, monthly_limit FROM petty_cash_limits ORDER BY updated_at DESC LIMIT 1"
        ).fetchone()
        if row:
            return {'daily': row['daily_limit'], 'monthly': row['monthly_limit']}
        return None

    def get_daily_expense_total(self, date_str=None):
        """Get total expense amount posted today (from activity logs)."""
        if not date_str:
            date_str = datetime.now().strftime('%Y-%m-%d')
        row = self.conn.execute(
            "SELECT COALESCE(SUM(CAST(detail AS REAL)), 0) as total FROM activity_logs "
            "WHERE action = 'PETTY_CASH_POSTED' AND DATE(timestamp) = ?",
            (date_str,)).fetchone()
        return row['total'] if row else 0.0

    def get_monthly_expense_total(self, year=None, month=None):
        """Get total expense amount posted this month."""
        now = datetime.now()
        if not year: year = now.year
        if not month: month = now.month
        month_str = f"{year}-{month:02d}"
        row = self.conn.execute(
            "SELECT COALESCE(SUM(CAST(detail AS REAL)), 0) as total FROM activity_logs "
            "WHERE action = 'PETTY_CASH_POSTED' AND strftime('%%Y-%%m', timestamp) = ?",
            (month_str,)).fetchone()
        return row['total'] if row else 0.0

    # --- FAVORITES / QUICK ACTIONS (Sprint 4) ---
    def add_favorite(self, user_id, action_key, action_label):
        """Add or increment a favorite action."""
        self.conn.execute(
            "INSERT INTO user_favorites (user_id, action_key, action_label) VALUES (?, ?, ?) "
            "ON CONFLICT(user_id, action_key) DO UPDATE SET usage_count = usage_count + 1",
            (user_id, action_key, action_label))
        self.conn.commit()

    def get_favorites(self, user_id, limit=5):
        """Get top favorite actions for a user."""
        return self.conn.execute(
            "SELECT action_key, action_label, usage_count FROM user_favorites "
            "WHERE user_id = ? ORDER BY usage_count DESC LIMIT ?",
            (user_id, limit)).fetchall()

    def remove_favorite(self, user_id, action_key):
        """Remove a specific favorite."""
        self.conn.execute(
            "DELETE FROM user_favorites WHERE user_id = ? AND action_key = ?",
            (user_id, action_key))
        self.conn.commit()

    # --- ALERT TRACKING (Sprint 1) ---
    def is_alert_sent(self, alert_key):
        """Check if an alert has already been sent (deduplication)."""
        row = self.conn.execute(
            "SELECT alert_key FROM alert_tracking WHERE alert_key = ?", (alert_key,)).fetchone()
        return row is not None

    def mark_alert_sent(self, alert_key):
        """Mark an alert as sent."""
        self.conn.execute(
            "INSERT OR IGNORE INTO alert_tracking (alert_key) VALUES (?)", (alert_key,))
        self.conn.commit()

    def cleanup_old_alerts(self, days=30):
        """Remove alert tracking records older than N days."""
        self.conn.execute(
            "DELETE FROM alert_tracking WHERE sent_at < datetime('now', ?)",
            (f'-{days} days',))
        self.conn.commit()

    # --- GDPR CONSENT MANAGEMENT ---
    def record_consent(self, user_id, policy_version):
        """Record that a user accepted a specific privacy policy version."""
        self.conn.execute(
            "INSERT INTO gdpr_consent (user_id, policy_version, action) VALUES (?, ?, 'ACCEPTED')",
            (user_id, policy_version))
        self.conn.commit()
        logger.info("GDPR consent recorded: user_%s accepted policy v%s", user_id, policy_version)

    def has_valid_consent(self, user_id, policy_version):
        """Check if user has accepted the current policy version (and hasn't revoked it)."""
        row = self.conn.execute(
            "SELECT action FROM gdpr_consent "
            "WHERE user_id = ? AND policy_version = ? "
            "ORDER BY created_at DESC LIMIT 1",
            (user_id, policy_version)).fetchone()
        return row is not None and row['action'] == 'ACCEPTED'

    def revoke_consent(self, user_id):
        """Record that a user revoked consent (does not delete — that's a separate operation)."""
        self.conn.execute(
            "INSERT INTO gdpr_consent (user_id, policy_version, action) VALUES (?, 'ALL', 'REVOKED')",
            (user_id,))
        self.conn.commit()
        logger.info("GDPR consent revoked for user_%s", user_id)

    def get_consent_history(self, user_id):
        """Get full consent audit trail for a user."""
        return self.conn.execute(
            "SELECT policy_version, action, created_at FROM gdpr_consent "
            "WHERE user_id = ? ORDER BY created_at DESC",
            (user_id,)).fetchall()

    # --- STALE APPROVALS (Sprint 1 - Escalation) ---
    def get_stale_pending_approvals(self, hours=24):
        """Get pending approvals older than N hours."""
        rows = self.conn.execute(
            "SELECT id, user_id, user_name, discount, "
            "(julianday('now') - julianday(created_at)) * 24 AS hours_pending "
            "FROM pending_approvals WHERE status = 'PENDING' "
            "AND (julianday('now') - julianday(created_at)) * 24 > ? "
            "ORDER BY created_at ASC",
            (hours,)).fetchall()
        return [dict(r) for r in rows]

    # --- MANAGER ANALYTICS (Sprint 3) ---
    def get_activity_summary(self, days=30):
        """Get activity counts grouped by action for the last N days."""
        rows = self.conn.execute(
            "SELECT action, COUNT(*) as cnt FROM activity_logs "
            "WHERE timestamp >= datetime('now', ?) GROUP BY action ORDER BY cnt DESC",
            (f'-{days} days',)).fetchall()
        return [dict(r) for r in rows]

    def get_user_activity_ranking(self, days=30):
        """Get user activity ranking for the last N days."""
        rows = self.conn.execute(
            "SELECT username, COUNT(*) as actions FROM activity_logs "
            "WHERE timestamp >= datetime('now', ?) AND username != 'SYSTEM' "
            "GROUP BY username ORDER BY actions DESC LIMIT 10",
            (f'-{days} days',)).fetchall()
        return [dict(r) for r in rows]

    def get_approval_stats(self):
        """Get approval statistics."""
        row = self.conn.execute(
            "SELECT "
            "COUNT(CASE WHEN status = 'PENDING' THEN 1 END) as pending, "
            "COUNT(CASE WHEN status = 'APPROVED' THEN 1 END) as approved, "
            "COUNT(CASE WHEN status = 'REJECTED' THEN 1 END) as rejected "
            "FROM pending_approvals").fetchone()
        return dict(row)

    # --- OTP AUTHENTICATION ---
    def store_otp(self, user_id, bp_id, email, otp_hash, otp_salt, expires_at):
        """Store a new OTP challenge. Invalidates any previous unused OTPs for this user."""
        self.conn.execute(
            "UPDATE otp_sessions SET used = 1 WHERE user_id = ? AND used = 0",
            (user_id,))
        self.conn.execute(
            "INSERT INTO otp_sessions (user_id, bp_id, email, otp_hash, otp_salt, expires_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, bp_id, email, otp_hash, otp_salt, expires_at))
        self.conn.commit()

    def get_pending_otp(self, user_id):
        """Get the latest unused, non-expired OTP session for a user."""
        row = self.conn.execute(
            "SELECT id, bp_id, email, otp_hash, otp_salt, attempts, expires_at "
            "FROM otp_sessions "
            "WHERE user_id = ? AND used = 0 AND expires_at > datetime('now') "
            "ORDER BY created_at DESC LIMIT 1",
            (user_id,)).fetchone()
        if row:
            return dict(row)
        return None

    def increment_otp_attempts(self, otp_id):
        """Increment the attempt counter for an OTP session."""
        self.conn.execute(
            "UPDATE otp_sessions SET attempts = attempts + 1 WHERE id = ?",
            (otp_id,))
        self.conn.commit()

    def mark_otp_used(self, otp_id):
        """Mark an OTP as successfully used."""
        self.conn.execute(
            "UPDATE otp_sessions SET used = 1 WHERE id = ?",
            (otp_id,))
        self.conn.commit()

    def link_user_bp(self, user_id, bp_id, email, user_type='b2b'):
        """Create or update the persistent link between a Telegram user and SAP BP."""
        self.conn.execute(
            "INSERT INTO user_bp_link (user_id, bp_id, email, user_type, last_login) "
            "VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP) "
            "ON CONFLICT(user_id) DO UPDATE SET bp_id=excluded.bp_id, "
            "email=excluded.email, last_login=CURRENT_TIMESTAMP",
            (user_id, bp_id, email, user_type))
        self.conn.commit()

    def get_user_bp(self, user_id):
        """Get the BP linked to a Telegram user."""
        row = self.conn.execute(
            "SELECT bp_id, email, user_type, last_login FROM user_bp_link WHERE user_id = ?",
            (user_id,)).fetchone()
        if row:
            return dict(row)
        return None

    def get_b2b_customer_id(self, user_id):
        """Returns the BP ID if user is B2B (locked), or None if internal (unrestricted)."""
        row = self.conn.execute(
            "SELECT bp_id, user_type FROM user_bp_link WHERE user_id = ?",
            (user_id,)).fetchone()
        if row and row['user_type'] == 'b2b':
            return row['bp_id']
        return None

    def set_user_type(self, user_id, user_type):
        """Set user type to 'b2b' or 'internal'."""
        self.conn.execute(
            "UPDATE user_bp_link SET user_type = ? WHERE user_id = ?",
            (user_type, user_id))
        self.conn.commit()

    # --- EXPENSE DUPLICATE DETECTION (Sprint 1) ---
    def check_expense_duplicate(self, vendor_id, amount, doc_date, ref):
        """Check if a similar expense was posted in the last 7 days."""
        rows = self.conn.execute(
            "SELECT detail FROM activity_logs "
            "WHERE action IN ('EXPENSE_POSTED', 'PETTY_CASH_POSTED') "
            "AND timestamp >= datetime('now', '-7 days') "
            "AND detail LIKE ? AND detail LIKE ? AND detail LIKE ?",
            (f'%{vendor_id}%', f'%{amount}%', f'%{ref}%')).fetchall()
        return len(rows) > 0
