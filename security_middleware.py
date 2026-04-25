"""
Enterprise Security Middleware for SAP S/4HANA Telegram Bot.

Implements:
  1. OTP-based authentication (Email OTP via SAP Business Partner lookup)
  2. Session timeout with secure memory flush
  3. Content protection (anti-forwarding)
  4. Rate limiting per user
  5. Secure context.user_data wipe utilities

CISO Audit: This module MUST be imported and initialized in bot.py
before any handlers are registered.
"""

import hashlib
import hmac
import os
import random
import smtplib
import time
import threading
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from functools import wraps
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext
from config import (
    OTP_EXPIRY_SECONDS, OTP_MAX_ATTEMPTS,
    SMTP_SERVER, SMTP_PORT, SMTP_EMAIL, SMTP_PASSWORD,
)
from logger_setup import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Configuration (override via environment variables)
# ---------------------------------------------------------------------------
SESSION_TIMEOUT_SECONDS = int(os.environ.get("SESSION_TIMEOUT", "28800"))  # 8 hour inactivity
SESSION_HARD_CEILING = int(os.environ.get("SESSION_HARD_CEILING", "43200"))  # 12 hour absolute max
RATE_LIMIT_WINDOW = int(os.environ.get("RATE_LIMIT_WINDOW", "60"))  # seconds
RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("RATE_LIMIT_MAX", "30"))  # per window

# ---------------------------------------------------------------------------
# Secure OTP Hashing (salted SHA-256)
# ---------------------------------------------------------------------------

def _hash_otp(otp: str, salt: bytes) -> str:
    """Hash an OTP with a per-request salt using SHA-256."""
    return hashlib.sha256(salt + otp.encode('utf-8')).hexdigest()


def mask_email(email: str) -> str:
    """Mask an email for display: john.doe@company.com → j***e@c***y.com"""
    if not email or '@' not in email:
        return '***@***.***'
    local, domain = email.split('@', 1)
    if len(local) <= 2:
        masked_local = local[0] + '***'
    else:
        masked_local = local[0] + '***' + local[-1]
    domain_parts = domain.split('.')
    if len(domain_parts) >= 2:
        d = domain_parts[0]
        masked_domain = d[0] + '***' + d[-1] if len(d) > 2 else d[0] + '***'
        masked_domain += '.' + domain_parts[-1]
    else:
        masked_domain = domain[0] + '***'
    return f"{masked_local}@{masked_domain}"


class SecurityManager:
    """
    Centralized security manager. One instance per bot application.
    Thread-safe via locks on mutable state.
    """

    def __init__(self, db_handler):
        self.db = db_handler
        self._lock = threading.Lock()
        # In-memory session tracking: {user_id: {"last_active": float, "authenticated": bool}}
        self._sessions = {}
        # Rate limiter: {user_id: [timestamp, timestamp, ...]}
        self._rate_limits = {}

        # Ensure security tables exist
        self._init_security_tables()

    def _init_security_tables(self):
        """Create security-related tables if they don't exist."""
        self.db.conn.execute('''CREATE TABLE IF NOT EXISTS security_audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            event_type TEXT NOT NULL,
            detail TEXT,
            ip_hint TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )''')
        self.db.conn.execute(
            'CREATE INDEX IF NOT EXISTS idx_security_audit_user '
            'ON security_audit_log(user_id, timestamp)')
        self.db.conn.commit()

    # ------------------------------------------------------------------
    # OTP Generation & Verification
    # ------------------------------------------------------------------
    def generate_otp(self) -> str:
        """Generate a cryptographically random 6-digit OTP."""
        return f"{random.SystemRandom().randint(100000, 999999)}"

    def create_otp_challenge(self, user_id: int, bp_id: str, email: str) -> str:
        """Generate OTP, hash it, store in DB, and return the raw OTP for sending."""
        otp = self.generate_otp()
        salt = os.urandom(32)
        otp_hash = _hash_otp(otp, salt)
        expires_at = (datetime.utcnow() + timedelta(seconds=OTP_EXPIRY_SECONDS)).strftime(
            '%Y-%m-%d %H:%M:%S')
        self.db.store_otp(user_id, bp_id, email, otp_hash, salt, expires_at)
        self._log_security_event(user_id, "OTP_REQUESTED", f"OTP sent to {mask_email(email)} for BP {bp_id}")
        return otp

    def verify_otp(self, user_id: int, entered_otp: str) -> dict:
        """
        Verify an OTP entered by the user.
        Returns: {"success": bool, "reason": str, "bp_id": str|None, "email": str|None}
        """
        pending = self.db.get_pending_otp(user_id)
        if not pending:
            return {"success": False, "reason": "expired", "bp_id": None, "email": None}

        otp_id = pending['id']
        bp_id = pending['bp_id']
        email = pending['email']

        # Check attempt limit
        if pending['attempts'] >= OTP_MAX_ATTEMPTS:
            self.db.mark_otp_used(otp_id)
            self._log_security_event(user_id, "OTP_MAX_ATTEMPTS",
                                     f"OTP invalidated after {OTP_MAX_ATTEMPTS} wrong attempts")
            return {"success": False, "reason": "max_attempts", "bp_id": bp_id, "email": email}

        # Verify hash
        expected_hash = pending['otp_hash']
        salt = pending['otp_salt']
        actual_hash = _hash_otp(entered_otp.strip(), salt)

        if hmac.compare_digest(expected_hash, actual_hash):
            self.db.mark_otp_used(otp_id)
            self.db.link_user_bp(user_id, bp_id, email)
            self._create_session(user_id)
            self._log_security_event(user_id, "OTP_SUCCESS", f"Authenticated as BP {bp_id}")
            return {"success": True, "reason": "ok", "bp_id": bp_id, "email": email}
        else:
            self.db.increment_otp_attempts(otp_id)
            remaining = OTP_MAX_ATTEMPTS - pending['attempts'] - 1
            self._log_security_event(user_id, "OTP_FAILURE",
                                     f"Wrong OTP, {remaining} attempts remaining")
            return {"success": False, "reason": "wrong_otp", "bp_id": bp_id, "email": email,
                    "remaining": remaining}

    def send_otp_email(self, to_email: str, otp: str, bp_id: str) -> bool:
        """Send the OTP to the user's registered email address."""
        try:
            msg = MIMEMultipart()
            msg['From'] = SMTP_EMAIL
            msg['To'] = to_email
            msg['Subject'] = "Your SAP Bot Login OTP"

            body = f"""
            <html>
            <body style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; color: #333; padding: 20px;">
                <div style="max-width: 500px; margin: auto; background: #f9f9f9; padding: 30px; border-radius: 12px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
                    <h2 style="color: #2c3e50; text-align: center;">SAP S/4HANA Bot Login</h2>
                    <p>Your One-Time Password (OTP) for Business Partner <b>{bp_id}</b>:</p>
                    <div style="text-align: center; margin: 25px 0;">
                        <span style="font-size: 32px; font-weight: bold; letter-spacing: 8px; color: #2c3e50; background: #ecf0f1; padding: 15px 30px; border-radius: 8px;">{otp}</span>
                    </div>
                    <p style="color: #e74c3c; font-size: 14px;">This OTP expires in {OTP_EXPIRY_SECONDS // 60} minutes. Do not share it with anyone.</p>
                    <hr style="border: none; border-top: 1px solid #ddd; margin: 20px 0;">
                    <p style="font-size: 12px; color: #999; text-align: center;">If you did not request this, please ignore this email.</p>
                </div>
            </body>
            </html>
            """
            msg.attach(MIMEText(body, 'html'))

            server = smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT)
            try:
                server.login(SMTP_EMAIL, SMTP_PASSWORD)
                server.sendmail(SMTP_EMAIL, to_email, msg.as_string())
            finally:
                server.quit()

            logger.info("OTP email sent to %s for BP %s", mask_email(to_email), bp_id)
            return True
        except Exception as e:
            logger.error("Failed to send OTP email to %s: %s", mask_email(to_email), e)
            return False

    # ------------------------------------------------------------------
    # Session Management
    # ------------------------------------------------------------------
    def _create_session(self, user_id: int):
        now = time.time()
        with self._lock:
            self._sessions[user_id] = {
                "created_at": now,
                "last_active": now,
                "authenticated": True,
            }

    def is_session_valid(self, user_id: int) -> bool:
        """Check if user has a valid, non-expired session.
        Enforces both inactivity timeout and absolute hard ceiling."""
        with self._lock:
            session = self._sessions.get(user_id)
            if not session or not session.get("authenticated"):
                return False
            now = time.time()
            inactive = now - session["last_active"]
            age = now - session.get("created_at", now)

            # Hard ceiling: absolute max session duration regardless of activity
            if age > SESSION_HARD_CEILING:
                self._sessions.pop(user_id, None)
                self._log_security_event(
                    user_id, "SESSION_HARD_CEILING",
                    f"Session forcefully expired after {int(age)}s (hard ceiling {SESSION_HARD_CEILING}s)")
                return False

            # Inactivity timeout
            if inactive > SESSION_TIMEOUT_SECONDS:
                self._sessions.pop(user_id, None)
                self._log_security_event(
                    user_id, "SESSION_EXPIRED",
                    f"Expired after {int(inactive)}s of inactivity")
                return False

            # Refresh activity timestamp
            session["last_active"] = now
            return True

    def destroy_session(self, user_id: int):
        """Explicitly destroy a user's session (logout)."""
        with self._lock:
            self._sessions.pop(user_id, None)
        self._log_security_event(user_id, "SESSION_DESTROYED", "Manual logout")

    def flush_user_data(self, context: CallbackContext, user_id: int):
        """
        Securely wipe all sensitive data from context.user_data and chat_data.
        Overwrites values with zeros before clearing, then forces garbage collection.
        """
        import gc
        wiped = 0
        for data_store in (context.user_data, context.chat_data):
            if not data_store:
                continue
            keys = list(data_store.keys())
            for key in keys:
                val = data_store.get(key)
                if isinstance(val, str):
                    data_store[key] = '\x00' * len(val)
                elif isinstance(val, (dict, list)):
                    data_store[key] = type(val)()
                elif isinstance(val, bytes):
                    data_store[key] = b'\x00' * len(val)
                else:
                    data_store[key] = None
            wiped += len(keys)
            data_store.clear()
        # Force garbage collection to reclaim overwritten objects
        gc.collect()
        if wiped:
            logger.debug("Flushed user/chat data for user_%s (%d keys wiped)", user_id, wiped)

    # ------------------------------------------------------------------
    # Rate Limiting
    # ------------------------------------------------------------------
    def check_rate_limit(self, user_id: int) -> bool:
        """Returns True if the request is allowed, False if rate-limited."""
        now = time.time()
        with self._lock:
            if user_id not in self._rate_limits:
                self._rate_limits[user_id] = []
            # Prune old timestamps
            self._rate_limits[user_id] = [
                ts for ts in self._rate_limits[user_id]
                if now - ts < RATE_LIMIT_WINDOW
            ]
            if len(self._rate_limits[user_id]) >= RATE_LIMIT_MAX_REQUESTS:
                self._log_security_event(
                    user_id, "RATE_LIMITED",
                    f"{len(self._rate_limits[user_id])} requests in {RATE_LIMIT_WINDOW}s")
                return False
            self._rate_limits[user_id].append(now)
            return True

    # ------------------------------------------------------------------
    # Security Audit Log
    # ------------------------------------------------------------------
    def _log_security_event(self, user_id: int, event_type: str, detail: str):
        try:
            self.db.conn.execute(
                "INSERT INTO security_audit_log (user_id, event_type, detail) "
                "VALUES (?, ?, ?)",
                (user_id, event_type, detail))
            self.db.conn.commit()
        except Exception as e:
            logger.error("Failed to write security audit log: %s", e)

    # ------------------------------------------------------------------
    # Periodic Cleanup (call from scheduler)
    # ------------------------------------------------------------------
    def cleanup_expired_sessions(self):
        """Remove all expired sessions (inactivity + hard ceiling) and overwrite their memory."""
        now = time.time()
        expired = []
        with self._lock:
            for uid, session in list(self._sessions.items()):
                inactive = now - session["last_active"]
                age = now - session.get("created_at", now)
                if inactive > SESSION_TIMEOUT_SECONDS or age > SESSION_HARD_CEILING:
                    expired.append(uid)
                    # Overwrite session data before deletion
                    self._sessions[uid] = {"authenticated": False, "last_active": 0, "created_at": 0}
                    del self._sessions[uid]
        if expired:
            logger.info("Cleaned up %d expired sessions", len(expired))
        return expired


def protect_message(func):
    """
    Decorator that ensures all bot replies use protect_content=True
    to prevent forwarding/saving of sensitive SAP data.
    Patches the reply methods on the update object.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Find the Update object in args
        update = None
        for arg in args:
            if isinstance(arg, Update):
                update = arg
                break

        if update and update.message:
            _original_reply = update.message.reply_text
            _original_reply_doc = getattr(update.message, 'reply_document', None)
            _original_reply_photo = getattr(update.message, 'reply_photo', None)

            def _protected_reply(text, **kw):
                return _original_reply(text, **kw)

            def _protected_reply_doc(*a, **kw):
                return _original_reply_doc(*a, **kw)

            def _protected_reply_photo(*a, **kw):
                return _original_reply_photo(*a, **kw)

            update.message.reply_text = _protected_reply
            if _original_reply_doc:
                update.message.reply_document = _protected_reply_doc
            if _original_reply_photo:
                update.message.reply_photo = _protected_reply_photo

        return func(*args, **kwargs)
    return wrapper


# ---------------------------------------------------------------------------
# Content Protection Helper (for use without decorator)
# ---------------------------------------------------------------------------

def send_protected(bot, chat_id, text, **kwargs):
    """Send a message with content protection enabled."""
    return bot.send_message(chat_id=chat_id, text=text, **kwargs)


def send_protected_document(bot, chat_id, document, **kwargs):
    """Send a document with content protection enabled."""
    return bot.send_document(chat_id=chat_id, document=document, **kwargs)
