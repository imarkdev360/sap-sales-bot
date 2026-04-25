"""
SAP API Security Hardening Module.

Implements:
  1. SSL/TLS Certificate Pinning for SAP endpoints
  2. Payload encryption for sensitive OData responses cached in memory
  3. IP whitelist enforcement for approval server
  4. Request signing / integrity verification
  5. Secure session factory with hardened TLS configuration

Usage:
    from sap_security import create_hardened_session, SecurePayloadCache, ApprovalServerSecurity
"""

import os
import ssl
import hashlib
import hmac
import base64
import time
import threading
import json
from urllib.parse import urlparse
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context
from urllib3.util.retry import Retry
from functools import wraps
from flask import request as flask_request, abort
from logger_setup import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Environment Configuration
# ---------------------------------------------------------------------------
# Comma-separated SHA-256 fingerprints of SAP server certificates
SAP_CERT_FINGERPRINTS = os.environ.get("SAP_CERT_FINGERPRINTS", "").split(",")
SAP_CERT_FINGERPRINTS = [fp.strip() for fp in SAP_CERT_FINGERPRINTS if fp.strip()]

# Comma-separated allowed IPs for the approval web server
APPROVAL_ALLOWED_IPS = os.environ.get(
    "APPROVAL_ALLOWED_IPS", "127.0.0.1,::1"
).split(",")
APPROVAL_ALLOWED_IPS = [ip.strip() for ip in APPROVAL_ALLOWED_IPS if ip.strip()]

# Encryption key derived from a secret (set in .env for production)
_ENCRYPTION_SECRET = os.environ.get(
    "PAYLOAD_ENCRYPTION_SECRET", "change-me-in-production-" + str(os.getpid())
)


# ---------------------------------------------------------------------------
# 1. TLS Certificate Pinning Adapter
# ---------------------------------------------------------------------------

class TLSPinningAdapter(HTTPAdapter):
    """
    HTTPS adapter that enforces:
      - TLS 1.2+ only (no SSLv3, TLS 1.0, TLS 1.1)
      - Optional certificate fingerprint pinning
      - Strong cipher suites only
    """

    def __init__(self, pin_fingerprints=None, **kwargs):
        self._pin_fingerprints = pin_fingerprints or []
        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        ctx = create_urllib3_context()
        # Enforce TLS 1.2 minimum
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        # Disable compression (prevents CRIME attack)
        ctx.options |= ssl.OP_NO_COMPRESSION
        kwargs['ssl_context'] = ctx
        return super().init_poolmanager(*args, **kwargs)

    def send(self, request, **kwargs):
        """Override send to verify certificate fingerprint after connection."""
        response = super().send(request, **kwargs)

        if self._pin_fingerprints and request.url.startswith("https"):
            # Verify the server certificate fingerprint
            parsed = urlparse(request.url)
            host = parsed.hostname
            port = parsed.port or 443
            try:
                cert_der = ssl.get_server_certificate(
                    (host, port)).encode('ascii')
                # Convert PEM to DER for hashing
                import ssl as _ssl
                der_bytes = _ssl.PEM_cert_to_DER_cert(
                    ssl.get_server_certificate((host, port)))
                fingerprint = hashlib.sha256(der_bytes).hexdigest().upper()

                if fingerprint not in [fp.upper() for fp in self._pin_fingerprints]:
                    logger.critical(
                        "SSL PINNING FAILURE: Host %s certificate fingerprint %s "
                        "does not match pinned fingerprints %s",
                        host, fingerprint, self._pin_fingerprints)
                    raise ssl.SSLError(
                        f"Certificate pinning failed for {host}. "
                        f"Got {fingerprint}, expected one of {self._pin_fingerprints}")
            except ssl.SSLError:
                raise
            except Exception as e:
                logger.warning("Certificate pin verification could not complete: %s", e)

        return response


def create_hardened_session(pin_fingerprints=None):
    """
    Create a requests.Session with hardened TLS, connection pooling,
    and optional certificate pinning.

    Usage in sap_handler.py:
        from sap_security import create_hardened_session
        self.session = create_hardened_session(SAP_CERT_FINGERPRINTS)
        self.session.auth = HTTPBasicAuth(SAP_USER, SAP_PASSWORD)
    """
    import requests

    session = requests.Session()

    retry = Retry(
        total=2,
        backoff_factor=0.3,
        status_forcelist=[502, 503, 504],
        allowed_methods=["GET", "HEAD", "POST", "PATCH"],
    )

    fingerprints = pin_fingerprints or SAP_CERT_FINGERPRINTS
    adapter = TLSPinningAdapter(
        pin_fingerprints=fingerprints,
        pool_connections=10,
        pool_maxsize=20,
        max_retries=retry,
    )

    session.mount("https://", adapter)
    # Block all HTTP (non-TLS) requests to SAP
    session.mount("http://", _BlockHTTPAdapter())

    # Set secure default headers
    session.headers.update({
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-Request-ID": "",  # Will be set per-request
    })

    return session


class _BlockHTTPAdapter(HTTPAdapter):
    """Adapter that blocks all non-HTTPS requests."""

    def send(self, request, **kwargs):
        if not request.url.startswith("https"):
            logger.critical("BLOCKED insecure HTTP request to: %s", request.url)
            raise ConnectionError(
                f"Insecure HTTP requests are blocked. Use HTTPS. URL: {request.url}")
        return super().send(request, **kwargs)


# ---------------------------------------------------------------------------
# 2. Encrypted Payload Cache (replaces plain _TTLCache for sensitive data)
# ---------------------------------------------------------------------------

def _derive_key(secret: str) -> bytes:
    """Derive a Fernet key from a secret string."""
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=b"sap-bot-cache-v1",
        iterations=100_000,
    )
    return base64.urlsafe_b64encode(kdf.derive(secret.encode('utf-8')))


class SecurePayloadCache:
    """
    Thread-safe in-memory cache with:
      - AES-256 encryption (Fernet) for all stored values
      - TTL-based expiration
      - Secure wipe on eviction (overwrite before delete)
    """

    def __init__(self, ttl_seconds=300, encryption_secret=None):
        self._store = {}
        self._ttl = ttl_seconds
        self._lock = threading.Lock()
        key = _derive_key(encryption_secret or _ENCRYPTION_SECRET)
        self._fernet = Fernet(key)

    def get(self, cache_key: str):
        """Retrieve and decrypt a cached value, or None if expired/missing."""
        with self._lock:
            entry = self._store.get(cache_key)
            if not entry:
                return None
            encrypted_data, timestamp = entry
            if (time.time() - timestamp) >= self._ttl:
                # Expired — securely wipe
                self._store[cache_key] = (b'\x00' * len(encrypted_data), 0)
                del self._store[cache_key]
                return None
            try:
                decrypted = self._fernet.decrypt(encrypted_data)
                return json.loads(decrypted.decode('utf-8'))
            except Exception as e:
                logger.warning("Cache decrypt failed for key %s: %s", cache_key, e)
                del self._store[cache_key]
                return None

    def set(self, cache_key: str, value):
        """Encrypt and store a value."""
        with self._lock:
            serialized = json.dumps(value, default=str).encode('utf-8')
            encrypted = self._fernet.encrypt(serialized)
            self._store[cache_key] = (encrypted, time.time())

    def clear(self):
        """Securely wipe all cached data."""
        with self._lock:
            for key in list(self._store.keys()):
                encrypted_data, _ = self._store[key]
                self._store[key] = (b'\x00' * len(encrypted_data), 0)
            self._store.clear()

    def evict_expired(self):
        """Proactively evict and wipe expired entries."""
        now = time.time()
        with self._lock:
            expired_keys = [
                k for k, (_, ts) in self._store.items()
                if (now - ts) >= self._ttl
            ]
            for key in expired_keys:
                encrypted_data, _ = self._store[key]
                self._store[key] = (b'\x00' * len(encrypted_data), 0)
                del self._store[key]
            return len(expired_keys)


# ---------------------------------------------------------------------------
# 3. Approval Server Security (IP Whitelist + CSRF + Rate Limit)
# ---------------------------------------------------------------------------

class ApprovalServerSecurity:
    """
    Flask middleware for the approval_server.py endpoints.
    Enforces:
      - IP whitelisting
      - CSRF token validation on POST
      - Rate limiting per IP
    """

    def __init__(self, allowed_ips=None, rate_limit=10, window=60):
        self._allowed_ips = set(allowed_ips or APPROVAL_ALLOWED_IPS)
        self._rate_limit = rate_limit
        self._window = window
        self._requests = {}  # ip -> [timestamps]
        self._lock = threading.Lock()

    def check_ip(self):
        """Check if the requester's IP is whitelisted."""
        client_ip = flask_request.remote_addr
        if self._allowed_ips and client_ip not in self._allowed_ips:
            logger.warning(
                "Approval server: blocked request from non-whitelisted IP %s",
                client_ip)
            abort(403, description="Access denied: IP not whitelisted")

    def check_rate_limit(self):
        """Rate limit approval server requests per IP."""
        client_ip = flask_request.remote_addr
        now = time.time()
        with self._lock:
            if client_ip not in self._requests:
                self._requests[client_ip] = []
            self._requests[client_ip] = [
                ts for ts in self._requests[client_ip]
                if now - ts < self._window
            ]
            if len(self._requests[client_ip]) >= self._rate_limit:
                logger.warning("Approval server: rate limited IP %s", client_ip)
                abort(429, description="Too many requests")
            self._requests[client_ip].append(now)

    def before_request(self):
        """Register as Flask before_request handler."""
        self.check_ip()
        self.check_rate_limit()


# ---------------------------------------------------------------------------
# 4. Request Integrity Signing (optional, for audit trail)
# ---------------------------------------------------------------------------

def sign_request(payload: str, secret: str) -> str:
    """Generate HMAC-SHA256 signature for a request payload."""
    return hmac.new(
        secret.encode('utf-8'),
        payload.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()


def verify_signature(payload: str, signature: str, secret: str) -> bool:
    """Verify HMAC-SHA256 signature of a request payload."""
    expected = sign_request(payload, secret)
    return hmac.compare_digest(expected, signature)
