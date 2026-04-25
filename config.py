"""
Application configuration.

All secrets and credentials are loaded from environment variables.
For local development, place them in a .env file (never commit it).

Non-secret constants (SAP API paths, business logic mappings) remain here.
"""

import os
import sys

# ---------------------------------------------------------------------------
# Load .env file if present (development convenience)
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # python-dotenv not installed; rely on real environment variables
    pass


def _require_env(key: str) -> str:
    """Retrieve a required environment variable or exit with a clear error."""
    value = os.environ.get(key)
    if not value:
        print(f"FATAL: Required environment variable '{key}' is not set.", file=sys.stderr)
        sys.exit(1)
    return value


def _get_env(key: str, default: str = "") -> str:
    """Retrieve an optional environment variable with a default."""
    return os.environ.get(key, default)


# ---------------------------------------------------------------------------
# Telegram Settings (secrets)
# ---------------------------------------------------------------------------
SALES_BOT_TOKEN: str = _require_env("SALES_BOT_TOKEN")
MANAGER_BOT_TOKEN: str = _require_env("MANAGER_BOT_TOKEN")
MANAGER_BOT_USERNAME: str = _require_env("MANAGER_BOT_USERNAME")

# ---------------------------------------------------------------------------
# SAP S/4HANA Cloud Credentials (secrets)
# ---------------------------------------------------------------------------
SAP_USER: str = _require_env("SAP_USER")
SAP_PASSWORD: str = _require_env("SAP_PASSWORD")
SAP_BASE_URL: str = _require_env("SAP_BASE_URL")

# ---------------------------------------------------------------------------
# Google Gemini AI (secret)
# ---------------------------------------------------------------------------
GOOGLE_API_KEY: str = _require_env("GOOGLE_API_KEY")

# ---------------------------------------------------------------------------
# MiniMax AI — Cross-vendor fallback when Gemini is rate-limited (optional)
# ---------------------------------------------------------------------------
MINIMAX_API_KEY: str = _get_env("MINIMAX_API_KEY", "")  # SECURITY: Never hardcode API keys — set via env var

# ---------------------------------------------------------------------------
# SMTP / Email Settings (secrets)
# ---------------------------------------------------------------------------
SMTP_SERVER: str = _get_env("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT: int = int(_get_env("SMTP_PORT", "465"))
SMTP_EMAIL: str = _require_env("SMTP_EMAIL")
SMTP_PASSWORD: str = _require_env("SMTP_PASSWORD")
MANAGER_EMAIL: str = _require_env("MANAGER_EMAIL")

# ---------------------------------------------------------------------------
# Approval Server
# ---------------------------------------------------------------------------
SERVER_IP: str = _get_env("SERVER_IP", "http://127.0.0.1:5000")
APPROVAL_SERVER_PORT: int = int(_get_env("APPROVAL_SERVER_PORT", "5000"))

# ---------------------------------------------------------------------------
# Discount Approval Thresholds
# ---------------------------------------------------------------------------
DISCOUNT_THRESHOLD: float = float(_get_env("DISCOUNT_THRESHOLD", "5.0"))
DISCOUNT_CONDITION_TYPE: str = _get_env("DISCOUNT_CONDITION_TYPE", "YK07")

# ---------------------------------------------------------------------------
# SAP OData / SOAP API Endpoints (non-secret, derived from base URL)
#
# WARNING: Do NOT modify these paths. They are tested against SAP S/4HANA
# Cloud and match the exact OData service paths in production.
# ---------------------------------------------------------------------------
BUSINESS_PARTNER_API: str = f"{SAP_BASE_URL}/sap/opu/odata/sap/API_BUSINESS_PARTNER"
PRODUCT_API: str = f"{SAP_BASE_URL}/sap/opu/odata/sap/API_PRODUCT_SRV"
PRICE_API: str = f"{SAP_BASE_URL}/sap/opu/odata/sap/API_SLSPRICINGCONDITIONRECORD_SRV"
ORDER_API: str = f"{SAP_BASE_URL}/sap/opu/odata/sap/API_SALES_ORDER_SRV"
QUOTE_API: str = f"{SAP_BASE_URL}/sap/opu/odata/sap/API_SALES_QUOTATION_SRV"
BILLING_API: str = f"{SAP_BASE_URL}/sap/opu/odata/sap/API_BILLING_DOCUMENT_SRV"
CREDIT_API: str = f"{SAP_BASE_URL}/sap/opu/odata/sap/YY1_TOTALEXPOSURE_CDS"
STOCK_API: str = f"{SAP_BASE_URL}/sap/opu/odata/sap/API_MATERIAL_STOCK_SRV"
SUPPLIER_INVOICE_API: str = f"{SAP_BASE_URL}/sap/opu/odata/sap/API_SUPPLIERINVOICE_PROCESS_SRV"
COMPANY_API: str = f"{SAP_BASE_URL}/sap/opu/odata/sap/API_COMPANYCODE_SRV"
GL_ACCOUNT_API: str = f"{SAP_BASE_URL}/sap/opu/odata/sap/API_GLACCOUNTINCHARTOFACCOUNTS_SRV"
COSTCENTER_API: str = f"{SAP_BASE_URL}/sap/opu/odata/sap/API_COSTCENTER_SRV"
TAX_API: str = f"{SAP_BASE_URL}/sap/opu/odata/sap/YY1_ZTAXFINALAPI_CDS/YY1_ZTaxFinalAPI"
JOURNAL_ENTRY_API: str = f"{SAP_BASE_URL}/sap/bc/srt/scs_ext/sap/journalentrycreaterequestconfi"
PRINT_QUEUE_API: str = f"{SAP_BASE_URL}/sap/opu/odata/sap/API_CLOUD_PRINT_PULL_SRV"

# ---------------------------------------------------------------------------
# Business Constants (non-secret)
# ---------------------------------------------------------------------------
SALES_QUEUE_NAME: str = "SALES_QUEUE"
BP_GROUPING: str = "BP02"
DB_NAME: str = _get_env("DB_NAME", "sap_bot_logs.db")

# ---------------------------------------------------------------------------
# OTP Authentication
# ---------------------------------------------------------------------------
OTP_EXPIRY_SECONDS: int = int(_get_env("OTP_EXPIRY_SECONDS", "300"))  # 5 minutes
OTP_MAX_ATTEMPTS: int = int(_get_env("OTP_MAX_ATTEMPTS", "3"))

# ---------------------------------------------------------------------------
# GDPR / Data Privacy
# ---------------------------------------------------------------------------
PRIVACY_POLICY_VERSION: str = _get_env("PRIVACY_POLICY_VERSION", "1.0")
SESSION_HARD_CEILING_SECONDS: int = int(_get_env("SESSION_HARD_CEILING", "14400"))  # 4 hours absolute max

# ---------------------------------------------------------------------------
# Expense Defaults (configurable via environment)
# ---------------------------------------------------------------------------
PETTY_CASH_GL: str = _get_env("PETTY_CASH_GL", "65100000")
DEFAULT_EXPENSE_GL: str = _get_env("DEFAULT_EXPENSE_GL", "61004000")
STOCK_LOW_THRESHOLD: int = int(_get_env("STOCK_LOW_THRESHOLD", "50"))
ITEMS_PER_PAGE: int = int(_get_env("ITEMS_PER_PAGE", "5"))

# Customer Master Data Logic for Domestic/Export classification
CUSTOMER_MASTER_LOGIC: dict = {
    '1000': {'domestic': ['DO', 'DC'], 'export': ['EX']},
    '2000': {'domestic': ['DC'], 'export': ['ET']},
    '5000': {'domestic': ['DR'], 'export': ['EX']},
}
