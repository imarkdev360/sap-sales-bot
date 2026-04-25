"""
Centralized ConversationHandler state constants.

This is the SINGLE SOURCE OF TRUTH for all state IDs used across the bot.
Every feature module must import states from here instead of defining its own.

WARNING: Do NOT change any state integer values. They are tied to the
ConversationHandler flow and have been tested in production.
"""

# --- Main Menu ---
MAIN_MENU: int = 0

# --- Customer States ---
CUSTOMER_MENU: int = 10
CUSTOMER_SEARCH_INPUT: int = 11
CUSTOMER_CREATE_CATEGORY: int = 12
CUSTOMER_CREATE_NAME: int = 13
CUSTOMER_CREATE_COUNTRY: int = 14
CUSTOMER_CREATE_REGION: int = 15
CUSTOMER_CREATE_CITY: int = 16
CUSTOMER_CREATE_STREET: int = 17
CUSTOMER_CREATE_POSTAL: int = 18
CUSTOMER_CREATE_MOBILE: int = 19
CUSTOMER_CREATE_EMAIL: int = 20
CUSTOMER_CREATE_CONFIRM: int = 21

# --- Material Search ---
MATERIAL_SEARCH_INPUT: int = 50
PRODUCT_SEARCH_INPUT: int = 51

# --- Order / Quote States ---
ORDER_ASK_CUSTOMER: int = 300
ORDER_ASK_REF: int = 301
ORDER_ASK_MATERIAL: int = 302
ORDER_ASK_QTY: int = 303
ORDER_ADD_MORE: int = 304
ORDER_CONFIRM: int = 305
ORDER_REMOVE_ITEM: int = 306
ORDER_ASK_DISCOUNT: int = 307
ORDER_ASK_QUOTE_ID: int = 308
ORDER_ASK_VALIDITY: int = 309

# --- Sales Menu ---
SALES_MENU: int = 400
SALES_SEARCH_INPUT: int = 401

# --- Dashboard States ---
DASH_SELECT_PERIOD: int = 500
DASH_ASK_START: int = 501
DASH_ASK_END: int = 502
DASH_ASK_TARGET: int = 503
DASH_SHOW_REPORT: int = 504
DASH_SELECT_STATUS_FILTER: int = 505

# --- Expense WITH Vendor States ---
E_CO: int = 600
E_V_SRCH: int = 601
E_V_CONF: int = 602
E_INPUT_METHOD: int = 603
E_SCAN_PHOTO: int = 604
E_REF: int = 605
E_DATE_DOC: int = 606
E_DATE_POST: int = 607
E_AMT: int = 608
E_DESC: int = 609
E_GL_SRCH: int = 610
E_CC_SRCH: int = 611
E_TAX_SEL: int = 612
E_ADD_MORE: int = 613
E_FINAL_REVIEW: int = 614
E_EDIT_MENU: int = 615
E_EDIT_VALUE: int = 616
E_VENDOR_TYPE: int = 617

# --- Expense WITHOUT Vendor States ---
EW_INPUT_METHOD: int = 801
EW_SCAN_PHOTO: int = 802
EW_REF: int = 803
EW_DATE_DOC: int = 804
EW_DATE_POST: int = 805
EW_AMT: int = 806
EW_DESC: int = 807
EW_GL_SRCH: int = 808
EW_CC_SRCH: int = 809
EW_TAX_SEL: int = 810
EW_FINAL_REVIEW: int = 811
EW_EDIT_MENU: int = 812
EW_EDIT_VALUE: int = 813

# --- Manager Approval States ---
MANAGER_REASON_INPUT: int = 999
MANAGER_CONFIRM_REJECT: int = 1000

# --- Manager Target Setting States ---
MGR_TARGET_PERIOD: int = 1100
MGR_TARGET_START: int = 1101
MGR_TARGET_END: int = 1102
MGR_TARGET_AMOUNT: int = 1103

# --- Manager User Management States ---
MGR_USER_LIST: int = 1200
MGR_USER_PERMISSIONS: int = 1201

# --- Customer 360 Panel ---
CUSTOMER_360: int = 1300

# --- Manager Analytics Dashboard ---
MGR_ANALYTICS: int = 1400
MGR_PETTY_CASH_MENU: int = 1401
MGR_PETTY_CASH_SET: int = 1402

# --- Dashboard Comparison Mode ---
DASH_COMPARE_SELECT: int = 506
DASH_COMPARE_START: int = 507
DASH_COMPARE_END: int = 508

# --- Delivery Tracking ---
DELIVERY_TRACKING: int = 310

# --- Favorites / Quick Actions ---
FAVORITES_MENU: int = 1500

# --- AI Copilot ---
COPILOT_CONFIRM: int = 1600

# --- OTP Authentication ---
OTP_ASK_BP_ID: int = 1700
OTP_ASK_CODE: int = 1701

# --- GDPR / Privacy Consent ---
GDPR_CONSENT: int = 1800
