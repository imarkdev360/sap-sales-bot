"""
B2B Secure SAP Handler — Zero-Trust Data Isolation Proxy.

Wraps the existing SAPHandler to enforce per-customer data isolation
for B2B (external) users. Internal sales reps bypass this entirely
and use SAPHandler directly.

Design:
  - GET calls: auto-inject $filter with the locked BP ID
  - POST calls: validate customer field matches the locked BP ID
  - Passthrough: calls that aren't customer-specific (materials, stock, GL, tax)

Usage in bot.py:
    bp_id = db.get_b2b_customer_id(user.id)
    if bp_id:
        sap = B2BSecureSAPHandler(real_sap_handler, bp_id)
    else:
        sap = real_sap_handler  # internal rep, full access
"""

from logger_setup import get_logger

logger = get_logger(__name__)


def _normalize_bp(bp_id):
    """Strip leading zeros for safe comparison. '0000001000' -> '1000'."""
    if not bp_id:
        return ''
    return str(bp_id).lstrip('0') or '0'


class B2BSecureSAPHandler:
    """
    Proxy that delegates to a real SAPHandler but enforces
    data isolation for a single Business Partner.
    """

    def __init__(self, sap_handler, locked_bp_id: str):
        self._sap = sap_handler
        self._bp_id = locked_bp_id  # always 10-char padded
        self._bp_norm = _normalize_bp(locked_bp_id)  # for comparison

    def _is_own_bp(self, bp_id):
        """Check if a BP ID belongs to the locked user (format-safe)."""
        return _normalize_bp(bp_id) == self._bp_norm

    @staticmethod
    def _extract_customer_from_details(details):
        """Extract customer BP ID from a SAP details dict, checking all known keys."""
        if not details:
            return None
        for key in ('SoldToParty', 'Customer', 'customer', 'BusinessPartner', 'SoldtoParty'):
            val = details.get(key)
            if val:
                return val
        return None

    # ==================================================================
    # BLOCKED OPERATIONS (B2B customers must never access these)
    # ==================================================================

    def get_all_customers_with_expansion(self):
        logger.warning("B2B BLOCKED: get_all_customers_with_expansion by BP %s", self._bp_id)
        return []

    def get_customers(self, skip=0, top=5):
        logger.warning("B2B BLOCKED: get_customers (directory browse) by BP %s", self._bp_id)
        return []

    def search_customers(self, keyword):
        logger.warning("B2B BLOCKED: search_customers by BP %s", self._bp_id)
        return []

    def create_business_partner_customer(self, bp_data, address_data, contact_data):
        logger.warning("B2B BLOCKED: create_business_partner_customer by BP %s", self._bp_id)
        return {"success": False, "error": "B2B customers cannot create business partners."}

    # ==================================================================
    # CUSTOMER DETAILS — locked to own BP only
    # ==================================================================

    def get_customer_details(self, bp_id):
        if not self._is_own_bp(bp_id):
            logger.warning("B2B ISOLATION: BP %s tried to access details of %s", self._bp_id, bp_id)
            return None
        return self._sap.get_customer_details(bp_id)

    def get_customer_sales_area(self, customer_id):
        if not self._is_own_bp(customer_id):
            logger.warning("B2B ISOLATION: BP %s tried to access sales area of %s", self._bp_id, customer_id)
            return None
        return self._sap.get_customer_sales_area(customer_id)

    # ==================================================================
    # SALES ORDERS — filtered to own BP
    # ==================================================================

    def get_sales_orders(self, skip=0, top=5):
        return self._sap.get_sales_orders_for_customer(self._bp_id, skip=skip, top=top)

    def get_sales_order_details(self, order_id):
        details = self._sap.get_sales_order_details(order_id)
        if details:
            owner_bp = self._extract_customer_from_details(details)
            logger.info("B2B ORDER DETAIL: order=%s, dict_keys=%s, resolved_owner=%s, locked_bp=%s",
                        order_id, list(details.keys()), owner_bp, self._bp_id)
            if not self._is_own_bp(owner_bp):
                logger.warning("B2B ISOLATION: BP %s tried to view order %s (belongs to %s)",
                               self._bp_id, order_id, owner_bp)
                return None
        return details

    def create_sales_order(self, customer_id, items_list, customer_ref, discount_pct=0.0, ref_doc=None):
        if not self._is_own_bp(customer_id):
            logger.warning("B2B POST BLOCKED: BP %s tried to create order for %s",
                           self._bp_id, customer_id)
            return {"success": False, "error": "You can only create orders for your own account."}
        return self._sap.create_sales_order(customer_id, items_list, customer_ref, discount_pct, ref_doc)

    # ==================================================================
    # QUOTATIONS — filtered to own BP
    # ==================================================================

    def get_quotations(self, skip=0, top=5):
        return self._sap.get_quotations_for_customer(self._bp_id, skip=skip, top=top)

    def get_quotation_details(self, quote_id):
        details = self._sap.get_quotation_details(quote_id)
        if details:
            owner_bp = self._extract_customer_from_details(details)
            logger.info("B2B QUOTE DETAIL: quote=%s, dict_keys=%s, resolved_owner=%s, locked_bp=%s",
                        quote_id, list(details.keys()), owner_bp, self._bp_id)
            if not self._is_own_bp(owner_bp):
                logger.warning("B2B ISOLATION: BP %s tried to view quote %s (belongs to %s)",
                               self._bp_id, quote_id, owner_bp)
                return None
        return details

    def create_sales_quotation(self, customer_id, items_list, customer_ref, valid_to_date=None):
        if not self._is_own_bp(customer_id):
            logger.warning("B2B POST BLOCKED: BP %s tried to create quote for %s",
                           self._bp_id, customer_id)
            return {"success": False, "error": "You can only create quotations for your own account."}
        return self._sap.create_sales_quotation(customer_id, items_list, customer_ref, valid_to_date)

    # ==================================================================
    # INVOICES (Billing Documents) — filtered to own BP
    # ==================================================================

    def get_customer_invoices(self, skip=0, top=5, status_filter=None):
        return self._sap.get_customer_invoices(self._bp_id, skip, top, status_filter=status_filter)

    def get_invoice_details(self, invoice_no):
        details = self._sap.get_invoice_details(invoice_no)
        if details:
            owner_bp = self._extract_customer_from_details(details)
            logger.info("B2B INVOICE DETAIL: invoice=%s, dict_keys=%s, resolved_owner=%s, locked_bp=%s",
                        invoice_no, list(details.keys()), owner_bp, self._bp_id)
            if not self._is_own_bp(owner_bp):
                logger.warning("B2B ISOLATION: BP %s tried to view invoice %s (belongs to %s)",
                               self._bp_id, invoice_no, owner_bp)
                return None
        return details

    # ==================================================================
    # CREDIT — locked to own BP
    # ==================================================================

    def get_credit_exposure(self, customer_id):
        if not self._is_own_bp(customer_id):
            logger.warning("B2B ISOLATION: BP %s tried to check credit of %s", self._bp_id, customer_id)
            return None
        return self._sap.get_credit_exposure(customer_id)

    # ==================================================================
    # BILLING / REVENUE — locked to own BP
    # ==================================================================

    def get_customer_revenue_summary(self, bp_id):
        if not self._is_own_bp(bp_id):
            logger.warning("B2B ISOLATION: BP %s tried to view revenue of %s", self._bp_id, bp_id)
            return None
        return self._sap.get_customer_revenue_summary(bp_id)

    # ==================================================================
    # PRICING — customer-specific, locked to own BP
    # ==================================================================

    def get_product_price(self, material_id, customer_id=None):
        return self._sap.get_product_price(material_id, self._bp_id)

    # ==================================================================
    # PASSTHROUGH — shared catalog data, no customer filter needed
    # ==================================================================

    def search_products(self, keyword):
        return self._sap.search_products(keyword)

    def get_stock_overview(self, material_id):
        return self._sap.get_stock_overview(material_id)

    # ==================================================================
    # PASSTHROUGH — internal utilities delegated as-is
    # ==================================================================

    def get_order_fulfillment_status(self, order_id):
        details = self._sap.get_sales_order_details(order_id)
        if details:
            owner_bp = self._extract_customer_from_details(details)
            if not self._is_own_bp(owner_bp):
                logger.warning("B2B ISOLATION: BP %s tried to track order %s", self._bp_id, order_id)
                return None
        return self._sap.get_order_fulfillment_status(order_id)

    def fetch_next_queue_item(self, queue_name):
        return self._sap.fetch_next_queue_item(queue_name)

    def fetch_queue_document(self, item_id, queue_name):
        return self._sap.fetch_queue_document(item_id, queue_name)

    # ==================================================================
    # CATCH-ALL — block any method not explicitly listed
    # ==================================================================

    def __getattr__(self, name):
        logger.warning("B2B BLOCKED: unproxied method '%s' called by BP %s", name, self._bp_id)
        def _blocked(*args, **kwargs):
            return None
        return _blocked
