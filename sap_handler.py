import requests
import json
import re
import os
import time
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from config import (
    SAP_USER, SAP_PASSWORD, SAP_BASE_URL, BUSINESS_PARTNER_API, PRODUCT_API,
    PRICE_API, ORDER_API, QUOTE_API, BILLING_API, CREDIT_API, STOCK_API,
    SUPPLIER_INVOICE_API, COMPANY_API, GL_ACCOUNT_API, COSTCENTER_API,
    TAX_API, JOURNAL_ENTRY_API, BP_GROUPING, DISCOUNT_CONDITION_TYPE,
)
from logger_setup import get_logger

logger = get_logger(__name__)


class _TTLCache:
    """Simple thread-safe in-memory cache with TTL expiration."""

    def __init__(self, ttl_seconds=300):
        self._store = {}
        self._ttl = ttl_seconds
        self._lock = threading.Lock()

    def get(self, key):
        with self._lock:
            entry = self._store.get(key)
            if entry and (time.time() - entry[1]) < self._ttl:
                return entry[0]
            if entry:
                del self._store[key]
        return None

    def set(self, key, value):
        with self._lock:
            self._store[key] = (value, time.time())

    def clear(self):
        with self._lock:
            self._store.clear()


class SAPHandler:
    def __init__(self):
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(SAP_USER, SAP_PASSWORD)
        self.session.headers.update({"Accept": "application/json", "Content-Type": "application/json"})

        # Connection pooling + retry for SAP APIs
        retry_strategy = Retry(total=2, backoff_factor=0.3, status_forcelist=[502, 503, 504])
        adapter = HTTPAdapter(pool_connections=10, pool_maxsize=20, max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        self.csrf_token = None
        self.cookies = None
        self.base_url = SAP_BASE_URL

        # In-memory caches (5 min TTL for reads, 2 min for searches)
        self._cache_customers = _TTLCache(ttl_seconds=300)
        self._cache_products = _TTLCache(ttl_seconds=300)
        self._cache_stock = _TTLCache(ttl_seconds=120)
        self._cache_price = _TTLCache(ttl_seconds=300)
        self._cache_companies = _TTLCache(ttl_seconds=600)

        # Thread pool for parallel SAP API calls
        self._executor = ThreadPoolExecutor(max_workers=4)

    def _parse_sap_date(self, sap_date_str):
        if not sap_date_str: return "N/A"
        try:
            timestamp = re.search(r'\d+', str(sap_date_str)).group()
            dt = datetime.fromtimestamp(int(timestamp) / 1000)
            return dt.strftime('%d-%m-%Y')
        except (AttributeError, ValueError, OSError) as e:
            logger.debug("Could not parse SAP date '%s': %s", sap_date_str, e)
            return "N/A"

    def _get_sap_timestamp(self, sap_date_str):
        if not sap_date_str: return None
        try:
            timestamp = re.search(r'\d+', str(sap_date_str)).group()
            return int(timestamp)
        except (AttributeError, ValueError):
            return None

    def _get_csrf_token(self, api_url):
        try:
            if "x-csrf-token" in self.session.headers: del self.session.headers["x-csrf-token"]
            self.session.cookies.clear()
            headers = {"x-csrf-token": "fetch", "Accept": "*/*"}
            clean_url = api_url.split('?')[0].rstrip('/')
            response = self.session.get(f"{clean_url}/$metadata", headers=headers, timeout=30)
            if response.status_code == 200:
                token = response.headers.get("x-csrf-token")
                if token:
                    self.csrf_token = token
                    self.cookies = response.cookies
                    self.session.headers.update({"x-csrf-token": token})
                    return True
            logger.warning("CSRF token fetch failed: HTTP %s from %s", response.status_code, clean_url)
            return False
        except requests.RequestException as e:
            logger.error("CSRF token request error: %s", e)
            return False

    def get_all_customers_with_expansion(self):
        try:
            url = f"{BUSINESS_PARTNER_API}/A_Customer?$expand=to_CustomerCompany,to_CustomerSalesArea&$select=Customer,CustomerName,to_CustomerCompany/CompanyCode,to_CustomerSalesArea/DistributionChannel&$format=json"
            res = self.session.get(url, timeout=45)
            if res.status_code == 200:
                return res.json().get('d', {}).get('results', [])
            return []
        except requests.RequestException as e:
            logger.error("SAP full customer fetch error: %s", e)
            return []

    def get_customers(self, skip=0, top=5):
        try:
            url = f"{BUSINESS_PARTNER_API}/A_Customer?$skip={skip}&$top={top}&$format=json&$select=Customer,CustomerName,CustomerFullName&$orderby=Customer desc"
            res = self.session.get(url, timeout=30)
            return res.json().get('d', {}).get('results', []) if res.status_code == 200 else []
        except requests.RequestException as e:
            logger.error("Get customers error: %s", e)
            return []

    def search_customers(self, keyword):
        try:
            term = keyword.strip().replace("'", "''")
            cache_key = f"cust_search_{term.lower()}"
            cached = self._cache_customers.get(cache_key)
            if cached is not None:
                return cached
            filter_str = f"substringof('{term}', CustomerName) or startswith(Customer, '{term}')"
            url = f"{BUSINESS_PARTNER_API}/A_Customer?$filter={filter_str}&$top=50&$format=json&$select=Customer,CustomerName,CustomerFullName"
            res = self.session.get(url, timeout=20)
            results = []
            if res.status_code == 200:
                items = res.json().get('d', {}).get('results', [])
                for i in items:
                    name = i.get('CustomerName') or i.get('CustomerFullName') or "Unknown"
                    results.append({"Customer": i['Customer'], "CustomerName": name})
            self._cache_customers.set(cache_key, results)
            return results
        except requests.RequestException as e:
            logger.error("Customer search error for '%s': %s", keyword, e)
            return []

    def get_customer_sales_area(self, customer_id):
        try:
            url = f"{BUSINESS_PARTNER_API}/A_CustomerSalesArea?$filter=Customer eq '{customer_id}'&$format=json&$top=1"
            res = self.session.get(url, timeout=30)
            if res.status_code == 200:
                r = res.json().get('d', {}).get('results', [])
                if r: return {"SalesOrganization": r[0]['SalesOrganization'],
                              "DistributionChannel": r[0]['DistributionChannel'],
                              "Division": r[0]['Division']}
            return None
        except requests.RequestException as e:
            logger.error("Sales area fetch error for customer '%s': %s", customer_id, e)
            return None

    def get_customer_details(self, bp_id):
        try:
            url = f"{BUSINESS_PARTNER_API}/A_BusinessPartner('{bp_id}')?$expand=to_BusinessPartnerAddress/to_EmailAddress,to_BusinessPartnerAddress/to_MobilePhoneNumber&$format=json"
            res = self.session.get(url, timeout=30)

            final_data = {}

            if res.status_code == 200:
                d = res.json().get('d', {})
                bp_cat = d.get('BusinessPartnerCategory')
                name = d.get(
                    'OrganizationBPName1') if bp_cat == '2' else f"{d.get('FirstName', '')} {d.get('LastName', '')}".strip()

                addr_data = d.get('to_BusinessPartnerAddress', {}).get('results', [])
                addr_txt, email, mobile = "N/A", "N/A", "N/A"

                if addr_data:
                    a = addr_data[0]
                    parts = [a.get('StreetName'), a.get('CityName'), a.get('Region'), a.get('Country')]
                    addr_txt = ", ".join([p for p in parts if p])
                    if a.get('to_EmailAddress', {}).get('results'):
                        email = a['to_EmailAddress']['results'][0].get('EmailAddress')
                    if a.get('to_MobilePhoneNumber', {}).get('results'):
                        mobile = a['to_MobilePhoneNumber']['results'][0].get('PhoneNumber')

                final_data = {
                    "BusinessPartner": bp_id,
                    "Name": name,
                    "Category": "Org" if bp_cat == '2' else "Person",
                    "Address": addr_txt,
                    "Email": email,
                    "Mobile": mobile
                }

                # Fetch Sales Area Data for Classification
                sa = self.get_customer_sales_area(bp_id)
                if sa:
                    final_data.update(sa)
                else:
                    final_data.update({"SalesOrganization": "N/A", "DistributionChannel": "N/A", "Division": "N/A"})

                return final_data
            return None
        except requests.RequestException as e:
            logger.error("Customer details fetch error for '%s': %s", bp_id, e)
            return None

    def create_business_partner_customer(self, bp_data, address_data, contact_data):
        try:
            if not self._get_csrf_token(BUSINESS_PARTNER_API): return {"success": False, "error": "CSRF Token Failed"}
            payload = {"BusinessPartnerCategory": bp_data['category'], "BusinessPartnerGrouping": BP_GROUPING,
                       "CorrespondenceLanguage": "EN", **bp_data['name_fields'],
                       "to_BusinessPartnerRole": {"results": [{"BusinessPartnerRole": "FLCU01"}]}}
            address_payload = {"Country": address_data['country'], "Region": address_data['region'],
                               "CityName": address_data['city'], "Language": "EN"}
            if address_data.get('street') != 'SKIP': address_payload["StreetName"] = address_data['street']
            if address_data.get('postal_code') != 'SKIP': address_payload["PostalCode"] = address_data['postal_code']
            if contact_data.get('mobile') != 'SKIP': address_payload["to_MobilePhoneNumber"] = {
                "results": [{"PhoneNumber": contact_data['mobile'], "IsDefaultPhoneNumber": True}]}
            if contact_data.get('email') != 'SKIP': address_payload["to_EmailAddress"] = {
                "results": [{"EmailAddress": contact_data['email'], "IsDefaultEmailAddress": True}]}
            payload["to_BusinessPartnerAddress"] = {"results": [address_payload]}
            clean_url = BUSINESS_PARTNER_API.split('?')[0].rstrip('/')
            res = self.session.post(f"{clean_url}/A_BusinessPartner", json=payload, cookies=self.cookies, timeout=45)
            if res.status_code in [200, 201]:
                d = res.json().get('d', {})
                name = d.get('OrganizationBPName1') or f"{d.get('FirstName', '')} {d.get('LastName', '')}".strip()
                if not name: name = "New Customer"
                logger.info("Business partner created: %s", d.get('BusinessPartner'))
                return {"success": True, "bp_id": d.get('BusinessPartner'), "name": name}
            else:
                try:
                    err = res.json()['error']['message']['value']
                except (KeyError, ValueError):
                    err = str(res.status_code)
                logger.warning("BP creation failed: %s", err)
                return {"success": False, "error": err}
        except Exception as e:
            logger.error("BP creation exception: %s", e, exc_info=True)
            return {"success": False, "error": str(e)}

    def search_products(self, keyword):
        try:
            safe_keyword = keyword.strip().replace("'", "''")
            cache_key = f"prod_search_{safe_keyword.lower()}"
            cached = self._cache_products.get(cache_key)
            if cached is not None:
                return cached
            results = []
            filter_str = f"substringof('{safe_keyword}', ProductDescription) or Product eq '{safe_keyword}'"
            url = f"{PRODUCT_API}/A_ProductDescription?$filter={filter_str} and Language eq 'EN'&$top=50&$format=json"
            res = self.session.get(url, timeout=20)
            if res.status_code == 200:
                items = res.json().get('d', {}).get('results', [])
                for i in items: results.append({"id": i['Product'], "name": i['ProductDescription']})
            self._cache_products.set(cache_key, results)
            return results
        except requests.RequestException as e:
            logger.error("Product search error for '%s': %s", keyword, e)
            return []

    def get_product_price(self, material_id, customer_id=None):
        try:
            padded_cust = customer_id.zfill(10) if customer_id else None
            cache_key = f"price_{material_id}_{padded_cust or 'generic'}"
            cached = self._cache_price.get(cache_key)
            if cached is not None:
                return cached

            url = f"{PRICE_API}/A_SlsPrcgCndnRecdValidity"
            exp = "&$expand=to_SlsPrcgConditionRecord&$format=json&$top=1"

            # Customer-specific condition types to try (in priority order)
            cust_condition_types = ['PR00', 'PPR0', 'ZPR0', 'ZPRC']
            result = None

            # Step 1: Try customer-specific price if customer_id provided
            if padded_cust:
                for ct in cust_condition_types:
                    try:
                        cust_filter = (
                            f"?$filter=Material eq '{material_id}'"
                            f" and ConditionType eq '{ct}'"
                            f" and Customer eq '{padded_cust}'"
                        )
                        res = self.session.get(f"{url}{cust_filter}{exp}", timeout=20)
                        if res.status_code == 200:
                            items = res.json().get('d', {}).get('results', [])
                            if items:
                                rec = items[0].get('to_SlsPrcgConditionRecord', {})
                                rate = rec.get('ConditionRateValue')
                                unit = rec.get('ConditionRateValueUnit', 'EUR')
                                if rate and float(rate) != 0:
                                    result = f"{rate} {unit}"
                                    logger.info("Customer-specific price found: mat=%s, cust=%s, ct=%s, price=%s",
                                                material_id, padded_cust, ct, result)
                                    break
                    except (requests.RequestException, ValueError, KeyError) as e:
                        logger.debug("Price lookup failed for ct=%s, cust=%s: %s", ct, padded_cust, e)
                        continue

            # Step 2: Fallback to standard/generic price (PPR0 without customer)
            if not result:
                try:
                    std_filter = f"?$filter=Material eq '{material_id}' and ConditionType eq 'PPR0'"
                    res = self.session.get(f"{url}{std_filter}{exp}", timeout=20)
                    if res.status_code == 200:
                        items = res.json().get('d', {}).get('results', [])
                        if items:
                            rec = items[0].get('to_SlsPrcgConditionRecord', {})
                            rate = rec.get('ConditionRateValue')
                            unit = rec.get('ConditionRateValueUnit', 'EUR')
                            if rate:
                                result = f"{rate} {unit}"
                except requests.RequestException as e:
                    logger.debug("Standard price fallback failed for mat=%s: %s", material_id, e)

            if not result:
                result = "0.00 EUR"

            self._cache_price.set(cache_key, result)
            return result
        except Exception as e:
            logger.error("Price fetch error for material '%s': %s", material_id, e)
            return "0.00 EUR"

    def get_stock_overview(self, material_id):
        try:
            cache_key = f"stock_{material_id}"
            cached = self._cache_stock.get(cache_key)
            if cached is not None:
                return cached
            params = f"?$filter=Material eq '{material_id}' and InventoryStockType eq '01'&$format=json"
            url = f"{STOCK_API}/A_MatlStkInAcctMod{params}"
            res = self.session.get(url, timeout=20)
            if res.status_code == 200:
                results = res.json().get('d', {}).get('results', [])
                total_qty = 0.0
                base_unit = "PC"
                hierarchy = {}
                for item in results:
                    qty = float(item.get('MatlWrhsStkQtyInMatlBaseUnit', 0))
                    plant = item.get('Plant')
                    loc = item.get('StorageLocation') or "General"
                    base_unit = item.get('MaterialBaseUnit', base_unit)
                    if qty > 0:
                        total_qty += qty
                        if plant not in hierarchy: hierarchy[plant] = {}
                        if loc in hierarchy[plant]:
                            hierarchy[plant][loc] += qty
                        else:
                            hierarchy[plant][loc] = qty
                result = {"total": total_qty, "unit": base_unit, "breakdown": hierarchy}
                self._cache_stock.set(cache_key, result)
                return result
            return None
        except requests.RequestException as e:
            logger.error("Stock overview error for '%s': %s", material_id, e)
            return None

    def create_sales_quotation(self, customer_id, items_list, customer_ref, valid_to_date=None):
        try:
            if not self._get_csrf_token(QUOTE_API): return {"success": False, "error": "CSRF Token Failed"}
            sa = self.get_customer_sales_area(customer_id)
            org = sa['SalesOrganization'] if sa else "1100"
            chn = sa['DistributionChannel'] if sa else "DO"

            today = datetime.now()
            date_from = today.strftime("%Y-%m-%dT00:00:00")

            if valid_to_date:
                date_to = f"{valid_to_date}T00:00:00"
            else:
                valid_to = today + timedelta(days=30)
                date_to = valid_to.strftime("%Y-%m-%dT00:00:00")

            sap_items = []
            for item in items_list:
                sap_items.append({"Material": item['Material'], "RequestedQuantity": str(item['Quantity'])})

            payload = {
                "SalesQuotationType": "QT",
                "SalesOrganization": org, "DistributionChannel": chn, "OrganizationDivision": "99",
                "SoldToParty": customer_id, "PurchaseOrderByCustomer": customer_ref,
                "BindingPeriodValidityStartDate": date_from, "BindingPeriodValidityEndDate": date_to,
                "to_Item": {"results": sap_items}
            }
            clean_url = QUOTE_API.split('?')[0].rstrip('/')
            res = self.session.post(f"{clean_url}/A_SalesQuotation", json=payload, cookies=self.cookies, timeout=60)
            if res.status_code in [200, 201]:
                d = res.json().get('d', {})
                logger.info("Sales quotation created: %s for customer %s", d.get('SalesQuotation'), customer_id)
                return {"success": True, "id": d.get('SalesQuotation'), "net": d.get('TotalNetAmount'),
                        "curr": d.get('TransactionCurrency')}
            else:
                try:
                    msg = res.json()['error']['message']['value']
                except (KeyError, ValueError):
                    msg = res.text
                logger.warning("Quotation creation failed: %s", msg)
                return {"success": False, "error": msg}
        except Exception as e:
            logger.error("Quotation creation exception: %s", e, exc_info=True)
            return {"success": False, "error": str(e)}

    def create_sales_order(self, customer_id, items_list, customer_ref, discount_pct=0.0, ref_doc=None):
        try:
            if not self._get_csrf_token(ORDER_API): return {"success": False, "error": "CSRF Token Failed"}
            sa = self.get_customer_sales_area(customer_id)
            org = sa['SalesOrganization'] if sa else "1100"
            chn = sa['DistributionChannel'] if sa else "DO"

            sap_items = []
            for item in items_list:
                line = {"Material": item['Material'], "RequestedQuantity": str(item['Quantity'])}
                if ref_doc:
                    line["ReferenceSDDocument"] = ref_doc
                    line["ReferenceSDDocumentItem"] = item.get('Ref_Item', '10')
                if discount_pct > 0:
                    line["to_PricingElement"] = {"results": [
                        {"ConditionType": DISCOUNT_CONDITION_TYPE, "ConditionRateValue": f"-{discount_pct}"}]}
                sap_items.append(line)

            payload = {
                "SalesOrderType": "OR", "SalesOrganization": org, "DistributionChannel": chn,
                "OrganizationDivision": "99",
                "SoldToParty": customer_id, "PurchaseOrderByCustomer": customer_ref,
                "to_Item": {"results": sap_items}
            }
            clean_url = ORDER_API.split('?')[0].rstrip('/')
            res = self.session.post(f"{clean_url}/A_SalesOrder", json=payload, cookies=self.cookies, timeout=60)
            if res.status_code in [200, 201]:
                d = res.json().get('d', {})
                logger.info("Sales order created: %s for customer %s", d.get('SalesOrder'), customer_id)
                return {"success": True, "id": d.get('SalesOrder'), "net": d.get('TotalNetAmount'),
                        "curr": d.get('TransactionCurrency')}
            else:
                try:
                    msg = res.json()['error']['message']['value']
                except (KeyError, ValueError):
                    msg = res.text
                logger.warning("Order creation failed: %s", msg)
                return {"success": False, "error": msg}
        except Exception as e:
            logger.error("Order creation exception: %s", e, exc_info=True)
            return {"success": False, "error": str(e)}

    def get_quotation_details(self, quote_id):
        try:
            url = f"{QUOTE_API}/A_SalesQuotation('{quote_id}')?$expand=to_Item&$format=json"
            res = self.session.get(url, timeout=30)
            if res.status_code == 200:
                d = res.json().get('d', {})
                items = []
                for i in d.get('to_Item', {}).get('results', []):
                    items.append({
                        "material": i.get('Material'),
                        "desc": i.get('SalesQuotationItemText'),
                        "item_no": i.get('SalesQuotationItem'),
                        "qty": f"{i.get('RequestedQuantity')} {i.get('RequestedQuantityUnit')}",
                        "net": f"{i.get('NetAmount')} {i.get('TransactionCurrency')}"
                    })
                fmt_date = self._parse_sap_date(d.get('CreationDate'))
                return {"id": d.get('SalesQuotation'), "customer": d.get('SoldToParty'),
                        "ref": d.get('PurchaseOrderByCustomer', 'N/A'), "date": fmt_date,
                        "total": f"{d.get('TotalNetAmount')} {d.get('TransactionCurrency')}",
                        "status": d.get('OverallSDProcessStatus'), "org": d.get('SalesOrganization'),
                        "channel": d.get('DistributionChannel'), "items": items}
            return None
        except requests.RequestException as e:
            logger.error("Quotation details fetch error for '%s': %s", quote_id, e)
            return None

    def get_sales_order_details(self, order_id):
        try:
            url = f"{ORDER_API}/A_SalesOrder('{order_id}')?$expand=to_Item&$format=json"
            res = self.session.get(url, timeout=30)
            if res.status_code == 200:
                d = res.json().get('d', {})
                items = []
                for i in d.get('to_Item', {}).get('results', []):
                    items.append({"material": i.get('Material'), "desc": i.get('SalesOrderItemText'),
                                  "qty": f"{i.get('RequestedQuantity')} {i.get('RequestedQuantityUnit')}",
                                  "net": f"{i.get('NetAmount')} {i.get('TransactionCurrency')}"})
                fmt_date = self._parse_sap_date(d.get('CreationDate'))
                quote_id = d.get('ReferenceSDDocument', 'N/A')
                invoice_id = "Pending"
                try:
                    inv_url = f"{BILLING_API}/A_BillingDocumentItem?$filter=ReferenceSDDocument eq '{order_id}'&$select=BillingDocument&$top=1&$format=json"
                    inv_res = self.session.get(inv_url, timeout=30)
                    if inv_res.status_code == 200:
                        inv_data = inv_res.json().get('d', {}).get('results', [])
                        if inv_data: invoice_id = inv_data[0]['BillingDocument']
                except requests.RequestException as e:
                    logger.debug("Invoice lookup failed for order '%s': %s", order_id, e)
                return {"id": d.get('SalesOrder'), "customer": d.get('SoldToParty'),
                        "ref": d.get('PurchaseOrderByCustomer', 'N/A'), "date": fmt_date,
                        "total": f"{d.get('TotalNetAmount')} {d.get('TransactionCurrency')}",
                        "status": d.get('OverallSDProcessStatus'), "org": d.get('SalesOrganization'),
                        "channel": d.get('DistributionChannel'), "quote_link": quote_id, "invoice_link": invoice_id,
                        "items": items}
            return None
        except requests.RequestException as e:
            logger.error("Order details fetch error for '%s': %s", order_id, e)
            return None

    def get_sales_orders(self, skip=0, top=5):
        try:
            url = f"{ORDER_API}/A_SalesOrder?$skip={skip}&$top={top}&$format=json&$select=SalesOrder,SoldToParty,TotalNetAmount,TransactionCurrency,OverallSDProcessStatus,CreationDate&$orderby=SalesOrder desc"
            res = self.session.get(url, timeout=30)
            return res.json().get('d', {}).get('results', []) if res.status_code == 200 else []
        except requests.RequestException as e:
            logger.error("Get sales orders error: %s", e)
            return []

    def get_sales_orders_for_customer(self, customer_id, skip=0, top=5):
        """Fetch sales orders filtered to a specific customer (B2B isolation)."""
        try:
            url = (f"{ORDER_API}/A_SalesOrder?$filter=SoldToParty eq '{customer_id}'"
                   f"&$skip={skip}&$top={top}&$format=json"
                   f"&$select=SalesOrder,SoldToParty,TotalNetAmount,TransactionCurrency,OverallSDProcessStatus,CreationDate"
                   f"&$orderby=SalesOrder desc")
            res = self.session.get(url, timeout=30)
            return res.json().get('d', {}).get('results', []) if res.status_code == 200 else []
        except requests.RequestException as e:
            logger.error("Get sales orders for customer '%s' error: %s", customer_id, e)
            return []

    def get_quotations(self, skip=0, top=5):
        try:
            url = f"{QUOTE_API}/A_SalesQuotation?$skip={skip}&$top={top}&$format=json&$select=SalesQuotation,TotalNetAmount,TransactionCurrency,OverallSDProcessStatus,CreationDate&$orderby=SalesQuotation desc"
            res = self.session.get(url, timeout=30)
            return res.json().get('d', {}).get('results', []) if res.status_code == 200 else []
        except requests.RequestException as e:
            logger.error("Get quotations error: %s", e)
            return []

    def get_quotations_for_customer(self, customer_id, skip=0, top=5):
        """Fetch quotations filtered to a specific customer (B2B isolation)."""
        try:
            url = (f"{QUOTE_API}/A_SalesQuotation?$filter=SoldToParty eq '{customer_id}'"
                   f"&$skip={skip}&$top={top}&$format=json"
                   f"&$select=SalesQuotation,SoldToParty,TotalNetAmount,TransactionCurrency,OverallSDProcessStatus,CreationDate"
                   f"&$orderby=SalesQuotation desc")
            res = self.session.get(url, timeout=30)
            return res.json().get('d', {}).get('results', []) if res.status_code == 200 else []
        except requests.RequestException as e:
            logger.error("Get quotations for customer '%s' error: %s", customer_id, e)
            return []

    def get_customer_invoices(self, customer_id, skip=0, top=5, status_filter=None):
        """Fetch billing documents (invoices) filtered to a specific customer (B2B isolation).

        status_filter:
          'pending'   -> add OverallBillingStatus eq 'A' (open / not yet completed)
          'completed' -> add InvoiceClearingStatus eq 'C' (cleared / paid)
          None        -> no extra filter
        """
        try:
            filt = f"SoldToParty eq '{customer_id}'"
            if status_filter == 'pending':
                filt += " and OverallBillingStatus eq 'A'"
            elif status_filter == 'completed':
                filt += " and InvoiceClearingStatus eq 'C'"
            url = (f"{BILLING_API}/A_BillingDocument?$filter={filt}"
                   f"&$skip={skip}&$top={top}&$format=json"
                   f"&$select=BillingDocument,BillingDocumentDate,TotalNetAmount,TotalGrossAmount,"
                   f"TransactionCurrency,OverallBillingStatus,InvoiceClearingStatus,SoldToParty"
                   f"&$orderby=BillingDocumentDate desc")
            res = self.session.get(url, timeout=30)
            if res.status_code != 200:
                return []
            results = res.json().get('d', {}).get('results', [])
            for inv in results:
                ts = self._get_sap_timestamp(inv.get('BillingDocumentDate'))
                inv['BillingDocumentDate_fmt'] = (
                    datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d') if ts else "N/A"
                )
            return results
        except requests.RequestException as e:
            logger.error("Get invoices for customer '%s' error: %s", customer_id, e)
            return []

    def get_invoice_details(self, invoice_no):
        """Fetch a single billing document with detail-view fields. Returns dict or None."""
        try:
            url = (f"{BILLING_API}/A_BillingDocument?$filter=BillingDocument eq '{invoice_no}'"
                   f"&$top=1&$format=json"
                   f"&$select=BillingDocument,BillingDocumentDate,BillingDocumentType,"
                   f"TotalNetAmount,TaxAmount,TotalGrossAmount,TransactionCurrency,"
                   f"CustomerPaymentTerms,SoldToParty,"
                   f"OverallBillingStatus,InvoiceClearingStatus")
            res = self.session.get(url, timeout=30)
            if res.status_code != 200:
                return None
            results = res.json().get('d', {}).get('results', [])
            if not results:
                return None
            inv = results[0]
            ts = self._get_sap_timestamp(inv.get('BillingDocumentDate'))
            inv['BillingDocumentDate_fmt'] = (
                datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d') if ts else "N/A"
            )
            return inv
        except requests.RequestException as e:
            logger.error("Get invoice details for '%s' error: %s", invoice_no, e)
            return None

    def _fetch_orders_analytics(self, sap_start, sap_end):
        """Fetch orders for analytics (called in parallel)."""
        result = {"total": 0, "val": 0.0, "status": {}, "list": []}
        try:
            url = f"{ORDER_API}/A_SalesOrder?$filter=CreationDate ge datetime'{sap_start}' and CreationDate le datetime'{sap_end}'&$format=json&$select=SalesOrder,TotalNetAmount,TransactionCurrency,OverallSDProcessStatus,CreationDate"
            res = self.session.get(url, timeout=30)
            if res.status_code == 200:
                items = res.json().get('d', {}).get('results', [])
                result["list"] = items
                result["total"] = len(items)
                for i in items:
                    result["val"] += float(i.get("TotalNetAmount", 0))
                    st = i.get("OverallSDProcessStatus")
                    label = "Completed" if st == 'C' else "Open" if st == 'A' else "Processing"
                    result["status"][label] = result["status"].get(label, 0) + 1
        except requests.RequestException as e:
            logger.error("Analytics orders fetch error: %s", e)
        return result

    def _fetch_quotes_analytics(self, sap_start, sap_end):
        """Fetch quotes for analytics (called in parallel)."""
        result = {"total": 0, "val": 0.0, "status": {}, "list": []}
        try:
            url = f"{QUOTE_API}/A_SalesQuotation?$filter=CreationDate ge datetime'{sap_start}' and CreationDate le datetime'{sap_end}'&$format=json&$select=SalesQuotation,TotalNetAmount,TransactionCurrency,OverallSDProcessStatus,CreationDate"
            res = self.session.get(url, timeout=30)
            if res.status_code == 200:
                items = res.json().get('d', {}).get('results', [])
                result["list"] = items
                result["total"] = len(items)
                for i in items:
                    result["val"] += float(i.get("TotalNetAmount", 0))
                    st = i.get("OverallSDProcessStatus")
                    label = "Completed" if st == 'C' else "Open" if st == 'A' else "Processing"
                    result["status"][label] = result["status"].get(label, 0) + 1
        except requests.RequestException as e:
            logger.error("Analytics quotes fetch error: %s", e)
        return result

    def _fetch_invoices_analytics(self, sap_start, sap_end):
        """Fetch invoices for analytics (called in parallel)."""
        result = {"total": 0, "val": 0.0, "list": []}
        try:
            url = f"{BILLING_API}/A_BillingDocument?$filter=BillingDocumentDate ge datetime'{sap_start}' and BillingDocumentDate le datetime'{sap_end}'&$format=json&$select=BillingDocument,TotalNetAmount,TransactionCurrency,BillingDocumentDate"
            res = self.session.get(url, timeout=30)
            if res.status_code == 200:
                items = res.json().get('d', {}).get('results', [])
                result["list"] = items
                result["total"] = len(items)
                for i in items: result["val"] += float(i.get("TotalNetAmount", 0))
        except requests.RequestException as e:
            logger.error("Analytics invoices fetch error: %s", e)
        return result

    def get_analytics_by_date(self, start_date_str, end_date_str):
        try:
            d1 = datetime.strptime(start_date_str, "%d-%m-%Y")
            d2 = datetime.strptime(end_date_str, "%d-%m-%Y")
            sap_start = d1.strftime("%Y-%m-%dT00:00:00")
            sap_end = d2.strftime("%Y-%m-%dT23:59:59")

            # Run all 3 SAP API calls in parallel
            fut_orders = self._executor.submit(self._fetch_orders_analytics, sap_start, sap_end)
            fut_quotes = self._executor.submit(self._fetch_quotes_analytics, sap_start, sap_end)
            fut_invoices = self._executor.submit(self._fetch_invoices_analytics, sap_start, sap_end)

            stats = {
                "orders": fut_orders.result(timeout=45),
                "quotes": fut_quotes.result(timeout=45),
                "invoices": fut_invoices.result(timeout=45),
            }
            return stats
        except Exception as e:
            logger.error("Analytics fetch error for period %s to %s: %s", start_date_str, end_date_str, e, exc_info=True)
            return None

    def get_credit_exposure(self, customer_id):
        try:
            today_date = datetime.now().strftime('%Y-%m-%dT00:00:00')
            endpoint = f"/YY1_TotalExposure(P_ExchangeRateType='M',P_DisplayCurrency='USD',P_KeyDate=datetime'{today_date}',P_ValuationThresholdPercent=100,P_ReadLineItem='Y')/Results"
            filter_query = f"?$filter=BusinessPartner eq '{customer_id}'&$format=json"
            full_url = f"{CREDIT_API}{endpoint}{filter_query}"
            res = self.session.get(full_url, timeout=30)
            if res.status_code == 200:
                results = res.json().get('d', {}).get('results', [])
                if results:
                    data = results[0]
                    return {"exposure": float(data.get('CustomerCreditExposureAmount', 0)),
                            "limit": float(data.get('CustomerCreditLimitAmount', 0)),
                            "remaining": float(data.get('RemainingCreditAmtInDspCrcy', 0)),
                            "currency": data.get('DisplayCurrency', 'USD'),
                            "percent": float(data.get('CreditLimitUtilizationPercent', 0)),
                            "risk_class": data.get('CreditRiskClass', 'N/A')}
            return None
        except requests.RequestException as e:
            logger.error("Credit exposure fetch error for '%s': %s", customer_id, e)
            return None

    def get_top_materials(self):
        try:
            url = f"{PRODUCT_API}/A_ProductDescription?$top=5&$format=json&$select=Product,ProductDescription&$filter=Language eq 'EN'"
            res = self.session.get(url, timeout=30)
            results = []
            if res.status_code == 200:
                data = res.json().get('d', {}).get('results', [])
                for i in data: results.append({"id": i['Product'], "name": i['ProductDescription']})
            return results
        except requests.RequestException as e:
            logger.error("Top materials fetch error: %s", e)
            return []

    def get_companies_dynamic(self):
        try:
            cached = self._cache_companies.get("all_companies")
            if cached is not None:
                return cached
            res = self.session.get(
                f"{COMPANY_API}/A_CompanyCode?$select=CompanyCode,CompanyCodeName,Currency,Country&$format=json",
                timeout=20)
            if res.status_code == 200:
                result = [
                    {"id": i['CompanyCode'], "name": i['CompanyCodeName'], "curr": i.get('Currency', 'USD'),
                     "country": i.get('Country', 'US')} for i in res.json()['d']['results']]
                self._cache_companies.set("all_companies", result)
                return result
            return []
        except requests.RequestException as e:
            logger.error("Company codes fetch error: %s", e)
            return []

    def search_vendors_dynamic(self, keyword, company_code):
        try:
            term = keyword.strip().replace("'", "''").lower()
            url = f"{BUSINESS_PARTNER_API}/A_SupplierCompany?$filter=CompanyCode eq '{company_code}'&$expand=to_Supplier&$select=Supplier,CompanyCode,to_Supplier/SupplierName&$top=100&$format=json"
            res = self.session.get(url, timeout=20)
            final_list = []
            if res.status_code == 200:
                for i in res.json()['d']['results']:
                    supp_id = i.get('Supplier', '')
                    supp_name = i.get('to_Supplier', {}).get('SupplierName', '') or "Unknown"
                    if not term or (term in supp_id.lower() or term in supp_name.lower()):
                        final_list.append({"id": supp_id, "name": supp_name})
                        if len(final_list) >= 15: break
            return final_list
        except requests.RequestException as e:
            logger.error("Vendor search error in company '%s': %s", company_code, e)
            return []

    def validate_vendor_in_company(self, vendor_id, company_code):
        try:
            url = f"{BUSINESS_PARTNER_API}/A_SupplierCompany(Supplier='{vendor_id}',CompanyCode='{company_code}')?$format=json"
            res = self.session.get(url, timeout=10)
            return {"valid": res.status_code == 200}
        except requests.RequestException:
            return {"valid": False}

    def search_gl_accounts_dynamic(self, keyword, company_code=None):
        try:
            term = keyword.strip().replace("'", "''").lower()
            url = f"{GL_ACCOUNT_API}/A_GLAccountText?$filter=ChartOfAccounts eq 'YCOA' and Language eq 'EN'&$select=GLAccount,GLAccountLongName&$top=1000&$format=json"
            res = self.session.get(url, timeout=30)
            final_list = []
            if res.status_code == 200:
                for i in res.json()['d']['results']:
                    gl_id = i.get('GLAccount', '')
                    gl_name = i.get('GLAccountLongName', '')
                    if not term or (term in gl_id.lower() or term in gl_name.lower()):
                        final_list.append({"id": gl_id, "name": gl_name})
                        if len(final_list) >= 30: break
            return final_list
        except requests.RequestException as e:
            logger.error("GL account search error: %s", e)
            return []

    def search_cost_centers_dynamic(self, keyword, company_code):
        try:
            term = keyword.strip().replace("'", "''").lower()
            url = f"{COSTCENTER_API}/A_CostCenter?$filter=CompanyCode eq '{company_code}'&$expand=to_Text&$top=5000&$format=json"
            res = self.session.get(url, timeout=30)
            final_list = []
            if res.status_code == 200:
                for i in res.json()['d']['results']:
                    cc_id = i.get('CostCenter', '')
                    cc_name = i.get('CostCenterName')
                    if not cc_name:
                        text_data = i.get('to_Text', {}).get('results', [])
                        if text_data:
                            found = next((t for t in text_data if t.get('Language') == 'EN'), text_data[0])
                            cc_name = found.get('CostCenterName') or found.get('Description')
                    if not cc_name: cc_name = "Cost Center"
                    if not term or (term in cc_id.lower() or term in cc_name.lower()):
                        final_list.append({"id": cc_id, "name": cc_name})
                        if len(final_list) >= 15: break
            return final_list
        except requests.RequestException as e:
            logger.error("Cost center search error in company '%s': %s", company_code, e)
            return []

    def search_tax_codes_dynamic(self, keyword, company_code):
        try:
            term = keyword.strip().replace("'", "''").lower()
            res_co = self.session.get(f"{COMPANY_API}/A_CompanyCode('{company_code}')?$select=Country&$format=json", timeout=10)
            country = res_co.json()['d']['Country'] if res_co.status_code == 200 else 'US'
            base_url = f"{SAP_BASE_URL}/sap/opu/odata/sap/YY1_ZTAXFINALAPI_CDS/YY1_ZTaxFinalAPI"
            url = f"{base_url}?$filter=TaxCalculationProcedure eq '0TXUSX'&$format=json" if company_code == '2000' else f"{base_url}?$filter=Country eq '{country}'&$format=json"
            res = self.session.get(url, timeout=20)
            unique_taxes = {}
            high_date_threshold = 250000000000000
            if res.status_code == 200:
                for i in res.json()['d']['results']:
                    code = i['TaxCode']
                    end_date_str = i.get('CndnRecordValidityEndDate')
                    ts = self._get_sap_timestamp(end_date_str)
                    is_valid = False
                    if company_code == '2000':
                        if ts is None: is_valid = True
                    else:
                        if ts and ts > high_date_threshold: is_valid = True
                    if is_valid and code not in unique_taxes:
                        name = i.get('TaxCodeName') or f"Tax {code}"
                        if not term or (term in code.lower() or term in name.lower()):
                            unique_taxes[code] = {'id': code, 'name': name}
            return sorted(list(unique_taxes.values()), key=lambda x: x['id'])
        except Exception as e:
            logger.error("Tax code search error for company '%s': %s", company_code, e)
            return []

    def _get_jurisdiction_code_from_cc(self, cost_center):
        try:
            res = self.session.get(
                f"{COSTCENTER_API}/A_CostCenter?$filter=CostCenter eq '{cost_center}'&$select=TaxJurisdiction&$format=json&top=1",
                timeout=10)
            if res.status_code == 200 and res.json()['d']['results']: return res.json()['d']['results'][0].get(
                'TaxJurisdiction')
            return None
        except requests.RequestException:
            return None

    def create_supplier_invoice_dynamic(self, data):
        try:
            import time
            if not self._get_csrf_token(SUPPLIER_INVOICE_API): return {"success": False, "error": "CSRF Token Failed"}
            doc_date = data.get('doc_date') or datetime.now().strftime("%Y-%m-%dT00:00:00")
            post_date = data.get('post_date') or datetime.now().strftime("%Y-%m-%dT00:00:00")
            ref_id = data.get('ref_id') or f"BOT-{int(time.time())}"
            gl_items = []
            total_amount = 0.0
            for index, item in enumerate(data['items']):
                amount = float(item['amount'])
                total_amount += amount
                cc_jur = self._get_jurisdiction_code_from_cc(item['cc'])
                if data['company'] == '2000' and not cc_jur: cc_jur = "7700000000"
                line = {"SupplierInvoiceItem": str(index + 1).zfill(4), "CompanyCode": data['company'],
                        "GLAccount": item['gl'], "CostCenter": item['cc'], "DocumentCurrency": data['currency'],
                        "SupplierInvoiceItemAmount": str(amount), "TaxCode": item['tax'], "DebitCreditCode": "S",
                        "SupplierInvoiceItemText": item['desc']}
                if cc_jur: line["TaxJurisdiction"] = cc_jur
                gl_items.append(line)
            payload = {"CompanyCode": data['company'], "DocumentDate": doc_date, "PostingDate": post_date,
                       "DueCalculationBaseDate": doc_date, "InvoicingParty": data['vendor'],
                       "DocumentCurrency": data['currency'], "InvoiceGrossAmount": str(total_amount),
                       "DocumentHeaderText": f"ExpClaim: {len(gl_items)} Items",
                       "SupplierInvoiceIDByInvcgParty": ref_id, "TaxIsCalculatedAutomatically": False,
                       "to_SupplierInvoiceItemGLAcct": gl_items}
            if data['company'] == '1000': payload["TaxDeterminationDate"] = doc_date
            clean_url = SUPPLIER_INVOICE_API.split('?')[0].rstrip('/')
            res = self.session.post(f"{clean_url}/A_SupplierInvoice", json=payload, cookies=self.cookies, timeout=60)
            if res.status_code == 201:
                logger.info("Supplier invoice created: %s", res.json()['d']['SupplierInvoice'])
                return {"success": True, "id": res.json()['d']['SupplierInvoice'],
                                               "year": res.json()['d']['FiscalYear']}
            try:
                err = res.json()['error']['message']['value']
            except (KeyError, ValueError):
                err = res.text[:150]
            logger.warning("Supplier invoice creation failed: %s", err)
            return {"success": False, "error": err}
        except Exception as e:
            logger.error("Supplier invoice creation exception: %s", e, exc_info=True)
            return {"success": False, "error": str(e)}

    def upload_attachment(self, invoice_id, fiscal_year, file_path):
        try:
            if not os.path.exists(file_path): return {"success": False, "error": "File missing"}
            obj_key = f"{invoice_id}{fiscal_year}"
            file_name = os.path.basename(file_path)
            mime_type = "image/png"
            if file_path.lower().endswith(".jpg") or file_path.lower().endswith(".jpeg"):
                mime_type = "image/jpeg"
            elif file_path.lower().endswith(".pdf"):
                mime_type = "application/pdf"
            service_url = f"{self.base_url}/sap/opu/odata/sap/API_CV_ATTACHMENT_SRV"
            if 'x-csrf-token' in self.session.headers: del self.session.headers['x-csrf-token']
            token_headers = {'x-csrf-token': 'fetch'}
            response_token = self.session.get(service_url, headers=token_headers, timeout=30)
            csrf_token = response_token.headers.get('x-csrf-token')
            if not csrf_token: return {"success": False, "error": "CSRF Token missing"}
            upload_url = f"{service_url}/AttachmentContentSet"
            headers = {'x-csrf-token': csrf_token, 'Content-Type': mime_type, 'Slug': file_name,
                       'BusinessObjectTypeName': 'BUS2081', 'LinkedSAPObjectKey': obj_key,
                       'HarmonizedDocumentType': 'SAT'}
            with open(file_path, 'rb') as f:
                file_data = f.read()
            response = requests.post(upload_url, headers=headers, data=file_data, cookies=self.session.cookies,
                                     auth=self.session.auth, timeout=60)
            if response.status_code in [200, 201]:
                logger.info("Attachment uploaded for invoice %s", invoice_id)
                return {"success": True}
            else:
                logger.warning("Attachment upload failed: HTTP %s", response.status_code)
                return {"success": False, "error": f"SAP Error {response.status_code}"}
        except Exception as e:
            logger.error("Attachment upload exception: %s", e, exc_info=True)
            return {"success": False, "error": str(e)}

    def fetch_next_queue_item(self, qname):
        try:
            url = f"{self.base_url}/sap/opu/odata/sap/API_CLOUD_PRINT_PULL_SRV/RetrieveNextQueueItem?Qname='{qname}'&Language='en'&$format=json"
            res = self.session.get(url, timeout=10)
            if res.status_code == 200: return res.json().get('d', {}).get('item_id')
            return None
        except requests.RequestException:
            return None

    def fetch_queue_document(self, item_id, qname):
        try:
            url = f"{self.base_url}/sap/opu/odata/sap/API_CLOUD_PRINT_PULL_SRV/Get_QItem_Document?item_id='{item_id}'&qname='{qname}'&main_doc=true&attachments=false&$format=json"
            res = self.session.get(url, timeout=30)
            if res.status_code == 200:
                results = res.json().get('d', {}).get('results', [])
                if results: return results[0]
            return None
        except requests.RequestException:
            return None

    # Journal Entry via SOAP - XML payload preserved exactly as tested
    def create_journal_entry_without_vendor(self, data):
        try:
            url = JOURNAL_ENTRY_API
            company = data.get('company', '1000')
            currency = data.get('currency', 'USD')
            doc_date = data.get('doc_date', '').split('T')[0]
            post_date = data.get('post_date', '').split('T')[0]

            item = data['items'][0]
            gl_expense = item['gl']
            cost_center = item['cc']
            tax_code = item['tax']
            desc = item['desc']
            gross_amount = float(item['amount'])

            gl_petty_cash = "10010000"

            # Master Data Mapping based on R&D
            if company == '1000':
                tax_rate = 0.19 if tax_code == 'V1' else 0.0
                profit_center = "100010"
                segment = "1000_B"
                xml_tax_determination = f"<TaxDeterminationDate>{doc_date}</TaxDeterminationDate>"
                xml_tax_group = ""
                xml_tax_jur = ""
                xml_tax_class = "<TaxItemClassification>VST</TaxItemClassification>"
            else:  # Company 2000
                tax_rate = 0.0725 if tax_code == 'I1' else 0.0
                profit_center = "200010"
                segment = "2010"
                xml_tax_determination = ""
                xml_tax_group = "<TaxItemGroup>1</TaxItemGroup>"
                xml_tax_jur = "<TaxJurisdiction>CA00000000</TaxJurisdiction>"
                xml_tax_class = "<TaxItemClassification>NVV</TaxItemClassification>"

            net_amount = round(gross_amount / (1 + tax_rate), 2)
            tax_amount = round(gross_amount - net_amount, 2)

            xml_payload = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:sfin="http://sap.com/xi/SAPSCORE/SFIN">
               <soapenv:Header/>
               <soapenv:Body>
                  <sfin:JournalEntryBulkCreateRequest>
                     <MessageHeader>
                        <CreationDateTime>{datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")}</CreationDateTime>
                     </MessageHeader>
                     <JournalEntryCreateRequest>
                        <MessageHeader>
                           <CreationDateTime>{datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")}</CreationDateTime>
                        </MessageHeader>
                        <JournalEntry>
                           <OriginalReferenceDocumentType>BKPFF</OriginalReferenceDocumentType>
                           <BusinessTransactionType>RFBU</BusinessTransactionType>
                           <AccountingDocumentType>SJ</AccountingDocumentType>
                           <CreatedByUser>BUSER</CreatedByUser>
                           <CompanyCode>{company}</CompanyCode>
                           <DocumentDate>{doc_date}</DocumentDate>
                           <PostingDate>{post_date}</PostingDate>
                           {xml_tax_determination}

                           <Item>
                              <ReferenceDocumentItem>1</ReferenceDocumentItem>
                              <GLAccount>{gl_expense}</GLAccount>
                              <AmountInTransactionCurrency currencyCode="{currency}">{net_amount}</AmountInTransactionCurrency>
                              <DebitCreditCode>S</DebitCreditCode>
                              <DocumentItemText>{desc}</DocumentItemText>
                              <Tax>
                                 <TaxCode>{tax_code}</TaxCode>
                                 {xml_tax_jur}
                                 {xml_tax_group}
                              </Tax>
                              <AccountAssignment>
                                 <ProfitCenter>{profit_center}</ProfitCenter>
                                 <Segment>{segment}</Segment>
                                 <CostCenter>{cost_center}</CostCenter>
                              </AccountAssignment>
                           </Item>

                           <Item>
                              <ReferenceDocumentItem>2</ReferenceDocumentItem>
                              <GLAccount>{gl_petty_cash}</GLAccount>
                              <AmountInTransactionCurrency currencyCode="{currency}">-{gross_amount}</AmountInTransactionCurrency>
                              <DebitCreditCode>H</DebitCreditCode>
                              <DocumentItemText>Paid via Petty Cash</DocumentItemText>
                           </Item>

                           <ProductTaxItem>
                              <ReferenceDocumentItem>3</ReferenceDocumentItem>
                              <TaxCode>{tax_code}</TaxCode>
                              {xml_tax_class}
                              {xml_tax_jur}
                              {xml_tax_group}
                              <AmountInTransactionCurrency currencyCode="{currency}">{tax_amount}</AmountInTransactionCurrency>
                              <DebitCreditCode>S</DebitCreditCode>
                              <TaxBaseAmountInTransCrcy currencyCode="{currency}">{net_amount}</TaxBaseAmountInTransCrcy>
                           </ProductTaxItem>
                        </JournalEntry>
                     </JournalEntryCreateRequest>
                  </sfin:JournalEntryBulkCreateRequest>
               </soapenv:Body>
            </soapenv:Envelope>"""

            headers = {'Content-Type': 'text/xml'}
            res = self.session.post(url, data=xml_payload, headers=headers, timeout=60)

            if res.status_code == 200:
                import xml.etree.ElementTree as ET
                root = ET.fromstring(res.content)
                doc_num = "0000000000"
                for elem in root.iter():
                    if 'AccountingDocument' in elem.tag:
                        doc_num = elem.text
                        break
                if doc_num != "0000000000":
                    logger.info("Journal entry created: %s", doc_num)
                    return {"success": True, "id": doc_num, "year": post_date[:4]}
                else:
                    errors = [elem.text for elem in root.iter() if
                              'Note' in elem.tag and elem.text and "BKPFF" not in elem.text]
                    error_msg = " | ".join(errors) if errors else "SAP Validation Failed"
                    logger.warning("Journal entry validation failed: %s", error_msg)
                    return {"success": False, "error": error_msg}
            else:
                logger.warning("Journal entry HTTP error: %s", res.status_code)
                return {"success": False, "error": f"HTTP Error {res.status_code}"}
        except Exception as e:
            logger.error("Journal entry exception: %s", e, exc_info=True)
            return {"success": False, "error": str(e)}

    # ================================================================
    # INNOVATION v2.0 — NEW SAP QUERY METHODS
    # ================================================================

    def get_expiring_quotations(self, days_ahead=3):
        """Get quotations expiring within N days. Used by Quote Expiry Engine."""
        try:
            today = datetime.now()
            future = today + timedelta(days=days_ahead)
            today_str = today.strftime("%Y-%m-%dT00:00:00")
            future_str = future.strftime("%Y-%m-%dT23:59:59")

            url = (
                f"{QUOTE_API}/A_SalesQuotation?"
                f"$filter=OverallSDProcessStatus eq 'A' "
                f"and SalesQuotationDate le datetime'{future_str}'"
                f"&$select=SalesQuotation,SoldToParty,TotalNetAmount,"
                f"TransactionCurrency,SalesQuotationDate,BindingPeriodValidityEndDate"
                f"&$top=50&$orderby=SalesQuotationDate asc"
            )
            res = self.session.get(url, timeout=30)
            if res.status_code == 200:
                results = res.json().get('d', {}).get('results', [])
                expiring = []
                for qt in results:
                    end_ts = self._get_sap_timestamp(qt.get('BindingPeriodValidityEndDate'))
                    if end_ts:
                        end_dt = datetime.fromtimestamp(end_ts / 1000)
                        if today <= end_dt <= future:
                            qt['ValidToDate'] = end_dt.strftime('%d-%m-%Y')
                            expiring.append(qt)
                return expiring
            return []
        except Exception as e:
            logger.error("Expiring quotations fetch error: %s", e)
            return []

    def get_order_fulfillment_status(self, order_id):
        """Get delivery and billing status for a sales order."""
        try:
            url = (
                f"{ORDER_API}/A_SalesOrder('{order_id}')?"
                f"$select=SalesOrder,OverallSDProcessStatus,TotalDeliveryStatus,"
                f"OverallBillingStatus,TotalNetAmount,TransactionCurrency,"
                f"SoldToParty,CreationDate"
            )
            res = self.session.get(url, timeout=30)
            if res.status_code == 200:
                d = res.json().get('d', {})
                status_map = {'A': 'Open', 'B': 'Partial', 'C': 'Complete', '': 'Not Started'}
                return {
                    'order': d.get('SalesOrder'),
                    'overall': status_map.get(d.get('OverallSDProcessStatus', ''), 'Unknown'),
                    'delivery': status_map.get(d.get('TotalDeliveryStatus', ''), 'Not Started'),
                    'billing': status_map.get(d.get('OverallBillingStatus', ''), 'Not Started'),
                    'amount': d.get('TotalNetAmount', '0'),
                    'currency': d.get('TransactionCurrency', 'EUR'),
                    'customer': d.get('SoldToParty', ''),
                    'date': self._parse_sap_date(d.get('CreationDate')),
                }
            return None
        except Exception as e:
            logger.error("Order fulfillment status error: %s", e)
            return None

    def get_customer_order_history(self, customer_id, top=10):
        """Get recent sales orders for a customer (Customer 360)."""
        try:
            url = (
                f"{ORDER_API}/A_SalesOrder?"
                f"$filter=SoldToParty eq '{customer_id}'"
                f"&$select=SalesOrder,TotalNetAmount,TransactionCurrency,"
                f"CreationDate,OverallSDProcessStatus"
                f"&$top={top}&$orderby=CreationDate desc"
            )
            res = self.session.get(url, timeout=30)
            if res.status_code == 200:
                return res.json().get('d', {}).get('results', [])
            return []
        except Exception as e:
            logger.error("Customer order history error: %s", e)
            return []

    def get_customer_revenue_summary(self, customer_id):
        """Get invoice totals for a customer (Customer 360)."""
        try:
            url = (
                f"{BILLING_API}/A_BillingDocument?"
                f"$filter=SoldToParty eq '{customer_id}'"
                f"&$select=BillingDocument,TotalNetAmount,TransactionCurrency,BillingDocumentDate"
                f"&$top=100&$orderby=BillingDocumentDate desc"
            )
            res = self.session.get(url, timeout=30)
            if res.status_code == 200:
                docs = res.json().get('d', {}).get('results', [])
                total_rev = sum(float(d.get('TotalNetAmount', 0)) for d in docs)
                return {
                    'total_revenue': total_rev,
                    'invoice_count': len(docs),
                    'currency': docs[0].get('TransactionCurrency', 'EUR') if docs else 'EUR',
                    'last_invoice_date': self._parse_sap_date(
                        docs[0].get('BillingDocumentDate')) if docs else 'N/A',
                }
            return None
        except Exception as e:
            logger.error("Customer revenue summary error: %s", e)
            return None

    def get_customer_quote_history(self, customer_id, top=10):
        """Get recent quotations for a customer (Customer 360)."""
        try:
            url = (
                f"{QUOTE_API}/A_SalesQuotation?"
                f"$filter=SoldToParty eq '{customer_id}'"
                f"&$select=SalesQuotation,TotalNetAmount,TransactionCurrency,"
                f"CreationDate,OverallSDProcessStatus"
                f"&$top={top}&$orderby=CreationDate desc"
            )
            res = self.session.get(url, timeout=30)
            if res.status_code == 200:
                return res.json().get('d', {}).get('results', [])
            return []
        except Exception as e:
            logger.error("Customer quote history error: %s", e)
            return []

    def get_expiring_price_conditions(self, days_ahead=7):
        """Get pricing conditions expiring within N days."""
        try:
            today = datetime.now()
            future = today + timedelta(days=days_ahead)
            today_str = today.strftime("%Y-%m-%d")
            future_str = future.strftime("%Y-%m-%d")

            url = (
                f"{PRICE_API}/A_SlsPrcgCndnRecdValidity?"
                f"$filter=ConditionValidityEndDate ge datetime'{today_str}T00:00:00' "
                f"and ConditionValidityEndDate le datetime'{future_str}T23:59:59'"
                f"&$select=ConditionRecord,ConditionType,Material,Customer,"
                f"ConditionRateValue,ConditionValidityEndDate"
                f"&$top=50"
            )
            res = self.session.get(url, timeout=30)
            if res.status_code == 200:
                return res.json().get('d', {}).get('results', [])
            return []
        except Exception as e:
            logger.error("Expiring price conditions error: %s", e)
            return []

    def get_smart_reorder_suggestions(self, customer_id):
        """Analyze order history to suggest frequently ordered materials."""
        try:
            url = (
                f"{ORDER_API}/A_SalesOrder?"
                f"$filter=SoldToParty eq '{customer_id}'"
                f"&$select=SalesOrder&$top=20&$orderby=CreationDate desc"
            )
            res = self.session.get(url, timeout=30)
            if res.status_code != 200:
                return []

            orders = res.json().get('d', {}).get('results', [])
            material_freq = {}

            for order in orders[:10]:
                order_id = order.get('SalesOrder')
                items_url = (
                    f"{ORDER_API}/A_SalesOrder('{order_id}')/to_Item?"
                    f"$select=Material,SalesOrderItemText,OrderQuantity"
                )
                items_res = self.session.get(items_url, timeout=20)
                if items_res.status_code == 200:
                    items = items_res.json().get('d', {}).get('results', [])
                    for item in items:
                        mat = item.get('Material', '')
                        if mat:
                            if mat not in material_freq:
                                material_freq[mat] = {
                                    'material': mat,
                                    'desc': item.get('SalesOrderItemText', 'N/A'),
                                    'count': 0,
                                    'total_qty': 0,
                                }
                            material_freq[mat]['count'] += 1
                            try:
                                material_freq[mat]['total_qty'] += float(
                                    item.get('OrderQuantity', 0))
                            except (ValueError, TypeError):
                                pass

            suggestions = sorted(material_freq.values(), key=lambda x: x['count'], reverse=True)
            for s in suggestions:
                if s['count'] > 0:
                    s['avg_qty'] = s['total_qty'] / s['count']
            return suggestions[:5]

        except Exception as e:
            logger.error("Smart reorder suggestions error: %s", e)
            return []

    # ================================================================
    # COPILOT: Optimized Billing/Invoice Query
    # ================================================================
    def get_billing_documents(self, date_filter=None, status_filter=None, customer_id=None, top=20):
        """
        Fetch billing documents with optimized OData $filter, $select, $top.
        Used by AI Copilot for natural language invoice queries.

        Args:
            date_filter: "today", "this_week", "this_month", or "YYYY-MM-DD"
            status_filter: "open" or "closed" or None (all)
            customer_id: filter by sold-to party
            top: max results (default 20, capped at 50)
        Returns:
            dict with success, documents list, summary counts
        """
        try:
            top = min(top, 50)
            filters = []
            today = datetime.now()

            # Date filter
            if date_filter == 'today':
                d = today.strftime("%Y-%m-%d")
                filters.append(f"BillingDocumentDate eq datetime'{d}T00:00:00'")
            elif date_filter == 'this_week':
                week_start = (today - timedelta(days=today.weekday())).strftime("%Y-%m-%d")
                filters.append(f"BillingDocumentDate ge datetime'{week_start}T00:00:00'")
                filters.append(f"BillingDocumentDate le datetime'{today.strftime('%Y-%m-%d')}T23:59:59'")
            elif date_filter == 'this_month':
                month_start = today.replace(day=1).strftime("%Y-%m-%d")
                filters.append(f"BillingDocumentDate ge datetime'{month_start}T00:00:00'")
                filters.append(f"BillingDocumentDate le datetime'{today.strftime('%Y-%m-%d')}T23:59:59'")
            elif date_filter and len(date_filter) == 10:
                filters.append(f"BillingDocumentDate eq datetime'{date_filter}T00:00:00'")

            # Customer filter
            if customer_id:
                filters.append(f"SoldToParty eq '{customer_id}'")

            filter_str = " and ".join(filters) if filters else ""
            filter_param = f"$filter={filter_str}&" if filter_str else ""

            url = (
                f"{BILLING_API}/A_BillingDocument?"
                f"{filter_param}"
                f"$select=BillingDocument,SoldToParty,TotalNetAmount,"
                f"TransactionCurrency,BillingDocumentDate,OverallSDProcessStatus"
                f"&$top={top}&$orderby=BillingDocumentDate desc&$format=json"
            )

            res = self.session.get(url, timeout=20)
            if res.status_code == 200:
                docs = res.json().get('d', {}).get('results', [])

                # Post-filter by status if needed (SAP may not support direct filter on this)
                if status_filter == 'open':
                    docs = [d for d in docs if d.get('OverallSDProcessStatus', '') != 'C']
                elif status_filter == 'closed':
                    docs = [d for d in docs if d.get('OverallSDProcessStatus', '') == 'C']

                total_amount = sum(float(d.get('TotalNetAmount', 0)) for d in docs)
                currency = docs[0].get('TransactionCurrency', 'EUR') if docs else 'EUR'

                return {
                    'success': True,
                    'documents': docs,
                    'count': len(docs),
                    'total_amount': total_amount,
                    'currency': currency,
                }

            logger.warning("Billing documents query returned %s", res.status_code)
            return {'success': False, 'documents': [], 'count': 0, 'error': f'HTTP {res.status_code}'}

        except requests.RequestException as e:
            logger.error("Billing documents query error: %s", e)
            return {'success': False, 'documents': [], 'count': 0, 'error': str(e)}

    # ================================================================
    # COPILOT: Dynamic OData Query Executor (Text-to-OData Agent)
    # ================================================================

    # Map AI-generated service names to actual base URLs
    _SERVICE_MAP = {
        'API_SALES_ORDER_SRV': ORDER_API,
        'API_SALES_QUOTATION_SRV': QUOTE_API,
        'API_BILLING_DOCUMENT_SRV': BILLING_API,
        'API_PRODUCT_SRV': PRODUCT_API,
        'API_BUSINESS_PARTNER': BUSINESS_PARTNER_API,
        'API_SLSPRICINGCONDITIONRECORD_SRV': PRICE_API,
        'API_MATERIAL_STOCK_SRV': STOCK_API,
        'YY1_TOTALEXPOSURE_CDS': CREDIT_API,
    }

    # Allowed read-only entity sets — WRITE entity sets are NOT here (safety)
    _ALLOWED_ENTITY_SETS = frozenset({
        'A_SalesOrder', 'A_SalesOrderItem',
        'A_SalesQuotation', 'A_SalesQuotationItem',
        'A_BillingDocument', 'A_BillingDocumentItem',
        'A_ProductDescription', 'A_Product',
        'A_Customer', 'A_CustomerSalesArea',
        'A_SlsPrcgCndnRecdValidity',
        'A_MatlStkInAcctMod',
        'YY1_TotalExposure',
        'A_BusinessPartner',
        'A_GLAccountText', 'A_CostCenter',
    })

    # Blocked query patterns — prevent writes via dynamic queries
    _BLOCKED_PATTERNS = ('$batch', 'POST', 'PUT', 'PATCH', 'DELETE', 'MERGE')

    def execute_dynamic_odata_query(self, service, entity_set, query_options):
        """
        Execute a dynamically generated OData READ query against SAP.

        Safety guardrails:
        - Only whitelisted entity sets are allowed
        - Only known service endpoints are allowed
        - Query is GET-only (no write patterns)
        - Hard $top cap at 50 records
        - 20-second timeout

        Args:
            service: AI-generated service key (e.g. 'API_SALES_ORDER_SRV')
            entity_set: OData entity set (e.g. 'A_SalesOrder')
            query_options: OData query string (e.g. '$filter=...&$select=...&$format=json')

        Returns:
            dict with: success, data (list of records), count, error
        """
        try:
            # --- GUARDRAIL 1: Validate service ---
            base_url = self._SERVICE_MAP.get(service)
            if not base_url:
                return {'success': False, 'data': [], 'count': 0,
                        'error': f"Unknown service: {service}"}

            # --- GUARDRAIL 2: Validate entity set ---
            if entity_set not in self._ALLOWED_ENTITY_SETS:
                return {'success': False, 'data': [], 'count': 0,
                        'error': f"Entity set not allowed: {entity_set}"}

            # --- GUARDRAIL 3: Block write-like patterns ---
            query_upper = query_options.upper()
            for blocked in self._BLOCKED_PATTERNS:
                if blocked in query_upper:
                    return {'success': False, 'data': [], 'count': 0,
                            'error': f"Blocked pattern in query: {blocked}"}

            # --- GUARDRAIL 4: Enforce $format=json ---
            if '$format=json' not in query_options:
                query_options += '&$format=json'

            # --- GUARDRAIL 5: Cap $top at 50 ---
            top_match = re.search(r'\$top=(\d+)', query_options)
            if top_match:
                top_val = int(top_match.group(1))
                if top_val > 50:
                    query_options = re.sub(r'\$top=\d+', '$top=50', query_options)
            else:
                query_options += '&$top=50'

            # Build final URL
            url = f"{base_url}/{entity_set}?{query_options}"
            logger.info("Dynamic OData query: %s", url[:200])

            res = self.session.get(url, timeout=20)

            if res.status_code == 200:
                body = res.json()
                results = body.get('d', {}).get('results', [])

                # Some single-entity queries return 'd' directly without 'results'
                if not results and 'd' in body and isinstance(body['d'], dict):
                    if '__metadata' in body['d']:
                        results = [body['d']]

                return {
                    'success': True,
                    'data': results,
                    'count': len(results),
                }

            logger.warning("Dynamic OData query returned HTTP %s for %s",
                           res.status_code, entity_set)
            # Include response body snippet for debugging
            error_body = res.text[:300] if res.text else ""
            return {
                'success': False, 'data': [], 'count': 0,
                'error': f"SAP returned HTTP {res.status_code}: {error_body}",
            }

        except requests.RequestException as e:
            logger.error("Dynamic OData query network error: %s", e)
            return {'success': False, 'data': [], 'count': 0, 'error': str(e)}
        except Exception as e:
            logger.error("Dynamic OData query unexpected error: %s", e, exc_info=True)
            return {'success': False, 'data': [], 'count': 0, 'error': str(e)}
