import google.generativeai as genai
import json
import requests
import PIL.Image
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from config import GOOGLE_API_KEY, MINIMAX_API_KEY
from logger_setup import get_logger

logger = get_logger(__name__)

# Gemini API timeout (seconds) — prevents infinite hangs
_LLM_TIMEOUT = 25

# Shared executor for Gemini calls (avoids thread leak)
_llm_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="gemini")

# ================================================================
# MINIMAX CROSS-VENDOR FALLBACK CONFIG
# ================================================================
_MINIMAX_API_URL = "https://api.minimax.io/v1/chat/completions"
_MINIMAX_MODELS = ["MiniMax-M2.5", "MiniMax-M2.5-highspeed"]
_MINIMAX_TIMEOUT = 30  # seconds

# ================================================================
# STEP 1 — INTENT ROUTER PROMPT (lightweight, fast classification)
# ================================================================
_ROUTER_SYSTEM_PROMPT = """You are an SAP S/4HANA enterprise assistant router with advanced Natural Language Understanding.
Classify the user message into ONE of these categories.

IMPORTANT: When in doubt, ALWAYS choose DYNAMIC_QUERY. Only use UNKNOWN for messages
that are completely unrelated to business, SAP, ERP, sales, finance, or enterprise operations
(e.g. "tell me a joke", "what's the weather").

WRITE OPERATIONS (ONLY if user explicitly wants to CREATE or POST something new):
- CREATE_ORDER      → User explicitly says "create", "make", "place" a Sales Order
- CREATE_QUOTE      → User explicitly says "create", "make" a Sales Quotation / Quote
- POST_PETTY_CASH   → User explicitly says "post", "record" a petty cash or journal entry

DYNAMIC_QUERY → THIS IS THE DEFAULT. Use for ANY of these:
  - Questions about data: "how many", "show me", "list", "what is", "tell me"
  - Price checks: "price of material", "what does X cost"
  - Stock checks: "stock of", "inventory of", "how much X in warehouse"
  - Credit checks: "credit limit", "exposure", "credit of customer"
  - Searching: "search", "find", "look up", "who is customer"
  - Viewing records: "show order", "view invoice", "details of"
  - Analytics: "total sales", "revenue", "count of", "open invoices"
  - Any question mentioning: orders, quotations, invoices, materials, customers, products, billing, pricing

UNKNOWN → The message has NOTHING to do with business/SAP (e.g. "hello", "tell me a joke")

═══════════════════════════════════════════════════
NATURAL LANGUAGE UNDERSTANDING RULES:
═══════════════════════════════════════════════════
1. Users may type casually (e.g. "make an order with 10 balls for customer 1000000").
   You MUST intelligently parse these messy inputs and extract all entities.
2. Infer intent from conversational synonyms:
   - "order", "place", "make an order", "I need to order" → CREATE_ORDER
   - "quote", "quotation", "make a quote", "get me a quote" → CREATE_QUOTE
   - "post expense", "petty cash", "log expense" → POST_PETTY_CASH
3. Product name mapping: If the user mentions a product by common name (e.g. "balls",
   "screws", "pipes"), place it in "material_name" so the system can search SAP.
   Only put a value in "material_id" if the user provides an explicit numeric ID.
4. Multi-intent: If a message contains multiple intents (e.g. "create a quote and then
   make an order"), pick the FIRST actionable intent only.

═══════════════════════════════════════════════════
STRICT DATA TYPE RULES FOR ENTITIES (CRITICAL):
═══════════════════════════════════════════════════
- "quantity"  → MUST be a JSON integer (e.g. 10), NEVER a string like "10"
- "discount"  → MUST be a JSON number (e.g. 5.0), NEVER a string like "5"
- "amount"    → MUST be a JSON number (e.g. 500.00), NEVER a string like "500"
- "confidence"→ MUST be a JSON number between 0.0 and 1.0
- "customer_id", "material_id", "reference", "currency", "description",
  "customer_name", "material_name" → MUST be JSON strings or null
- If a numeric entity cannot be determined, set it to null — NEVER use 0 as placeholder.

Respond in STRICT JSON (no markdown, no code blocks):
{"category": "DYNAMIC_QUERY", "confidence": 0.95, "entities": {"customer_id": null, "customer_name": null, "material_id": null, "material_name": null, "quantity": null, "discount": null, "reference": null, "amount": null, "description": null, "currency": null}, "summary": "summary here"}"""

# ================================================================
# STEP 2 — DYNAMIC TEXT-TO-ODATA PROMPT (schema-aware query builder)
# ================================================================
_ODATA_SCHEMA_PROMPT = """You are an SAP S/4HANA OData query generator.
Given a user's natural language question, generate the exact OData query to answer it.

TODAY'S DATE: {today}

═══════════════════════════════════════════════════
AVAILABLE ODATA ENDPOINTS AND THEIR SCHEMAS:
═══════════════════════════════════════════════════

1. SALES ORDERS — service: API_SALES_ORDER_SRV
   Entity: A_SalesOrder
   Key fields:
     SalesOrder, SoldToParty, TotalNetAmount, TransactionCurrency,
     CreationDate, OverallSDProcessStatus, PurchaseOrderByCustomer,
     SalesOrganization, DistributionChannel
   Navigation: to_Item → A_SalesOrderItem (Material, SalesOrderItemText,
     RequestedQuantity, RequestedQuantityUnit, NetAmount, TransactionCurrency)
   Status codes: '' = Open, 'A' = Open, 'B' = Partially Processed, 'C' = Completed
   Date format in $filter: datetime'YYYY-MM-DDT00:00:00'

2. SALES QUOTATIONS — service: API_SALES_QUOTATION_SRV
   Entity: A_SalesQuotation
   Key fields:
     SalesQuotation, SoldToParty, TotalNetAmount, TransactionCurrency,
     CreationDate, OverallSDProcessStatus, SalesQuotationDate,
     BindingPeriodValidityEndDate
   Navigation: to_Item → A_SalesQuotationItem (Material, SalesQuotationItemText,
     RequestedQuantity, NetAmount)
   Status codes: same as orders

3. BILLING DOCUMENTS (Invoices) — service: API_BILLING_DOCUMENT_SRV
   Entity: A_BillingDocument
   Key fields:
     BillingDocument, SoldToParty, TotalNetAmount, TransactionCurrency,
     BillingDocumentDate, OverallSDProcessStatus
   Entity: A_BillingDocumentItem
   Key fields:
     BillingDocument, BillingDocumentItem, ReferenceSDDocument, Material,
     NetAmount, TransactionCurrency

4. PRODUCTS / MATERIALS — service: API_PRODUCT_SRV
   Entity: A_ProductDescription
   Key fields: Product, ProductDescription, Language
   Always filter: Language eq 'EN'
   For search: substringof('keyword', ProductDescription)

5. CUSTOMERS — service: API_BUSINESS_PARTNER
   Entity: A_Customer
   Key fields: Customer, CustomerName, CustomerFullName
   For search: substringof('keyword', CustomerName) or startswith(Customer, 'id')

6. PRICING CONDITIONS — service: API_SLSPRICINGCONDITIONRECORD_SRV
   Entity: A_SlsPrcgCndnRecdValidity
   Key fields: ConditionRecord, ConditionType, Material, Customer,
     ConditionRateValue, ConditionRateValueUnit, ConditionValidityStartDate,
     ConditionValidityEndDate
   For standard price: ConditionType eq 'PPR0' (and no Customer filter)
   For customer-specific: add Customer eq 'XXXXX'
   For all customers for a material: ConditionType eq 'PPR0' and Material eq 'XXXXX' (no customer filter)
   NOTE: Material IDs must be 18-char zero-padded: material "100" → "000000000000000100"

7. MATERIAL STOCK — service: API_MATERIAL_STOCK_SRV
   Entity: A_MatlStkInAcctMod
   Key fields: Material, Plant, StorageLocation, MatlWrhsStkQtyInMatlBaseUnit,
     MaterialBaseUnit

8. CREDIT EXPOSURE — service: YY1_TOTALEXPOSURE_CDS
   Entity: YY1_TotalExposure
   Key fields: BusinessPartner, TotalExposure, CreditLimit

═══════════════════════════════════════════════════
ODATA QUERY RULES:
═══════════════════════════════════════════════════
- ALWAYS include $format=json
- ALWAYS include $select with only the fields you need (performance)
- Use $top to limit results (default 25, max 50)
- Use $orderby for sorting (e.g. CreationDate desc)
- Use $filter for conditions. Date filter: datetime'YYYY-MM-DDT00:00:00'
- For counting, use $top=50 with minimal $select — the system counts results automatically.
  NEVER use $top values above 50. The system enforces a hard cap of 50.
- $inlinecount=allpages returns __count in results (SAP may not always support this)
- For "today": use {today}T00:00:00
- For "this week": calculate Monday of current week
- For "this month": use first day of month
- String values in filters use single quotes: SoldToParty eq '1000000'
- DO NOT use $count endpoint (not reliable in SAP OData V2). Instead select minimal fields and count in post-processing.
- For "open" status: OverallSDProcessStatus ne 'C'
- For "completed" status: OverallSDProcessStatus eq 'C'
- Strings in substringof use: substringof('value', FieldName)

═══════════════════════════════════════════════════

Respond in STRICT JSON (no markdown, no code blocks):
{{
    "service": "API_SALES_ORDER_SRV | API_SALES_QUOTATION_SRV | API_BILLING_DOCUMENT_SRV | API_PRODUCT_SRV | API_BUSINESS_PARTNER | API_SLSPRICINGCONDITIONRECORD_SRV | API_MATERIAL_STOCK_SRV | YY1_TOTALEXPOSURE_CDS",
    "entity_set": "A_SalesOrder | A_SalesQuotation | A_BillingDocument | ... (exact entity set name)",
    "query_options": "$filter=...&$select=...&$top=...&$orderby=...&$format=json",
    "explanation": "Brief explanation of what this query does"
}}"""

# ================================================================
# STEP 3 — RESPONSE SYNTHESIZER PROMPT
# ================================================================
_SYNTHESIZER_PROMPT = """You are an SAP S/4HANA assistant. The user asked a question,
and the system has queried SAP OData and retrieved real data.

USER'S ORIGINAL QUESTION: {question}

SAP ODATA RESPONSE DATA (JSON):
{data}

TOTAL RECORDS RETURNED: {count}

Instructions:
- Answer the user's question directly and naturally based on the data above.
- Use numbers, counts, totals, and summaries as appropriate.
- If the data is a list, format it neatly with key fields.
- Keep it concise — under 15 lines for lists, 3-5 lines for summaries.
- Do NOT show raw JSON. Give a human-friendly answer.
- Use plain text only. No markdown headers. Keep it clean.
- If zero results: say clearly "No records found" and suggest why (date range, filter, etc).
- For amounts, include currency and use comma formatting (e.g., 1,234.56 EUR).
- For dates in /Date(timestamp)/ format, convert to DD-MM-YYYY.

Respond with ONLY the formatted answer text (no JSON wrapper, no code blocks)."""


_EXPENSE_IMAGE_PROMPT = """You are an SAP S/4HANA enterprise assistant AI.
The user has sent an image (likely a receipt/invoice) with a text caption.
Your job is to:
1. Extract data from the image (vendor, amount, date, description, currency).
2. Understand the caption to determine the posting type.

"Petty cash", "without vendor", "journal entry" → type is "PETTY_CASH"
"Expense claim", "supplier invoice", "with vendor" → type is "SUPPLIER_INVOICE"

Respond in STRICT JSON (no markdown, no code blocks):
{
    "posting_type": "PETTY_CASH or SUPPLIER_INVOICE",
    "vendor_name": "string or null",
    "amount": "number (no currency symbols)",
    "currency": "ISO code (e.g. USD, EUR, INR)",
    "date": "YYYY-MM-DD",
    "description": "short 3-4 word description",
    "category": "one keyword (Food, Travel, Office, Fuel, etc.)",
    "confidence": 0.0 to 1.0
}"""


def _call_gemini_with_timeout(model, content, timeout=_LLM_TIMEOUT):
    """Call Gemini generate_content with a hard timeout to prevent infinite hangs."""
    future = _llm_executor.submit(model.generate_content, content)
    try:
        return future.result(timeout=timeout)
    except FuturesTimeoutError:
        future.cancel()
        raise TimeoutError(f"Gemini API did not respond within {timeout}s")


def _clean_json_response(text):
    """Strip markdown code fences, leading 'json' label, and MiniMax <think> tags."""
    import re
    # Strip MiniMax reasoning tags: <think>...</think>
    text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
    # Strip Gemini markdown code fences
    text = text.replace("```json", "").replace("```", "").strip()
    if text.startswith("json"):
        text = text[4:].strip()
    return text


class AIHandler:
    def __init__(self):
        genai.configure(api_key=GOOGLE_API_KEY)

        # Model priority — 5 models for maximum resilience
        # Lite variants have SEPARATE rate limits from their parent models
        self.models_to_try = [
            'gemini-2.5-flash',
            'gemini-2.0-flash',
            'gemini-2.5-flash-lite',
            'gemini-2.0-flash-lite',
            'gemini-2.0-flash-001',
        ]
        # Track models that returned 429 to skip them for 60s
        self._rate_limited = {}  # model_name → unblock_timestamp

    def _call_minimax_fallback(self, prompt, system_instruction=""):
        """
        Cross-vendor fallback — calls MiniMax AI when all Gemini models are exhausted.
        Uses the OpenAI-compatible chat completions endpoint.
        Tries multiple MiniMax models for resilience.
        Returns raw text response or None on failure.
        """
        if not MINIMAX_API_KEY:
            logger.warning("MiniMax fallback skipped: no API key configured")
            return None

        messages = []
        if system_instruction:
            messages.append({"role": "system", "content": system_instruction})
        messages.append({"role": "user", "content": prompt})

        headers = {
            "Authorization": f"Bearer {MINIMAX_API_KEY}",
            "Content-Type": "application/json",
        }

        for model_name in _MINIMAX_MODELS:
            payload = {
                "model": model_name,
                "messages": messages,
                "max_tokens": 2048,
                "temperature": 0.1,
            }

            try:
                logger.info("MiniMax fallback: calling %s at %s", model_name, _MINIMAX_API_URL)
                resp = requests.post(
                    _MINIMAX_API_URL,
                    json=payload,
                    headers=headers,
                    timeout=_MINIMAX_TIMEOUT,
                )

                if resp.status_code == 401:
                    logger.error("MiniMax AUTH FAILED (401): API key is invalid or expired. "
                                 "Response: %s", resp.text[:300])
                    return None  # Auth error — no point trying other models

                if resp.status_code == 429:
                    logger.warning("MiniMax %s rate-limited (429), trying next model", model_name)
                    continue

                if resp.status_code != 200:
                    logger.warning("MiniMax %s returned HTTP %d: %s",
                                   model_name, resp.status_code, resp.text[:300])
                    continue

                data = resp.json()

                # OpenAI-compatible response parsing
                choices = data.get("choices", [])
                if not choices:
                    logger.warning("MiniMax %s returned empty choices: %s", model_name, str(data)[:300])
                    continue

                text = choices[0].get("message", {}).get("content", "")
                if not text:
                    logger.warning("MiniMax %s returned empty content", model_name)
                    continue

                logger.info("MiniMax %s returned %d chars", model_name, len(text))
                return text

            except requests.exceptions.Timeout:
                logger.warning("MiniMax %s timed out after %ds", model_name, _MINIMAX_TIMEOUT)
                continue
            except requests.exceptions.ConnectionError as e:
                logger.warning("MiniMax %s connection error: %s", model_name, str(e)[:150])
                continue
            except (KeyError, IndexError, ValueError) as e:
                logger.warning("MiniMax %s response parse error: %s", model_name, str(e)[:150])
                continue
            except Exception as e:
                logger.warning("MiniMax %s unexpected error: %s", model_name, str(e)[:200])
                continue

        logger.error("ALL MiniMax models also failed")
        return None

    def _try_generate(self, prompt, timeout=_LLM_TIMEOUT):
        """
        Try Gemini models in order, skip rate-limited ones.
        If ALL Gemini models fail → route to MiniMax as ultimate fallback.
        Returns raw text or None.
        """
        import time
        now = time.time()
        for model_name in self.models_to_try:
            # Skip models that were rate-limited recently
            blocked_until = self._rate_limited.get(model_name, 0)
            if now < blocked_until:
                logger.debug("Skipping rate-limited model %s (%.0fs left)",
                             model_name, blocked_until - now)
                continue
            try:
                logger.info("Trying Gemini model: %s", model_name)
                model = genai.GenerativeModel(model_name)
                response = _call_gemini_with_timeout(model, prompt, timeout=timeout)
                text = response.text
                logger.info("Gemini %s returned %d chars", model_name, len(text) if text else 0)
                return text
            except TimeoutError:
                logger.warning("Model %s timed out after %ds", model_name, timeout)
                continue
            except Exception as e:
                err_str = str(e)
                if '429' in err_str or 'quota' in err_str.lower():
                    self._rate_limited[model_name] = now + 60
                    logger.warning("Model %s rate-limited (429), blocking for 60s", model_name)
                else:
                    logger.warning("Model %s failed: %s", model_name, err_str[:200])
                continue

        # ── ALL Gemini models exhausted → MiniMax cross-vendor fallback ──
        logger.warning("Gemini chain exhausted, switching to MiniMax fallback")

        # Extract system instruction if the prompt contains our known separator
        system_instruction = ""
        user_content = prompt if isinstance(prompt, str) else str(prompt)

        # Our prompts are formatted as: "<system>\n\nUSER MESSAGE:\n<text>"
        # or "<system>\n\nUSER QUESTION:\n<text>"
        for sep in ("\n\nUSER MESSAGE:\n", "\n\nUSER QUESTION:\n"):
            if sep in user_content:
                system_instruction, user_content = user_content.split(sep, 1)
                break

        minimax_text = self._call_minimax_fallback(user_content, system_instruction)
        if minimax_text:
            return minimax_text

        logger.error("ALL AI providers failed (Gemini + MiniMax)")
        return None

    # ================================================================
    # STEP 1: Lightweight Intent Router
    # ================================================================
    def classify_intent(self, user_text):
        """
        Fast classification — routes to WRITE intents or DYNAMIC_QUERY.
        Returns dict with: success, intent, confidence, entities, summary, _reason
        """
        prompt = f"{_ROUTER_SYSTEM_PROMPT}\n\nUSER MESSAGE:\n{user_text}"
        raw = self._try_generate(prompt)

        if not raw:
            logger.error("classify_intent: ALL models returned None for: %s", user_text[:100])
            return {"success": False, "intent": "UNKNOWN", "confidence": 0,
                    "entities": {}, "summary": "All AI models unavailable",
                    "_reason": "ALL_MODELS_FAILED"}

        logger.info("classify_intent raw response (first 300 chars): %s", raw[:300])

        try:
            cleaned = _clean_json_response(raw)
            data = json.loads(cleaned)
            category = data.get("category", "UNKNOWN")
            logger.info("Router classified: %s (%.2f) summary=%s",
                        category, data.get('confidence', 0), data.get('summary', ''))

            return {
                "success": True,
                "intent": category,
                "confidence": data.get("confidence", 0.0),
                "entities": data.get("entities", {}),
                "summary": data.get("summary", ""),
                "_reason": "OK",
            }
        except (json.JSONDecodeError, ValueError) as e:
            logger.error("classify_intent JSON parse failed: %s | raw: %s", e, raw[:500])
            return {"success": False, "intent": "UNKNOWN", "confidence": 0,
                    "entities": {}, "summary": f"Parse error: {e}",
                    "_reason": "PARSE_ERROR", "_raw": raw[:300]}

    # ================================================================
    # STEP 2: Dynamic Text-to-OData Query Generation
    # ================================================================
    def generate_odata_query(self, user_text):
        """
        Schema-aware OData query generation. Gemini reads the SAP schema
        and produces the exact endpoint + query string.

        Returns:
            dict with keys: success, service, entity_set, query_options, explanation
        """
        today = datetime.now().strftime("%Y-%m-%d")
        prompt = _ODATA_SCHEMA_PROMPT.format(today=today) + f"\n\nUSER QUESTION:\n{user_text}"
        raw = self._try_generate(prompt)

        if not raw:
            return {"success": False, "error": "AI query generation timed out"}

        try:
            data = json.loads(_clean_json_response(raw))
            service = data.get("service", "")
            entity_set = data.get("entity_set", "")
            query_options = data.get("query_options", "")
            explanation = data.get("explanation", "")

            if not service or not entity_set:
                return {"success": False, "error": "AI generated incomplete query"}

            logger.info("OData query generated: %s/%s?%s", service, entity_set, query_options[:120])

            return {
                "success": True,
                "service": service,
                "entity_set": entity_set,
                "query_options": query_options,
                "explanation": explanation,
            }
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning("OData query generation parse error: %s", e)
            return {"success": False, "error": f"Parse error: {e}"}

    # ================================================================
    # STEP 3: Natural Language Response Synthesis
    # ================================================================
    def synthesize_response(self, user_question, sap_data, record_count):
        """
        Feed raw SAP JSON data back to Gemini for human-friendly summarization.

        Args:
            user_question: Original natural language question
            sap_data: Raw JSON data from SAP (truncated to prevent token overflow)
            record_count: Total number of records returned
        Returns:
            str — Human-friendly answer text, or a fallback error string
        """
        # Truncate data to avoid token limits — send first 30 records max
        if isinstance(sap_data, list):
            truncated = sap_data[:30]
        else:
            truncated = sap_data

        data_str = json.dumps(truncated, indent=None, default=str)
        # Hard cap at 12000 chars to stay well within context
        if len(data_str) > 12000:
            data_str = data_str[:12000] + "\n... (truncated)"

        prompt = _SYNTHESIZER_PROMPT.format(
            question=user_question,
            data=data_str,
            count=record_count,
        )

        raw = self._try_generate(prompt, timeout=20)
        if not raw:
            return f"Retrieved {record_count} record(s) from SAP but could not generate summary."

        # The synthesizer returns plain text, not JSON
        # Strip MiniMax <think> tags if present
        import re
        cleaned = re.sub(r'<think>.*?</think>', '', raw, flags=re.DOTALL)
        return cleaned.strip()

    # ================================================================
    # MULTI-MODAL: Image + Caption for Expense Posting
    # ================================================================
    def classify_expense_image(self, image_path, caption=""):
        """
        Analyze a receipt/invoice image with an optional caption to extract
        expense data and determine the posting type.
        """
        img = None
        last_error = None

        try:
            logger.info("Copilot processing expense image: %s | caption: %s", image_path, caption)
            img = PIL.Image.open(image_path)

            prompt = f"{_EXPENSE_IMAGE_PROMPT}\n\nUSER CAPTION:\n{caption}" if caption else _EXPENSE_IMAGE_PROMPT

            for model_name in self.models_to_try:
                try:
                    model = genai.GenerativeModel(model_name)
                    response = _call_gemini_with_timeout(model, [prompt, img], timeout=30)

                    text_resp = _clean_json_response(response.text)
                    data = json.loads(text_resp)
                    logger.info("Copilot expense extraction successful via %s", model_name)

                    img.close()
                    return {"success": True, "data": data}

                except (json.JSONDecodeError, ValueError) as e:
                    logger.warning("Copilot expense model %s unparseable: %s", model_name, e)
                    last_error = e
                    continue
                except TimeoutError as e:
                    logger.warning("Copilot expense model %s timed out: %s", model_name, e)
                    last_error = e
                    continue
                except Exception as e:
                    logger.warning("Copilot expense model %s failed: %s", model_name, e)
                    last_error = e
                    continue

            if img:
                img.close()
            return {"success": False, "data": {}, "error": f"AI models failed: {last_error}"}

        except Exception as e:
            if img:
                try:
                    img.close()
                except Exception:
                    pass
            logger.error("Copilot expense image critical error: %s", e, exc_info=True)
            return {"success": False, "data": {}, "error": str(e)}

    # ================================================================
    # ORIGINAL: Receipt Scanner (kept for existing expense flows)
    # ================================================================
    def analyze_receipt(self, image_path, gl_accounts=None):
        """
        Extracts Vendor Name, Amount, Date, and Description from a receipt image
        using Gemini AI with model fallback.
        """
        img = None
        last_error = None

        try:
            logger.info("AI processing receipt image: %s", image_path)
            img = PIL.Image.open(image_path)

            gl_context = ""
            if gl_accounts:
                gl_lines = [f"  - {g.get('id', '')}: {g.get('name', '')}" for g in gl_accounts[:20]]
                gl_context = (
                    "\n\nAvailable GL Accounts for categorization:\n"
                    + "\n".join(gl_lines)
                    + "\n\nBased on the receipt content, suggest the best matching GL Account ID "
                    "in the 'suggested_gl' field."
                )

            prompt = f"""
            You are an expert accountant SAP bot. Analyze this receipt image.
            Extract the following details in strict JSON format (no markdown, no code blocks):
            {{
                "vendor_name": "Name of the establishment (e.g. Starbucks, Marriott, Uber)",
                "amount": "Total Amount (number only, remove currency symbols)",
                "currency": "ISO Currency Code (e.g. USD, EUR, INR)",
                "date": "YYYY-MM-DD (Date of transaction)",
                "description": "A very short 3-4 word description (e.g. 'Team Lunch', 'Taxi to Airport', 'Hotel Stay')",
                "category": "One keyword describing the expense (e.g. Food, Travel, Lodging, Office, Fuel)",
                "suggested_gl": "Best matching GL Account ID from the list below, or null if no list provided"
            }}
            If a field is not visible, use null.{gl_context}
            """

            for model_name in self.models_to_try:
                try:
                    logger.debug("Trying AI model: %s", model_name)
                    model = genai.GenerativeModel(model_name)
                    response = _call_gemini_with_timeout(model, [prompt, img], timeout=30)

                    text_resp = _clean_json_response(response.text)
                    data = json.loads(text_resp)
                    logger.info("AI extraction successful with model: %s", model_name)

                    img.close()
                    return {"success": True, "data": data}

                except (json.JSONDecodeError, ValueError) as inner_e:
                    logger.warning("Model %s returned unparseable response: %s", model_name, inner_e)
                    last_error = inner_e
                    continue
                except TimeoutError as inner_e:
                    logger.warning("Model %s timed out: %s", model_name, inner_e)
                    last_error = inner_e
                    continue
                except Exception as inner_e:
                    logger.warning("Model %s failed: %s", model_name, inner_e)
                    last_error = inner_e
                    continue

            if img: img.close()
            return {"success": False, "error": f"AI models failed. Last: {last_error}"}

        except Exception as e:
            if img:
                try:
                    img.close()
                except Exception:
                    pass
            logger.error("Critical AI error: %s", e, exc_info=True)
            return {"success": False, "error": str(e)}
