import json
import base64
import time
from config import SALES_QUEUE_NAME
from logger_setup import get_logger

logger = get_logger(__name__)


class PDFManager:
    def __init__(self, sap_handler, db_handler):
        self.sap = sap_handler
        self.db = db_handler

    def get_sales_order_pdf(self, order_id):
        """
        1. Checks Local DB Cache first.
        2. If missing, Drains SAP Queue -> Caches ALL items -> Matches Order ID.
        3. Returns binary PDF bytes or None.
        """
        try:
            target_id = str(int(order_id))
        except (ValueError, TypeError):
            return None

        # --- PHASE 1: CHECK LOCAL DB ---
        logger.debug("Checking PDF cache for order %s", target_id)
        cached_b64 = self.db.get_pdf_from_cache(target_id)
        if cached_b64:
            logger.info("PDF found in cache for order %s", target_id)
            return base64.b64decode(cached_b64)

        # --- PHASE 2: SYNC WITH SAP (Queue Drain) ---
        logger.info("PDF not cached, syncing SAP queue for order %s", target_id)

        max_checks = 15

        for i in range(max_checks):
            # A. Get Next Item ID
            item_id = self.sap.fetch_next_queue_item(SALES_QUEUE_NAME)

            if not item_id:
                logger.debug("SAP print queue empty, sync stopped")
                break

            # B. Get Document Data
            doc_data = self.sap.fetch_queue_document(item_id, SALES_QUEUE_NAME)

            if doc_data:
                blob = doc_data.get('Blob')
                meta_str = doc_data.get('Metadata')

                if blob and meta_str:
                    try:
                        meta_json = json.loads(meta_str)
                        sap_raw_id = meta_json.get('metadata', {}).get('business_detail_metadata', {}).get(
                            'appl_object_id', '0')
                        sap_clean_id = str(int(sap_raw_id))

                        # D. SAVE TO DB
                        self.db.save_pdf_to_cache(sap_clean_id, blob)
                        logger.debug("Drained and cached PDF for order %s", sap_clean_id)

                        # E. Check for Match
                        if sap_clean_id == target_id:
                            logger.info("Target PDF found during queue sync: order %s", target_id)
                            return base64.b64decode(blob)

                    except (json.JSONDecodeError, ValueError, KeyError) as e:
                        logger.warning("Metadata parse error during queue drain: %s", e)
                        continue

            time.sleep(0.5)

        # --- PHASE 3: FINAL CHECK ---
        cached_b64 = self.db.get_pdf_from_cache(target_id)
        if cached_b64:
            return base64.b64decode(cached_b64)

        return None
