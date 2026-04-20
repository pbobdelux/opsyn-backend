import logging
import os
import time

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("opsyn-sync-worker")

SYNC_URL = os.getenv("SYNC_URL", "https://opsyn-backend-production.up.railway.app/sync/leaflink")
SYNC_BRAND_ID = os.getenv("SYNC_BRAND_ID", "noble-nectar")
SYNC_INTERVAL_SECONDS = int(os.getenv("SYNC_INTERVAL_SECONDS", "60"))


def run_scheduler():
    logger.info("Sync scheduler started")
    logger.info("Using sync URL: %s", SYNC_URL)
    logger.info("Using brand id: %s", SYNC_BRAND_ID)
    logger.info("Interval: %s seconds", SYNC_INTERVAL_SECONDS)

    while True:
        try:
            logger.info("Starting LeafLink sync")
            response = requests.post(
                SYNC_URL,
                json={"brand_id": SYNC_BRAND_ID},
                timeout=120,
            )
            logger.info("Sync response: %s %s", response.status_code, response.text)
        except Exception as exc:
            logger.exception("Sync failed: %s", exc)

        logger.info("Sleeping for %s seconds", SYNC_INTERVAL_SECONDS)
        time.sleep(SYNC_INTERVAL_SECONDS)