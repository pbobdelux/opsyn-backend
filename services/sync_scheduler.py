import logging
import os
import time
from datetime import datetime

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

logger = logging.getLogger("opsyn-sync-worker")

# =============================================================================
# CONFIG
# =============================================================================

SYNC_URL = os.getenv(
    "SYNC_URL",
    "https://opsyn-backend-production.up.railway.app/sync/leaflink/run",
)

SYNC_SECRET = os.getenv("OPSYN_SYNC_SECRET", "")

SYNC_ORG_ID = os.getenv("SYNC_ORG_ID", "org_onboarding")
SYNC_BRAND_ID = os.getenv("SYNC_BRAND_ID", "noble-nectar")

# Default = every 30 minutes
SYNC_INTERVAL_SECONDS = int(os.getenv("SYNC_INTERVAL_SECONDS", "1800"))

# Retry settings
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5


# =============================================================================
# CORE SYNC FUNCTION
# =============================================================================

def run_single_sync():
    logger.info("🚀 Starting LeafLink sync")

    headers = {}
    if SYNC_SECRET:
        headers["x-opsyn-secret"] = SYNC_SECRET

    payload = {
        "org_id": SYNC_ORG_ID,
        "brand_id": SYNC_BRAND_ID,
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            start_time = datetime.utcnow()

            response = requests.post(
                SYNC_URL,
                json=payload,
                headers=headers,
                timeout=120,
            )

            duration = (datetime.utcnow() - start_time).total_seconds()

            if response.status_code == 200:
                logger.info("✅ Sync successful (%.2fs)", duration)
                logger.info("Response: %s", response.text[:500])
                return True
            else:
                logger.warning(
                    "⚠️ Sync failed (attempt %s/%s) - status: %s",
                    attempt,
                    MAX_RETRIES,
                    response.status_code,
                )
                logger.warning("Response: %s", response.text[:500])

        except Exception as exc:
            logger.exception(
                "❌ Sync exception (attempt %s/%s): %s",
                attempt,
                MAX_RETRIES,
                exc,
            )

        if attempt < MAX_RETRIES:
            logger.info("Retrying in %s seconds...", RETRY_DELAY_SECONDS)
            time.sleep(RETRY_DELAY_SECONDS)

    logger.error("🚨 Sync failed after %s attempts", MAX_RETRIES)
    return False


# =============================================================================
# SCHEDULER LOOP
# =============================================================================

def run_scheduler():
    logger.info("===================================")
    logger.info("Opsyn Sync Scheduler Started")
    logger.info("===================================")
    logger.info("Sync URL: %s", SYNC_URL)
    logger.info("Org ID: %s", SYNC_ORG_ID)
    logger.info("Brand ID: %s", SYNC_BRAND_ID)
    logger.info("Interval: %s seconds", SYNC_INTERVAL_SECONDS)
    logger.info("===================================")

    while True:
        run_single_sync()

        logger.info("😴 Sleeping for %s seconds...", SYNC_INTERVAL_SECONDS)
        time.sleep(SYNC_INTERVAL_SECONDS)