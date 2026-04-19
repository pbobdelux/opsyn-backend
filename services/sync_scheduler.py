import os
import time
import logging
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)

logger = logging.getLogger(__name__)

SYNC_INTERVAL_SECONDS = int(os.getenv("SYNC_INTERVAL_SECONDS", "300"))

def run_scheduler():
    backend_url = os.getenv("BACKEND_URL")
    if not backend_url:
        raise RuntimeError("Missing BACKEND_URL environment variable")

    sync_url = f"{backend_url.rstrip('/')}/sync/leaflink"

    logger.info("Sync scheduler started")
    logger.info("Using sync URL: %s", sync_url)
    logger.info("Interval: %s seconds", SYNC_INTERVAL_SECONDS)

    while True:
        try:
            logger.info("Starting LeafLink sync")
            response = requests.post(sync_url, timeout=120)
            logger.info("Sync response: %s %s", response.status_code, response.text)
        except Exception as e:
            logger.exception("Sync failed: %s", e)

        logger.info("Sleeping for %s seconds", SYNC_INTERVAL_SECONDS)
        time.sleep(SYNC_INTERVAL_SECONDS)