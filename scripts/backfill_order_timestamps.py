"""
backfill_order_timestamps.py — One-time backfill for external_created_at / external_updated_at.

Orders synced before these columns were added have NULL values, which breaks
sorting and filtering logic that relies on these timestamps.

This script:
  1. Queries all orders where external_created_at IS NULL, grouped by brand_id.
  2. For each brand, looks up the active LeafLink BrandAPICredential.
  3. Fetches each order's detail from LeafLink using its external_order_id.
  4. Extracts created_at / modified timestamps via normalize_datetime().
  5. Updates the order row with the recovered timestamps.
  6. Processes in batches of BATCH_SIZE to avoid long-running transactions.

Usage:
    python scripts/backfill_order_timestamps.py

Environment variables:
    DATABASE_URL  — PostgreSQL connection string (required, same as main app)

Exit codes:
    0  — completed (even if some orders failed; check the summary log)
    1  — fatal startup error (missing DATABASE_URL, import failure, etc.)
"""

import asyncio
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Optional

# ---------------------------------------------------------------------------
# Path bootstrap — the script lives in scripts/ which is one level below the
# repo root where all shared modules (database, models, services) live.
# ---------------------------------------------------------------------------
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

# ---------------------------------------------------------------------------
# Logging — configure before any imports that may emit log lines at import time
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("backfill_order_timestamps")

# ---------------------------------------------------------------------------
# Batch size — mirrors HEADER_BATCH_SIZE used by the main sync worker
# ---------------------------------------------------------------------------
BATCH_SIZE = 25


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: Optional[datetime]) -> str:
    return dt.isoformat() if dt else "None"


# ---------------------------------------------------------------------------
# Core backfill logic
# ---------------------------------------------------------------------------

async def backfill() -> None:
    """Main entry point — runs the full backfill and prints a summary."""

    # Deferred imports so that the path bootstrap above runs first and the
    # database module can validate DATABASE_URL before we proceed.
    from sqlalchemy import select, text

    from database import AsyncSessionLocal
    from models import BrandAPICredential, Order
    from services.leaflink_client import LeafLinkClient
    from services.leaflink_sync import normalize_datetime

    run_start = time.monotonic()
    logger.info("[Backfill] starting backfill of external_created_at / external_updated_at")

    # ------------------------------------------------------------------
    # Step 1: Find all distinct brand_ids that have NULL-timestamp orders
    # ------------------------------------------------------------------
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Order.brand_id)
            .where(Order.external_created_at.is_(None))
            .distinct()
        )
        brand_ids: list[str] = [row[0] for row in result.fetchall()]

    if not brand_ids:
        logger.info("[Backfill] no orders with NULL external_created_at — nothing to do")
        return

    logger.info("[Backfill] found %s brand(s) with NULL-timestamp orders: %s", len(brand_ids), brand_ids)

    # Counters across all brands
    total_processed = 0
    total_updated = 0
    total_failed = 0
    total_skipped = 0  # orders skipped due to missing creds or no timestamps returned

    # ------------------------------------------------------------------
    # Step 2: Process each brand
    # ------------------------------------------------------------------
    for brand_id in brand_ids:
        logger.info("[Backfill] processing brand_id=%s", brand_id)

        # ---- 2a. Fetch LeafLink credentials for this brand ----
        async with AsyncSessionLocal() as db:
            cred_result = await db.execute(
                select(BrandAPICredential).where(
                    BrandAPICredential.brand_id == brand_id,
                    BrandAPICredential.integration_name == "leaflink",
                    BrandAPICredential.is_active == True,  # noqa: E712
                )
            )
            cred = cred_result.scalar_one_or_none()

        if not cred:
            logger.warning(
                "[Backfill] no active LeafLink credentials for brand_id=%s — skipping brand",
                brand_id,
            )
            # Count how many orders we're skipping for this brand
            async with AsyncSessionLocal() as db:
                count_result = await db.execute(
                    select(Order.id).where(
                        Order.brand_id == brand_id,
                        Order.external_created_at.is_(None),
                    )
                )
                skipped_count = len(count_result.fetchall())
            total_skipped += skipped_count
            logger.warning(
                "[Backfill] skipped %s orders for brand_id=%s (no credentials)",
                skipped_count,
                brand_id,
            )
            continue

        api_key: str = cred.api_key or ""
        company_id: str = cred.company_id or ""
        base_url: str = cred.base_url or ""
        auth_scheme = cred.auth_scheme  # may be None — LeafLinkClient defaults to Api-Key

        if not api_key.strip() or not base_url.strip():
            logger.warning(
                "[Backfill] incomplete credentials for brand_id=%s (api_key_present=%s base_url_present=%s) — skipping brand",
                brand_id,
                bool(api_key.strip()),
                bool(base_url.strip()),
            )
            async with AsyncSessionLocal() as db:
                count_result = await db.execute(
                    select(Order.id).where(
                        Order.brand_id == brand_id,
                        Order.external_created_at.is_(None),
                    )
                )
                skipped_count = len(count_result.fetchall())
            total_skipped += skipped_count
            continue

        # ---- 2b. Build LeafLink client ----
        try:
            client = LeafLinkClient(
                api_key=api_key,
                base_url=base_url,
                company_id=company_id,
                brand_id=brand_id,
                auth_scheme=auth_scheme,
            )
        except Exception as client_exc:
            logger.error(
                "[Backfill] failed to build LeafLinkClient for brand_id=%s error=%s — skipping brand",
                brand_id,
                client_exc,
            )
            async with AsyncSessionLocal() as db:
                count_result = await db.execute(
                    select(Order.id).where(
                        Order.brand_id == brand_id,
                        Order.external_created_at.is_(None),
                    )
                )
                skipped_count = len(count_result.fetchall())
            total_skipped += skipped_count
            continue

        # ---- 2c. Fetch all NULL-timestamp orders for this brand ----
        async with AsyncSessionLocal() as db:
            orders_result = await db.execute(
                select(Order.id, Order.external_order_id).where(
                    Order.brand_id == brand_id,
                    Order.external_created_at.is_(None),
                )
            )
            orders_to_fix: list[tuple[int, str]] = orders_result.fetchall()

        logger.info(
            "[Backfill] brand_id=%s has %s orders with NULL timestamps",
            brand_id,
            len(orders_to_fix),
        )

        # ---- 2d. Process in batches ----
        batches = [
            orders_to_fix[i : i + BATCH_SIZE]
            for i in range(0, len(orders_to_fix), BATCH_SIZE)
        ]
        total_batches = len(batches)

        brand_updated = 0
        brand_failed = 0
        brand_skipped = 0

        for batch_num, batch in enumerate(batches, 1):
            logger.info(
                "[Backfill] brand_id=%s batch %s/%s (%s orders)",
                brand_id,
                batch_num,
                total_batches,
                len(batch),
            )

            batch_updates: list[dict] = []  # collect (order_id, ext_created, ext_updated) tuples

            # ---- Fetch timestamps from LeafLink for each order in the batch ----
            for order_id, external_order_id in batch:
                total_processed += 1

                if not external_order_id:
                    logger.warning(
                        "[Backfill] order_id=%s has no external_order_id — skipping",
                        order_id,
                    )
                    brand_skipped += 1
                    continue

                try:
                    # LeafLink order detail endpoint: orders-received/{id}/
                    # Run the synchronous HTTP call in a thread executor so we
                    # don't block the event loop during network I/O.
                    loop = asyncio.get_event_loop()
                    detail_path = f"orders-received/{external_order_id}/"

                    raw_response = await loop.run_in_executor(
                        None,
                        lambda path=detail_path: client._get_raw(path),
                    )

                    if not raw_response.ok:
                        logger.warning(
                            "[Backfill] LeafLink returned status=%s for order_id=%s external_order_id=%s — skipping",
                            raw_response.status_code,
                            order_id,
                            external_order_id,
                        )
                        brand_skipped += 1
                        continue

                    content_type = raw_response.headers.get("Content-Type", "")
                    if "application/json" not in content_type.lower():
                        logger.warning(
                            "[Backfill] non-JSON response for order_id=%s external_order_id=%s content_type=%s — skipping",
                            order_id,
                            external_order_id,
                            content_type,
                        )
                        brand_skipped += 1
                        continue

                    raw_order: dict = raw_response.json()

                    # Extract timestamps using the same field priority as sync_leaflink_orders_headers_only
                    created_raw = (
                        raw_order.get("created")
                        or raw_order.get("created_at")
                        or raw_order.get("submitted_at")
                        or raw_order.get("external_created_at")
                    )
                    modified_raw = (
                        raw_order.get("modified")
                        or raw_order.get("updated")
                        or raw_order.get("updated_at")
                        or raw_order.get("external_updated_at")
                    )

                    external_created_at = normalize_datetime(created_raw)
                    external_updated_at = normalize_datetime(modified_raw)

                    if external_created_at is None and external_updated_at is None:
                        logger.warning(
                            "[Backfill] no timestamps found in LeafLink response for order_id=%s external_order_id=%s "
                            "created_raw=%s modified_raw=%s — skipping",
                            order_id,
                            external_order_id,
                            created_raw,
                            modified_raw,
                        )
                        brand_skipped += 1
                        continue

                    logger.info(
                        "[Backfill] order_id=%s external_order_id=%s external_created_at=%s external_updated_at=%s",
                        order_id,
                        external_order_id,
                        _iso(external_created_at),
                        _iso(external_updated_at),
                    )

                    batch_updates.append({
                        "order_id": order_id,
                        "external_created_at": external_created_at,
                        "external_updated_at": external_updated_at,
                    })

                except Exception as fetch_exc:
                    logger.error(
                        "[Backfill] failed to fetch order_id=%s external_order_id=%s error=%s",
                        order_id,
                        external_order_id,
                        fetch_exc,
                        exc_info=True,
                    )
                    brand_failed += 1
                    continue

            # ---- Commit the batch of timestamp updates ----
            if batch_updates:
                try:
                    async with AsyncSessionLocal() as db:
                        async with db.begin():
                            for update in batch_updates:
                                # Convert aware datetimes to ISO strings for asyncpg
                                # (mirrors serialize_datetimes_for_sql in leaflink_sync.py)
                                ext_created_str = (
                                    update["external_created_at"].isoformat()
                                    if update["external_created_at"] is not None
                                    else None
                                )
                                ext_updated_str = (
                                    update["external_updated_at"].isoformat()
                                    if update["external_updated_at"] is not None
                                    else None
                                )
                                now_str = _utc_now().isoformat()

                                await db.execute(
                                    text(
                                        """
                                        UPDATE orders
                                        SET
                                            external_created_at = :external_created_at,
                                            external_updated_at = :external_updated_at,
                                            updated_at          = :updated_at
                                        WHERE id = :order_id
                                          AND external_created_at IS NULL
                                        """
                                    ),
                                    {
                                        "order_id": update["order_id"],
                                        "external_created_at": ext_created_str,
                                        "external_updated_at": ext_updated_str,
                                        "updated_at": now_str,
                                    },
                                )
                                brand_updated += 1

                    logger.info(
                        "[Backfill] brand_id=%s batch %s/%s committed %s updates",
                        brand_id,
                        batch_num,
                        total_batches,
                        len(batch_updates),
                    )

                except Exception as commit_exc:
                    logger.error(
                        "[Backfill] brand_id=%s batch %s/%s commit failed error=%s",
                        brand_id,
                        batch_num,
                        total_batches,
                        commit_exc,
                        exc_info=True,
                    )
                    # Count all orders in this batch as failed since the transaction rolled back
                    brand_failed += len(batch_updates)
                    brand_updated -= len(batch_updates)  # undo the optimistic count
                    continue

        logger.info(
            "[Backfill] brand_id=%s complete — updated=%s failed=%s skipped=%s",
            brand_id,
            brand_updated,
            brand_failed,
            brand_skipped,
        )

        total_updated += brand_updated
        total_failed += brand_failed
        total_skipped += brand_skipped

    # ------------------------------------------------------------------
    # Final summary
    # ------------------------------------------------------------------
    elapsed = round(time.monotonic() - run_start, 2)

    print("")
    print("=" * 60)
    print("  Backfill complete")
    print("=" * 60)
    print(f"  Brands processed : {len(brand_ids)}")
    print(f"  Orders processed : {total_processed}")
    print(f"  Updated          : {total_updated}")
    print(f"  Failed           : {total_failed}")
    print(f"  Skipped          : {total_skipped}")
    print(f"  Duration         : {elapsed}s")
    print("=" * 60)

    logger.info(
        "[Backfill] summary brands=%s processed=%s updated=%s failed=%s skipped=%s duration=%ss",
        len(brand_ids),
        total_processed,
        total_updated,
        total_failed,
        total_skipped,
        elapsed,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if not os.getenv("DATABASE_URL"):
        logger.error("[Backfill] DATABASE_URL environment variable is not set — aborting")
        sys.exit(1)

    try:
        asyncio.run(backfill())
    except KeyboardInterrupt:
        logger.info("[Backfill] interrupted by user")
        sys.exit(0)
    except Exception as fatal:
        logger.error("[Backfill] fatal error: %s", fatal, exc_info=True)
        sys.exit(1)
