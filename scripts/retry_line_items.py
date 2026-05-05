"""
retry_line_items.py — Safe backfill/retry for OrderLine rows.

Finds orders that have line_items_json stored but are missing OrderLine rows,
then re-inserts them.  Idempotent: uses DELETE + INSERT (not UPSERT) so
duplicate rows are never created.

Usage:
    python scripts/retry_line_items.py --brand-id <uuid> [--limit 100]

Environment variables:
    DATABASE_URL  — PostgreSQL connection string (required, same as main app)

Exit codes:
    0  — completed (even if some orders failed; check the summary log)
    1  — fatal startup error (missing DATABASE_URL, import failure, etc.)
"""

import argparse
import asyncio
import logging
import os
import sys
import time

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
logger = logging.getLogger("retry_line_items")


async def retry_line_items_for_orders(brand_id: str, limit: int = 100) -> dict:
    """
    Find orders with line_items_json but no OrderLine rows and retry inserting them.

    Idempotent: deletes any stale OrderLine rows before re-inserting, so running
    this script multiple times is safe.

    Returns:
        {
            "orders_processed": int,
            "line_items_inserted": int,
            "line_items_failed": int,
            "errors": list[str],
        }
    """
    from sqlalchemy import func, select

    from database import AsyncSessionLocal
    from models import Order, OrderLine
    from services.leaflink_sync import (
        _insert_line_items_standalone,
        normalize_line_items,
        safe_uuid,
    )

    brand_id_value = safe_uuid(brand_id)

    logger.info(
        "[RetryLineItems] starting brand_id=%s limit=%s",
        brand_id_value,
        limit,
    )

    # ------------------------------------------------------------------
    # Step 1: Find orders that have line_items_json but no OrderLine rows
    # ------------------------------------------------------------------
    async with AsyncSessionLocal() as db:
        # Subquery: order IDs that already have at least one OrderLine
        has_lines_subq = (
            select(OrderLine.order_id)
            .where(OrderLine.order_id == Order.id)
            .correlate(Order)
            .exists()
        )

        result = await db.execute(
            select(Order)
            .where(
                Order.brand_id == brand_id_value,
                Order.line_items_json.isnot(None),
                ~has_lines_subq,
            )
            .limit(limit)
        )
        orders = result.scalars().all()

    logger.info(
        "[RetryLineItems] found %s orders with missing line items brand_id=%s",
        len(orders),
        brand_id_value,
    )

    if not orders:
        logger.info("[RetryLineItems] nothing to do — all orders have line items")
        return {
            "orders_processed": 0,
            "line_items_inserted": 0,
            "line_items_failed": 0,
            "errors": [],
        }

    total_inserted = 0
    total_failed = 0
    errors: list[str] = []

    # ------------------------------------------------------------------
    # Step 2: For each order, delete stale lines and re-insert from JSON
    # ------------------------------------------------------------------
    for order in orders:
        order_id = order.id
        external_id = order.external_order_id
        line_items_json = order.line_items_json

        if not isinstance(line_items_json, list):
            logger.warning(
                "[RetryLineItems] order_id=%s external_id=%s — line_items_json is not a list (%s), skipping",
                order_id,
                external_id,
                type(line_items_json).__name__,
            )
            continue

        normalized = normalize_line_items(line_items_json)
        if not normalized:
            logger.warning(
                "[RetryLineItems] order_id=%s external_id=%s — no normalized line items, skipping",
                order_id,
                external_id,
            )
            continue

        try:
            async with AsyncSessionLocal() as db:
                async with db.begin():
                    inserted, skipped, failed = await _insert_line_items_standalone(
                        db, order_id, normalized
                    )

            logger.info(
                "[RetryLineItems] order_id=%s external_id=%s inserted=%s skipped=%s failed=%s",
                order_id,
                external_id,
                inserted,
                skipped,
                failed,
            )
            total_inserted += inserted
            total_failed += failed

        except Exception as exc:
            err_msg = f"order_id={order_id} external_id={external_id} error={str(exc)[:300]}"
            logger.error("[RetryLineItems] FAILED %s", err_msg, exc_info=True)
            errors.append(err_msg)

    return {
        "orders_processed": len(orders),
        "line_items_inserted": total_inserted,
        "line_items_failed": total_failed,
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Retry inserting OrderLine rows for orders that are missing them."
    )
    parser.add_argument(
        "--brand-id",
        required=True,
        help="Brand UUID to process",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of orders to process (default: 100)",
    )
    return parser.parse_args()


async def _main() -> None:
    args = _parse_args()
    run_start = time.monotonic()

    result = await retry_line_items_for_orders(
        brand_id=args.brand_id,
        limit=args.limit,
    )

    elapsed = round(time.monotonic() - run_start, 2)

    print("")
    print("=" * 60)
    print("  retry_line_items complete")
    print("=" * 60)
    print(f"  Brand ID          : {args.brand_id}")
    print(f"  Orders processed  : {result['orders_processed']}")
    print(f"  Line items inserted: {result['line_items_inserted']}")
    print(f"  Line items failed : {result['line_items_failed']}")
    print(f"  Errors            : {len(result['errors'])}")
    print(f"  Duration          : {elapsed}s")
    print("=" * 60)

    if result["errors"]:
        print("\nErrors:")
        for err in result["errors"]:
            print(f"  - {err}")

    logger.info(
        "[RetryLineItems] summary brand_id=%s orders=%s inserted=%s failed=%s errors=%s duration=%ss",
        args.brand_id,
        result["orders_processed"],
        result["line_items_inserted"],
        result["line_items_failed"],
        len(result["errors"]),
        elapsed,
    )


if __name__ == "__main__":
    if not os.getenv("DATABASE_URL"):
        logger.error("[RetryLineItems] DATABASE_URL environment variable is not set — aborting")
        sys.exit(1)

    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        logger.info("[RetryLineItems] interrupted by user")
        sys.exit(0)
    except Exception as fatal:
        logger.error("[RetryLineItems] fatal error: %s", fatal, exc_info=True)
        sys.exit(1)
