"""
order_identity_audit.py — Order identity analysis and upsert key resolution.

Provides:
  - audit_order_identity(): live DB analysis of null/unknown/duplicate external IDs
  - get_order_match_key(): determines the correct conflict target for upsert

These utilities address the root cause of the 12,300 → 16,074 plateau:
orders with missing or 'unknown' external_order_id were silently colliding
on the (brand_id, external_order_id) unique constraint, causing updates
instead of inserts for distinct LeafLink orders.
"""

import logging
import uuid
from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger("order_identity_audit")


async def audit_order_identity(db: AsyncSession, brand_id: str) -> dict:
    """
    Query the live database for order identity anomalies for a given brand.

    Checks for:
      - Orders with external_order_id IS NULL
      - Orders with external_order_id = 'unknown'
      - Orders with duplicate order_number per brand (same brand, same
        order_number, different external_order_id — indicates collision risk)
      - Orders with duplicate external_order_id per brand (should be 0 due
        to UNIQUE constraint — non-zero means constraint is missing)
      - Orders with NULL order_number

    Returns a dict with all counts and up to 5 sample rows per category.
    Logs [ORDER_IDENTITY_AUDIT] with a summary.

    Args:
        db:       AsyncSession to use for queries.
        brand_id: Brand UUID string.

    Returns:
        {
            "null_external_order_id_count": int,
            "unknown_external_order_id_count": int,
            "duplicate_order_number_count": int,
            "duplicate_external_order_id_count": int,
            "null_order_number_count": int,
            "null_external_order_id_samples": list[dict],
            "unknown_external_order_id_samples": list[dict],
            "duplicate_order_number_samples": list[dict],
        }
    """
    result: dict = {
        "null_external_order_id_count": 0,
        "unknown_external_order_id_count": 0,
        "duplicate_order_number_count": 0,
        "duplicate_external_order_id_count": 0,
        "null_order_number_count": 0,
        "null_external_order_id_samples": [],
        "unknown_external_order_id_samples": [],
        "duplicate_order_number_samples": [],
    }

    try:
        # 1. Count orders with external_order_id IS NULL
        null_ext_res = await db.execute(
            text(
                "SELECT COUNT(*) FROM orders"
                " WHERE brand_id::text = :brand_id"
                " AND external_order_id IS NULL"
            ),
            {"brand_id": brand_id},
        )
        result["null_external_order_id_count"] = null_ext_res.scalar() or 0

        # 2. Count orders with external_order_id = 'unknown'
        unknown_ext_res = await db.execute(
            text(
                "SELECT COUNT(*) FROM orders"
                " WHERE brand_id::text = :brand_id"
                " AND external_order_id = 'unknown'"
            ),
            {"brand_id": brand_id},
        )
        result["unknown_external_order_id_count"] = unknown_ext_res.scalar() or 0

        # 3. Count orders with duplicate order_number per brand
        # (same brand, same order_number, different external_order_id)
        dup_order_num_res = await db.execute(
            text(
                """
                SELECT COUNT(*) FROM (
                    SELECT order_number
                    FROM orders
                    WHERE brand_id::text = :brand_id
                      AND order_number IS NOT NULL
                    GROUP BY order_number
                    HAVING COUNT(DISTINCT external_order_id) > 1
                ) AS dups
                """
            ),
            {"brand_id": brand_id},
        )
        result["duplicate_order_number_count"] = dup_order_num_res.scalar() or 0

        # 4. Count orders with duplicate external_order_id per brand
        # (should be 0 — UNIQUE constraint enforces this)
        dup_ext_id_res = await db.execute(
            text(
                """
                SELECT COUNT(*) FROM (
                    SELECT external_order_id
                    FROM orders
                    WHERE brand_id::text = :brand_id
                      AND external_order_id IS NOT NULL
                    GROUP BY external_order_id
                    HAVING COUNT(*) > 1
                ) AS dups
                """
            ),
            {"brand_id": brand_id},
        )
        result["duplicate_external_order_id_count"] = dup_ext_id_res.scalar() or 0

        # 5. Count orders with NULL order_number
        null_order_num_res = await db.execute(
            text(
                "SELECT COUNT(*) FROM orders"
                " WHERE brand_id::text = :brand_id"
                " AND order_number IS NULL"
            ),
            {"brand_id": brand_id},
        )
        result["null_order_number_count"] = null_order_num_res.scalar() or 0

        # 6. Sample rows with NULL external_order_id (up to 5)
        null_samples_res = await db.execute(
            text(
                "SELECT id, order_number, status, created_at"
                " FROM orders"
                " WHERE brand_id::text = :brand_id"
                " AND external_order_id IS NULL"
                " ORDER BY created_at DESC"
                " LIMIT 5"
            ),
            {"brand_id": brand_id},
        )
        result["null_external_order_id_samples"] = [
            {
                "id": str(row[0]),
                "order_number": row[1],
                "status": row[2],
                "created_at": row[3].isoformat() if row[3] and hasattr(row[3], "isoformat") else str(row[3]),
            }
            for row in null_samples_res.fetchall()
        ]

        # 7. Sample rows with external_order_id = 'unknown' (up to 5)
        unknown_samples_res = await db.execute(
            text(
                "SELECT id, order_number, status, created_at"
                " FROM orders"
                " WHERE brand_id::text = :brand_id"
                " AND external_order_id = 'unknown'"
                " ORDER BY created_at DESC"
                " LIMIT 5"
            ),
            {"brand_id": brand_id},
        )
        result["unknown_external_order_id_samples"] = [
            {
                "id": str(row[0]),
                "order_number": row[1],
                "status": row[2],
                "created_at": row[3].isoformat() if row[3] and hasattr(row[3], "isoformat") else str(row[3]),
            }
            for row in unknown_samples_res.fetchall()
        ]

        # 8. Sample duplicate order_number groups (up to 5 order_numbers)
        dup_samples_res = await db.execute(
            text(
                """
                SELECT order_number, COUNT(DISTINCT external_order_id) AS ext_id_count,
                       MIN(external_order_id) AS sample_ext_id_1,
                       MAX(external_order_id) AS sample_ext_id_2
                FROM orders
                WHERE brand_id::text = :brand_id
                  AND order_number IS NOT NULL
                GROUP BY order_number
                HAVING COUNT(DISTINCT external_order_id) > 1
                ORDER BY ext_id_count DESC
                LIMIT 5
                """
            ),
            {"brand_id": brand_id},
        )
        result["duplicate_order_number_samples"] = [
            {
                "order_number": row[0],
                "distinct_external_id_count": row[1],
                "sample_external_id_1": row[2],
                "sample_external_id_2": row[3],
            }
            for row in dup_samples_res.fetchall()
        ]

    except Exception as exc:
        logger.error(
            "[ORDER_IDENTITY_AUDIT] query_error brand=%s error=%s",
            brand_id,
            str(exc)[:300],
        )
        # Return partial results — don't crash the caller
        return result

    logger.info(
        "[ORDER_IDENTITY_AUDIT] brand=%s null_external_id=%d unknown_external_id=%d"
        " duplicate_order_number=%d duplicate_external_id=%d null_order_number=%d",
        brand_id,
        result["null_external_order_id_count"],
        result["unknown_external_order_id_count"],
        result["duplicate_order_number_count"],
        result["duplicate_external_order_id_count"],
        result["null_order_number_count"],
    )

    return result


def get_order_match_key(
    external_order_id: Optional[str],
    order_number: Optional[str],
    brand_id: str,
) -> tuple[str, str]:
    """
    Determine the correct conflict target for an order upsert.

    Priority:
      1. external_order_id present and not 'unknown'
         → match_key_type = "external_id"
         → conflict target: (brand_id, external_order_id)
      2. order_number present (fallback when external_id is missing/unknown)
         → match_key_type = "order_number"
         → conflict target: (brand_id, order_number)
      3. Neither present
         → match_key_type = "generated_uuid"
         → conflict target: (brand_id, <new UUID>) — always inserts, never matches

    Args:
        external_order_id: LeafLink order PK (may be None or 'unknown').
        order_number:       Human-readable order number (may be None).
        brand_id:           Brand UUID string (for logging only).

    Returns:
        (match_key_type, match_key_value) tuple where:
          - match_key_type  is one of "external_id", "order_number", "generated_uuid"
          - match_key_value is the resolved key value to use in the conflict clause
    """
    # Case 1: valid external_order_id
    if external_order_id and external_order_id.strip() and external_order_id != "unknown":
        return ("external_id", external_order_id)

    # Case 2: fallback to order_number
    if order_number and order_number.strip():
        logger.debug(
            "[ORDER_MATCH_DECISION] brand=%s external_id_missing=true"
            " order_number_present=true match_key_type=order_number",
            brand_id,
        )
        return ("order_number", order_number)

    # Case 3: safe fallback — generate a UUID that cannot match any existing row
    safe_id = str(uuid.uuid4())
    logger.warning(
        "[ORDER_MATCH_DECISION] brand=%s external_id_missing=true"
        " order_number_missing=true match_key_type=generated_uuid safe_id=%s",
        brand_id,
        safe_id,
    )
    return ("generated_uuid", safe_id)
