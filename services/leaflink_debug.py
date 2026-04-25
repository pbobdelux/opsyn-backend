import logging
import os

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger("leaflink_debug")


async def get_debug_info(db: AsyncSession) -> dict:
    """Return diagnostic information about the LeafLink integration."""
    info: dict = {
        "ok": True,
        "env": {
            "LEAFLINK_API_KEY_set": bool(os.getenv("LEAFLINK_API_KEY", "").strip()),
            "LEAFLINK_BASE_URL": os.getenv("LEAFLINK_BASE_URL", "https://app.leaflink.com/api/v2"),
            "LEAFLINK_COMPANY_ID_set": bool(os.getenv("LEAFLINK_COMPANY_ID", "").strip()),
            "MOCK_MODE": os.getenv("MOCK_MODE", "false").strip().lower() == "true",
        },
        "database": {},
    }

    try:
        result = await db.execute(text("SELECT COUNT(*) FROM orders"))
        row = result.scalar_one_or_none()
        info["database"]["orders_count"] = row if row is not None else 0
        info["database"]["connected"] = True
    except Exception as exc:
        logger.warning("leaflink_debug: db check failed: %s", exc)
        info["database"]["connected"] = False
        info["database"]["error"] = str(exc)

    return info
