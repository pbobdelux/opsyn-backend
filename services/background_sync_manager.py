"""
Background sync manager — singleton that tracks active LeafLink background
syncs per brand.

In-memory state is used for fast status reads; progress is also persisted to
BrandAPICredential so it survives service restarts.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import select

from database import AsyncSessionLocal
from models import BrandAPICredential

logger = logging.getLogger("background_sync_manager")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class BackgroundSyncManager:
    """
    Singleton that tracks active background syncs per brand.

    State shape per brand_id:
    {
        "pages_synced": int,
        "total_pages": int,
        "active": bool,
        "started_at": str (ISO),
        "errors": list[str],
    }
    """

    _instance: Optional["BackgroundSyncManager"] = None

    def __new__(cls) -> "BackgroundSyncManager":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._state: dict[str, dict[str, Any]] = {}
            cls._instance._tasks: dict[str, asyncio.Task] = {}
        return cls._instance

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def is_syncing(self, brand_id: str) -> bool:
        """Return True if a background sync task is currently active."""
        entry = self._state.get(brand_id)
        if not entry:
            return False
        task = self._tasks.get(brand_id)
        if task and not task.done():
            return True
        # Task finished — mark inactive
        if entry.get("active"):
            entry["active"] = False
        return False

    def get_status(self, brand_id: str) -> dict[str, Any]:
        """Return current in-memory progress for a brand."""
        entry = self._state.get(brand_id)
        if not entry:
            return {
                "brand_id": brand_id,
                "active": False,
                "pages_synced": 0,
                "total_pages": 0,
                "started_at": None,
                "errors": [],
            }
        # Reconcile active flag with task state
        task = self._tasks.get(brand_id)
        active = bool(task and not task.done())
        return {
            "brand_id": brand_id,
            "active": active,
            "pages_synced": entry.get("pages_synced", 0),
            "total_pages": entry.get("total_pages", 0),
            "started_at": entry.get("started_at"),
            "errors": entry.get("errors", []),
        }

    def start_sync(
        self,
        brand_id: str,
        api_key: str,
        company_id: str,
        total_pages: int,
        start_page: int,
    ) -> asyncio.Task:
        """
        Spawn a background asyncio task that calls
        sync_leaflink_background_continuous() and register it here.

        Returns the created Task so callers can optionally await it.
        """
        # Cancel any existing task for this brand before starting a new one
        existing = self._tasks.get(brand_id)
        if existing and not existing.done():
            logger.info(
                "[BackgroundSyncManager] cancelling_existing_task brand=%s", brand_id
            )
            existing.cancel()

        self._state[brand_id] = {
            "pages_synced": start_page - 1,
            "total_pages": total_pages,
            "active": True,
            "started_at": _utc_now().isoformat(),
            "errors": [],
        }

        from services.leaflink_sync import sync_leaflink_background_continuous

        async def _run():
            try:
                await sync_leaflink_background_continuous(
                    brand_id=brand_id,
                    api_key=api_key,
                    company_id=company_id,
                    start_page=start_page,
                    total_pages=total_pages,
                    manager=self,
                )
            except asyncio.CancelledError:
                logger.info(
                    "[BackgroundSyncManager] task_cancelled brand=%s", brand_id
                )
            except Exception as exc:
                logger.error(
                    "[BackgroundSyncManager] task_error brand=%s error=%s",
                    brand_id,
                    exc,
                    exc_info=True,
                )
                state = self._state.get(brand_id, {})
                state.setdefault("errors", []).append(str(exc))
            finally:
                state = self._state.get(brand_id, {})
                if state:
                    state["active"] = False

        task = asyncio.create_task(_run(), name=f"bg_sync_{brand_id}")
        self._tasks[brand_id] = task
        logger.info(
            "[BackgroundSyncManager] task_started brand=%s start_page=%s total_pages=%s",
            brand_id,
            start_page,
            total_pages,
        )
        return task

    def stop_sync(self, brand_id: str) -> None:
        """Gracefully cancel the background sync task for a brand."""
        task = self._tasks.get(brand_id)
        if task and not task.done():
            task.cancel()
            logger.info("[BackgroundSyncManager] stop_requested brand=%s", brand_id)
        state = self._state.get(brand_id)
        if state:
            state["active"] = False

    def record_page_complete(
        self,
        brand_id: str,
        page_number: int,
        error: Optional[str] = None,
    ) -> None:
        """Update in-memory progress after a page batch completes."""
        state = self._state.get(brand_id)
        if state is None:
            return
        state["pages_synced"] = page_number
        if error:
            state.setdefault("errors", []).append(error)

    # ------------------------------------------------------------------
    # DB persistence helpers
    # ------------------------------------------------------------------

    async def persist_progress(
        self,
        brand_id: str,
        last_synced_page: int,
        total_pages: int,
        sync_status: str = "syncing",
        error: Optional[str] = None,
    ) -> None:
        """Write last_synced_page and sync_status to BrandAPICredential."""
        try:
            async with AsyncSessionLocal() as session:
                async with session.begin():
                    result = await session.execute(
                        select(BrandAPICredential).where(
                            BrandAPICredential.brand_id == brand_id,
                            BrandAPICredential.integration_name == "leaflink",
                        )
                    )
                    cred = result.scalar_one_or_none()
                    if cred:
                        cred.last_synced_page = last_synced_page
                        cred.total_pages_available = total_pages
                        cred.sync_status = sync_status
                        cred.last_sync_at = _utc_now()
                        if error:
                            cred.last_error = error
                        elif sync_status not in ("error", "syncing"):
                            cred.last_error = None
            logger.info(
                "[BackgroundSyncManager] progress_persisted brand=%s page=%s/%s status=%s",
                brand_id,
                last_synced_page,
                total_pages,
                sync_status,
            )
        except Exception as exc:
            logger.error(
                "[BackgroundSyncManager] persist_progress_failed brand=%s error=%s",
                brand_id,
                exc,
            )


# Module-level singleton instance
sync_manager = BackgroundSyncManager()
