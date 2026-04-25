import logging
import os
from typing import Any, Optional
from datetime import datetime, timezone, timedelta
import httpx

logger = logging.getLogger("twin_voice_service")

TWIN_API_BASE = os.getenv("TWIN_API_BASE", "").strip()
TWIN_API_KEY = os.getenv("TWIN_API_KEY", "").strip()
TWIN_VOICE_BRAIN_AGENT_ID = os.getenv("TWIN_VOICE_BRAIN_AGENT_ID", "").strip()


class TwinVoiceService:
    """Service for interacting with Twin Voice Brain agent."""

    def __init__(self):
        self.api_base = TWIN_API_BASE
        self.api_key = TWIN_API_KEY
        self.agent_id = TWIN_VOICE_BRAIN_AGENT_ID
        self.configured = bool(self.api_base and self.api_key and self.agent_id)

    async def send_message(
        self,
        message: str,
        tenant_org_id: str,
        context: Optional[dict[str, Any]] = None,
        opsyn_backend_url: str = "",
    ) -> dict[str, Any]:
        """Send a message to Twin Voice Brain agent."""
        if not self.configured:
            logger.error("twin_voice: not configured")
            raise RuntimeError("Twin Voice Brain not configured")

        try:
            # Build the payload for Twin
            payload = {
                "agent_id": self.agent_id,
                "message": message,
                "tenant_org_id": tenant_org_id,
                "context": context or {},
                "opsyn_backend_url": opsyn_backend_url,
                "opsyn_endpoints": {
                    "dashboard": "/crm/dashboard",
                    "customers": "/crm/customers",
                    "customer_detail": "/crm/customers/{customer_id}",
                    "customer_orders": "/crm/customers/{customer_id}/orders",
                    "customer_activity": "/crm/customers/{customer_id}/activity",
                    "recent_orders": "/crm/recent-orders",
                    "sync_status": "/crm/sync-status",
                },
            }

            logger.info(
                "twin_voice: send_message agent_id=%s tenant_org_id=%s message_length=%s",
                self.agent_id[:8] + "...",
                tenant_org_id,
                len(message),
            )

            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.api_base}/v1/agents/{self.agent_id}/runs",
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                    timeout=60,
                )

                if response.status_code not in (200, 201):
                    logger.error(
                        "twin_voice: send_message failed status=%s body=%s",
                        response.status_code,
                        response.text[:200],
                    )
                    raise RuntimeError(f"Twin API error: {response.status_code}")

                data = response.json()
                run_id = data.get("id")

                if not run_id:
                    logger.error("twin_voice: send_message no run_id in response")
                    raise RuntimeError("No run_id in Twin response")

                logger.info("twin_voice: run_created run_id=%s", run_id[:8] + "...")

                # Poll for completion
                result = await self._poll_run(run_id)
                return result

        except Exception as e:
            logger.error("twin_voice: send_message_failed error=%s", e, exc_info=True)
            raise

    async def _poll_run(self, run_id: str, timeout_seconds: int = 60) -> dict[str, Any]:
        """Poll Twin run until completion."""
        start_time = datetime.now(timezone.utc)
        timeout = timedelta(seconds=timeout_seconds)

        try:
            async with httpx.AsyncClient() as client:
                while True:
                    elapsed = datetime.now(timezone.utc) - start_time
                    if elapsed > timeout:
                        logger.error("twin_voice: poll_run timeout run_id=%s", run_id[:8] + "...")
                        raise RuntimeError("Twin run polling timeout")

                    response = await client.get(
                        f"{self.api_base}/v1/agents/{self.agent_id}/runs/{run_id}",
                        headers={
                            "Authorization": f"Bearer {self.api_key}",
                        },
                        timeout=10,
                    )

                    if response.status_code != 200:
                        logger.error(
                            "twin_voice: poll_run failed status=%s",
                            response.status_code,
                        )
                        raise RuntimeError(f"Twin API error: {response.status_code}")

                    data = response.json()
                    status = data.get("status")

                    logger.info("twin_voice: poll_run status=%s run_id=%s", status, run_id[:8] + "...")

                    if status in ("completed", "failed", "stopped"):
                        return {
                            "ok": status == "completed",
                            "run_id": run_id,
                            "status": status,
                            "output": data.get("output", {}),
                            "error": data.get("error"),
                        }

                    # Wait before polling again
                    import asyncio
                    await asyncio.sleep(0.5)

        except Exception as e:
            logger.error("twin_voice: poll_run_failed error=%s", e, exc_info=True)
            raise

    def is_healthy(self) -> bool:
        """Check if Twin Voice Brain is configured."""
        return self.configured


# Global service instance
twin_voice_service = TwinVoiceService()
