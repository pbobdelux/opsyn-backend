import logging
import os
from typing import Any, Optional
import httpx
import json

logger = logging.getLogger("twin_voice_service")

TWIN_BRAIN_WEBHOOK_URL = os.getenv("TWIN_BRAIN_WEBHOOK_URL", "").strip()
TWIN_BRAIN_WEBHOOK_SECRET = os.getenv("TWIN_BRAIN_WEBHOOK_SECRET", "").strip()
TWIN_VOICE_BRAIN_AGENT_ID = os.getenv("TWIN_VOICE_BRAIN_AGENT_ID", "").strip()


class TwinVoiceService:
    """Service for interacting with Twin Voice Brain via webhook."""

    def __init__(self):
        self.webhook_url = TWIN_BRAIN_WEBHOOK_URL
        self.webhook_secret = TWIN_BRAIN_WEBHOOK_SECRET
        self.agent_id = TWIN_VOICE_BRAIN_AGENT_ID
        self.configured = bool(self.webhook_url and self.webhook_secret)

    async def send_message(
        self,
        message: str,
        tenant_org_id: str,
        context: Optional[dict[str, Any]] = None,
        opsyn_backend_url: str = "",
    ) -> dict[str, Any]:
        """Send a message to Twin Voice Brain via webhook."""
        if not self.configured:
            logger.error("twin_voice: not configured webhook_url=%s webhook_secret=%s", 
                        bool(self.webhook_url), bool(self.webhook_secret))
            raise RuntimeError("Twin Voice Brain webhook not configured")

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
                "twin_voice: send_message_via_webhook tenant_org_id=%s message_length=%s",
                tenant_org_id,
                len(message),
            )

            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self.webhook_url,
                    headers={
                        "X-Twin-Secret": self.webhook_secret,
                        "Content-Type": "application/json",
                    },
                    json=payload,
                    timeout=60,
                )

                if response.status_code not in (200, 201, 202):
                    logger.error(
                        "twin_voice: webhook_failed status=%s body=%s",
                        response.status_code,
                        response.text[:200],
                    )
                    raise RuntimeError(f"Twin webhook error: {response.status_code}")

                data = response.json()

                logger.info("twin_voice: webhook_response_received status=%s", response.status_code)

                # Return the response directly (Twin webhook returns the result)
                return {
                    "ok": response.status_code in (200, 201, 202),
                    "status": "completed",
                    "output": data,
                    "error": None,
                }

        except Exception as e:
            logger.error("twin_voice: send_message_failed error=%s", e, exc_info=True)
            raise

    def is_healthy(self) -> bool:
        """Check if Twin Voice Brain webhook is configured."""
        return self.configured


# Global service instance
twin_voice_service = TwinVoiceService()
