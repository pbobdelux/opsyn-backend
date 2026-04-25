import logging
import os
from typing import Any, Optional
import httpx
import json
import uuid
from datetime import datetime, timezone

logger = logging.getLogger("twin_voice_service")

TWIN_BRAIN_WEBHOOK_URL = os.getenv("TWIN_BRAIN_WEBHOOK_URL", "").strip()
TWIN_BRAIN_WEBHOOK_SECRET = os.getenv("TWIN_BRAIN_WEBHOOK_SECRET", "").strip()
TWIN_VOICE_BRAIN_AGENT_ID = os.getenv("TWIN_VOICE_BRAIN_AGENT_ID", "").strip()

# In-memory store for async callback results (can be moved to Redis/Postgres later)
TWIN_CALLBACK_RESULTS = {}


class TwinVoiceService:
    """Service for interacting with Twin Voice Brain via webhook (sync or async)."""

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
        """Send a message to Twin Voice Brain via webhook (supports sync and async modes)."""
        if not self.configured:
            logger.error(
                "twin_voice: not configured webhook_url=%s webhook_secret=%s",
                bool(self.webhook_url),
                bool(self.webhook_secret),
            )
            raise RuntimeError("Twin Voice Brain webhook not configured")

        try:
            # Generate correlation ID for async callback tracking
            correlation_id = str(uuid.uuid4())

            # Build the payload for Twin
            payload = {
                "agent_id": self.agent_id,
                "message": message,
                "tenant_org_id": tenant_org_id,
                "correlation_id": correlation_id,
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
                "twin_voice: send_message_via_webhook tenant_org_id=%s message_length=%s correlation_id=%s",
                tenant_org_id,
                len(message),
                correlation_id[:8] + "...",
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

                # Log response details (safe - no secrets)
                content_type = response.headers.get("content-type", "unknown")
                response_body = response.text[:1000]  # First 1000 chars

                logger.info(
                    "twin_voice: webhook_response status=%s content_type=%s body_length=%s",
                    response.status_code,
                    content_type,
                    len(response.text),
                )

                logger.debug(
                    "twin_voice: webhook_response_preview status=%s body=%s",
                    response.status_code,
                    response_body,
                )

                # Handle async mode (202 Accepted)
                if response.status_code == 202:
                    logger.info("twin_voice: async_mode correlation_id=%s", correlation_id[:8] + "...")
                    return {
                        "ok": True,
                        "status": "pending",
                        "kind": "pending",
                        "speak_text": "I'm checking that now.",
                        "display_text": "Processing your request...",
                        "data": {},
                        "correlation_id": correlation_id,
                        "twin_status_code": response.status_code,
                        "twin_content_type": content_type,
                    }

                # Handle sync mode (200/201 with JSON response)
                if response.status_code in (200, 201):
                    # Try to parse JSON
                    if "application/json" in content_type:
                        try:
                            data = response.json()
                            logger.info("twin_voice: sync_mode_json_response")
                            return {
                                "ok": True,
                                "status": "completed",
                                "output": data,
                                "error": None,
                                "twin_status_code": response.status_code,
                                "twin_content_type": content_type,
                            }
                        except json.JSONDecodeError as e:
                            logger.error(
                                "twin_voice: json_parse_failed error=%s body=%s",
                                e,
                                response_body,
                            )
                            # Return pending if JSON parse fails
                            return {
                                "ok": True,
                                "status": "pending",
                                "kind": "pending",
                                "speak_text": "I'm processing that.",
                                "display_text": "Processing your request...",
                                "data": {},
                                "twin_status_code": response.status_code,
                                "twin_content_type": content_type,
                                "twin_response_preview": response_body[:200],
                            }
                    else:
                        # Non-JSON response (HTML, plain text, etc.)
                        logger.warning(
                            "twin_voice: non_json_response content_type=%s",
                            content_type,
                        )
                        return {
                            "ok": True,
                            "status": "pending",
                            "kind": "pending",
                            "speak_text": "I'm checking that now.",
                            "display_text": "Processing your request...",
                            "data": {},
                            "twin_status_code": response.status_code,
                            "twin_content_type": content_type,
                            "twin_response_preview": response_body[:200],
                        }

                # Handle error responses
                if response.status_code >= 400:
                    logger.error(
                        "twin_voice: webhook_error status=%s body=%s",
                        response.status_code,
                        response_body,
                    )
                    raise RuntimeError(f"Twin webhook error: {response.status_code}")

                # Unexpected status code
                logger.warning(
                    "twin_voice: unexpected_status status=%s",
                    response.status_code,
                )
                return {
                    "ok": True,
                    "status": "pending",
                    "kind": "pending",
                    "speak_text": "I'm processing that.",
                    "display_text": "Processing your request...",
                    "data": {},
                    "twin_status_code": response.status_code,
                    "twin_content_type": content_type,
                }

        except Exception as e:
            logger.error("twin_voice: send_message_failed error=%s", e, exc_info=True)
            raise

    def store_callback_result(self, correlation_id: str, result: dict[str, Any]) -> None:
        """Store callback result for async mode."""
        logger.info("twin_voice: storing_callback_result correlation_id=%s", correlation_id[:8] + "...")

        # Extract the fields we need from the callback
        speak_text = result.get("speak_text", "")
        display_text = result.get("display_text", "")
        kind = result.get("kind", "answer")
        data = result.get("data", {})
        error = result.get("error")

        # Store the result
        TWIN_CALLBACK_RESULTS[correlation_id] = {
            "ok": True,
            "kind": kind,
            "speak_text": speak_text,
            "display_text": display_text,
            "data": data,
            "error": error,
            "received_at": datetime.now(timezone.utc).isoformat(),
            "raw_callback": result,  # Store full callback for debugging
        }

        logger.info(
            "twin_voice: callback_stored correlation_id=%s kind=%s",
            correlation_id[:8] + "...",
            kind,
        )

    def get_callback_result(self, correlation_id: str) -> Optional[dict[str, Any]]:
        """Retrieve callback result for async mode."""
        if correlation_id in TWIN_CALLBACK_RESULTS:
            logger.info("twin_voice: retrieving_callback_result correlation_id=%s", correlation_id[:8] + "...")
            return TWIN_CALLBACK_RESULTS[correlation_id]
        return None

    def is_healthy(self) -> bool:
        """Check if Twin Voice Brain webhook is configured."""
        return self.configured


# Global service instance
twin_voice_service = TwinVoiceService()
