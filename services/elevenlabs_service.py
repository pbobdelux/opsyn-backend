import logging
import os
from typing import Any, Optional

import httpx

logger = logging.getLogger("elevenlabs_service")

ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY", "").strip()
ELEVENLABS_AGENT_ID = os.getenv("ELEVENLABS_AGENT_ID", "").strip()
ELEVENLABS_BASE_URL = "https://api.elevenlabs.io/v1"


class ElevenLabsService:
    """Service for interacting with ElevenLabs conversational AI."""

    def __init__(self):
        self.api_key = ELEVENLABS_API_KEY
        self.agent_id = ELEVENLABS_AGENT_ID
        self.base_url = ELEVENLABS_BASE_URL
        self.configured = bool(self.api_key and self.agent_id)

    async def get_conversation_token(self) -> dict[str, Any]:
        """Get a conversation token for the agent (WebRTC connection)."""
        if not self.configured:
            logger.error("elevenlabs: not configured")
            raise RuntimeError("ElevenLabs not configured")

        try:
            url = f"{self.base_url}/convai/conversation/token"
            params = {"agent_id": self.agent_id}

            logger.info("elevenlabs: get_conversation_token url=%s agent_id=%s", url, self.agent_id[:8] + "...")

            async with httpx.AsyncClient() as client:
                response = await client.get(
                    url,
                    params=params,
                    headers={
                        "xi-api-key": self.api_key,
                    },
                    timeout=10,
                )

                logger.info(
                    "elevenlabs: get_conversation_token response status=%s",
                    response.status_code,
                )

                if response.status_code != 200:
                    response_body = response.text[:500]  # Truncate to 500 chars
                    logger.error(
                        "elevenlabs: get_conversation_token failed status=%s body=%s",
                        response.status_code,
                        response_body,
                    )

                    # Return error details for debugging
                    raise RuntimeError(
                        f"ElevenLabs API error: status={response.status_code}, body={response_body}"
                    )

                data = response.json()
                token = data.get("token")

                if not token:
                    logger.error("elevenlabs: get_conversation_token no token in response data=%s", data)
                    raise RuntimeError("No token in ElevenLabs response")

                logger.info("elevenlabs: conversation_token_obtained token_length=%s", len(token))
                return {
                    "token": token,
                    "agent_id": self.agent_id,
                }

        except Exception as e:
            logger.error("elevenlabs: get_conversation_token_failed error=%s", e, exc_info=True)
            raise

    async def send_message(
        self,
        conversation_id: str,
        message: str,
    ) -> dict[str, Any]:
        """Send a message to the agent and get a response."""
        if not self.configured:
            logger.error("elevenlabs: not configured")
            raise RuntimeError("ElevenLabs not configured")

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.base_url}/convai/conversation/{conversation_id}/message",
                    headers={
                        "xi-api-key": self.api_key,
                        "Content-Type": "application/json",
                    },
                    json={
                        "message": message,
                    },
                    timeout=30,
                )

                if response.status_code != 200:
                    logger.error(
                        "elevenlabs: send_message failed status=%s body=%s",
                        response.status_code,
                        response.text[:200],
                    )
                    raise RuntimeError(f"ElevenLabs API error: {response.status_code}")

                data = response.json()
                logger.info(
                    "elevenlabs: message_sent conversation_id=%s response_length=%s",
                    conversation_id,
                    len(data.get("response", "")),
                )
                return data

        except Exception as e:
            logger.error("elevenlabs: send_message_failed conversation_id=%s error=%s", conversation_id, e)
            raise

    def is_healthy(self) -> bool:
        """Check if ElevenLabs service is configured."""
        return self.configured


# Global service instance
elevenlabs_service = ElevenLabsService()
