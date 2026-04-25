import logging
import os
from typing import Any, Optional
import httpx
import base64

logger = logging.getLogger("elevenlabs_stt_tts")

ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY", "").strip()
ELEVENLABS_TTS_VOICE_ID = os.getenv("ELEVENLABS_TTS_VOICE_ID", "").strip()
ELEVENLABS_STT_MODEL = os.getenv("ELEVENLABS_STT_MODEL", "scribe_v1").strip()
ELEVENLABS_BASE_URL = "https://api.elevenlabs.io/v1"


class ElevenLabsSTTTTSService:
    """Service for ElevenLabs STT and TTS."""

    def __init__(self):
        self.api_key = ELEVENLABS_API_KEY
        self.tts_voice_id = ELEVENLABS_TTS_VOICE_ID
        self.stt_model = ELEVENLABS_STT_MODEL
        self.base_url = ELEVENLABS_BASE_URL
        self.stt_configured = bool(self.api_key)
        self.tts_configured = bool(self.api_key and self.tts_voice_id)

    async def transcribe_audio(
        self,
        audio_base64: str,
        audio_mime: str = "audio/wav",
    ) -> str:
        """Transcribe audio to text using ElevenLabs STT."""
        if not self.stt_configured:
            logger.error("elevenlabs_stt: not configured")
            raise RuntimeError("ElevenLabs STT not configured")

        try:
            # Decode base64 audio
            audio_bytes = base64.b64decode(audio_base64)

            logger.info("elevenlabs_stt: transcribe_audio audio_length=%s", len(audio_bytes))

            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.base_url}/speech-to-text",
                    headers={
                        "xi-api-key": self.api_key,
                    },
                    files={
                        "audio": ("audio.wav", audio_bytes, audio_mime),
                    },
                    timeout=30,
                )

                if response.status_code != 200:
                    logger.error(
                        "elevenlabs_stt: transcribe_audio failed status=%s body=%s",
                        response.status_code,
                        response.text[:200],
                    )
                    raise RuntimeError(f"ElevenLabs STT error: {response.status_code}")

                data = response.json()
                text = data.get("text", "")

                if not text:
                    logger.error("elevenlabs_stt: transcribe_audio no text in response")
                    raise RuntimeError("No text in ElevenLabs STT response")

                logger.info("elevenlabs_stt: transcribe_audio_complete text_length=%s", len(text))
                return text

        except Exception as e:
            logger.error("elevenlabs_stt: transcribe_audio_failed error=%s", e, exc_info=True)
            raise

    async def synthesize_speech(self, text: str) -> str:
        """Synthesize text to speech using ElevenLabs TTS."""
        if not self.tts_configured:
            logger.error("elevenlabs_tts: not configured")
            raise RuntimeError("ElevenLabs TTS not configured")

        try:
            logger.info("elevenlabs_tts: synthesize_speech text_length=%s", len(text))

            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.base_url}/text-to-speech/{self.tts_voice_id}",
                    headers={
                        "xi-api-key": self.api_key,
                        "Content-Type": "application/json",
                    },
                    json={
                        "text": text,
                        "model_id": "eleven_monolingual_v1",
                    },
                    timeout=30,
                )

                if response.status_code != 200:
                    logger.error(
                        "elevenlabs_tts: synthesize_speech failed status=%s body=%s",
                        response.status_code,
                        response.text[:200],
                    )
                    raise RuntimeError(f"ElevenLabs TTS error: {response.status_code}")

                audio_bytes = response.content
                audio_base64 = base64.b64encode(audio_bytes).decode("utf-8")

                logger.info("elevenlabs_tts: synthesize_speech_complete audio_length=%s", len(audio_bytes))
                return audio_base64

        except Exception as e:
            logger.error("elevenlabs_tts: synthesize_speech_failed error=%s", e, exc_info=True)
            raise

    def is_stt_healthy(self) -> bool:
        """Check if ElevenLabs STT is configured."""
        return self.stt_configured

    def is_tts_healthy(self) -> bool:
        """Check if ElevenLabs TTS is configured."""
        return self.tts_configured


# Global service instance
elevenlabs_stt_tts_service = ElevenLabsSTTTTSService()
