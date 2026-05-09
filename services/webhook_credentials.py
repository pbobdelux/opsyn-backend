"""
Webhook credential service — per-brand webhook key management via AWS Secrets Manager.

Webhook keys are NEVER stored in plaintext in the database.  The database holds
only a reference (secret name/ARN) and the last-4 characters for display.

Public API:
  get_webhook_key(brand_id)          — fetch raw key from Secrets Manager (cached 5 min)
  store_webhook_key(brand_id, key)   — store key in Secrets Manager, return secret_ref
  delete_webhook_key(brand_id)       — delete key from Secrets Manager
  extract_last4(raw_key)             — return last 4 chars of key (safe for display)
"""

import logging
import os
import time
from typing import Optional

logger = logging.getLogger("webhook_credentials")

# ---------------------------------------------------------------------------
# In-memory TTL cache — avoids repeated Secrets Manager calls per request
# ---------------------------------------------------------------------------
_CACHE_TTL_SECONDS = 300  # 5 minutes

# { brand_id: (raw_key_or_None, fetched_at_monotonic) }
_key_cache: dict[str, tuple[Optional[str], float]] = {}


def _cache_get(brand_id: str) -> Optional[str]:
    """Return cached key if still within TTL, else None."""
    entry = _key_cache.get(brand_id)
    if entry is None:
        return None
    raw_key, fetched_at = entry
    if time.monotonic() - fetched_at > _CACHE_TTL_SECONDS:
        del _key_cache[brand_id]
        return None
    return raw_key


def _cache_set(brand_id: str, raw_key: Optional[str]) -> None:
    _key_cache[brand_id] = (raw_key, time.monotonic())


def _cache_invalidate(brand_id: str) -> None:
    _key_cache.pop(brand_id, None)


# ---------------------------------------------------------------------------
# AWS Secrets Manager client (lazy-initialised)
# ---------------------------------------------------------------------------

def _get_secrets_client():
    """Return a boto3 Secrets Manager client.

    Raises ImportError if boto3 is not installed, RuntimeError if AWS
    credentials are not configured.
    """
    import boto3  # type: ignore

    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    return boto3.client("secretsmanager", region_name=region)


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def extract_last4(raw_key: str) -> str:
    """Return the last 4 characters of a webhook key (or the full key if shorter)."""
    if not raw_key:
        return ""
    return raw_key[-4:] if len(raw_key) >= 4 else raw_key


# ---------------------------------------------------------------------------
# Core operations
# ---------------------------------------------------------------------------

async def get_webhook_key(brand_id: str) -> Optional[str]:
    """
    Fetch the raw webhook key for a brand from AWS Secrets Manager.

    Checks the in-memory TTL cache first to avoid repeated AWS calls.
    Loads the secret_ref from BrandAPICredential, then fetches the value
    from Secrets Manager.

    Returns the raw key string, or None if not configured.
    Logs [WEBHOOK_CREDENTIAL_LOADED] on success (never logs the key value).
    """
    if not brand_id:
        return None

    # Check cache first
    cached = _cache_get(brand_id)
    if cached is not None:
        return cached

    # Load secret_ref from DB
    try:
        from sqlalchemy import select
        from database import AsyncSessionLocal
        from models import BrandAPICredential

        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(BrandAPICredential).where(
                    BrandAPICredential.brand_id == brand_id,
                    BrandAPICredential.integration_name == "leaflink",
                    BrandAPICredential.is_active == True,
                )
            )
            cred = result.scalar_one_or_none()

        if cred is None:
            logger.warning(
                "[WEBHOOK_CREDENTIAL_LOAD] brand_id=%s result=no_credential",
                brand_id,
            )
            _cache_set(brand_id, None)
            return None

        secret_ref = getattr(cred, "webhook_key_secret_ref", None)
        if not secret_ref:
            logger.info(
                "[WEBHOOK_CREDENTIAL_LOAD] brand_id=%s result=no_secret_ref",
                brand_id,
            )
            _cache_set(brand_id, None)
            return None

    except Exception as exc:
        logger.error(
            "[WEBHOOK_CREDENTIAL_LOAD] brand_id=%s db_error=%s",
            brand_id,
            str(exc)[:300],
        )
        return None

    # Fetch from AWS Secrets Manager
    try:
        client = _get_secrets_client()
        response = client.get_secret_value(SecretId=secret_ref)
        raw_key = response.get("SecretString") or ""

        if not raw_key:
            logger.warning(
                "[WEBHOOK_CREDENTIAL_LOAD] brand_id=%s result=empty_secret secret_ref=%s",
                brand_id,
                secret_ref,
            )
            _cache_set(brand_id, None)
            return None

        _cache_set(brand_id, raw_key)
        logger.info(
            "[WEBHOOK_CREDENTIAL_LOADED] brand_id=%s secret_ref=%s key_length=%d",
            brand_id,
            secret_ref,
            len(raw_key),
        )
        return raw_key

    except Exception as exc:
        logger.error(
            "[WEBHOOK_CREDENTIAL_LOAD] brand_id=%s aws_error=%s secret_ref=%s",
            brand_id,
            str(exc)[:300],
            secret_ref,
        )
        return None


async def store_webhook_key(brand_id: str, raw_key: str) -> str:
    """
    Store a webhook key in AWS Secrets Manager.

    Generates a unique secret name: leaflink-webhook-{brand_id}-{timestamp}
    Creates (or updates) the secret in Secrets Manager.
    Returns the secret ARN or name (secret_ref) for storage in the DB.
    Logs [WEBHOOK_CREDENTIAL_STORED] on success (never logs the key value).
    """
    import time as _time

    timestamp = int(_time.time())
    # Sanitise brand_id for use in secret name (replace non-alphanumeric with -)
    safe_brand = "".join(c if c.isalnum() or c == "-" else "-" for c in brand_id)
    secret_name = f"leaflink-webhook-{safe_brand}-{timestamp}"

    try:
        client = _get_secrets_client()

        try:
            # Try to create a new secret
            response = client.create_secret(
                Name=secret_name,
                SecretString=raw_key,
                Description=f"LeafLink webhook key for brand {brand_id}",
                Tags=[
                    {"Key": "brand_id", "Value": brand_id},
                    {"Key": "service", "Value": "opsyn-backend"},
                    {"Key": "integration", "Value": "leaflink-webhook"},
                ],
            )
            secret_ref = response.get("ARN") or secret_name
        except client.exceptions.ResourceExistsException:
            # Secret already exists — update it
            response = client.put_secret_value(
                SecretId=secret_name,
                SecretString=raw_key,
            )
            secret_ref = response.get("ARN") or secret_name

        # Invalidate cache so next get_webhook_key fetches the new value
        _cache_invalidate(brand_id)

        logger.info(
            "[WEBHOOK_CREDENTIAL_STORED] brand_id=%s secret_ref=%s",
            brand_id,
            secret_ref,
        )
        return secret_ref

    except Exception as exc:
        logger.error(
            "[WEBHOOK_CREDENTIAL_STORE] brand_id=%s aws_error=%s",
            brand_id,
            str(exc)[:300],
        )
        raise


async def delete_webhook_key(brand_id: str) -> None:
    """
    Delete the webhook key for a brand from AWS Secrets Manager.

    Reads the secret_ref from BrandAPICredential, then deletes the secret.
    Logs [WEBHOOK_CREDENTIAL_DELETED] on success.
    """
    try:
        from sqlalchemy import select
        from database import AsyncSessionLocal
        from models import BrandAPICredential

        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(BrandAPICredential).where(
                    BrandAPICredential.brand_id == brand_id,
                    BrandAPICredential.integration_name == "leaflink",
                )
            )
            cred = result.scalar_one_or_none()

        secret_ref = getattr(cred, "webhook_key_secret_ref", None) if cred else None

        if not secret_ref:
            logger.info(
                "[WEBHOOK_CREDENTIAL_DELETE] brand_id=%s result=no_secret_ref_to_delete",
                brand_id,
            )
            _cache_invalidate(brand_id)
            return

    except Exception as exc:
        logger.error(
            "[WEBHOOK_CREDENTIAL_DELETE] brand_id=%s db_error=%s",
            brand_id,
            str(exc)[:300],
        )
        return

    try:
        client = _get_secrets_client()
        client.delete_secret(
            SecretId=secret_ref,
            ForceDeleteWithoutRecovery=True,
        )
        _cache_invalidate(brand_id)
        logger.info(
            "[WEBHOOK_CREDENTIAL_DELETED] brand_id=%s secret_ref=%s",
            brand_id,
            secret_ref,
        )
    except Exception as exc:
        logger.error(
            "[WEBHOOK_CREDENTIAL_DELETE] brand_id=%s aws_error=%s secret_ref=%s",
            brand_id,
            str(exc)[:300],
            secret_ref,
        )
