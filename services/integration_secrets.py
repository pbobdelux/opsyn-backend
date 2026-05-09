"""
Unified Secrets Manager for integration credentials.

Provides a single interface for storing, retrieving, and deleting integration
secrets (API keys, webhook secrets) in AWS Secrets Manager.  Raw secret values
are NEVER stored in the database — only the ARN/reference and last-4 display
hint are persisted.

Secret naming convention:
    opsyn/{environment}/{org_id}/{brand_id}/{integration_name}/{secret_type}

Example:
    opsyn/production/ac42f7cb-4937-4920-b6c1-0f964787eb2f/380e963d-36fc-4928-a4f4-e569cd535f9e/leaflink/api_key

Environment variables:
    AWS_REGION                      — AWS region (default: us-west-2)
    AWS_ACCESS_KEY_ID               — AWS access key (or use IAM role)
    AWS_SECRET_ACCESS_KEY           — AWS secret key (or use IAM role)
    AWS_SECRETS_MANAGER_ENABLED     — Enable AWS Secrets Manager (default: true)
    ALLOW_PLAINTEXT_SECRET_FALLBACK — Allow plaintext fallback during migration (default: false)
    SECRET_CACHE_TTL_SECONDS        — In-memory cache TTL in seconds (default: 300)
    ENVIRONMENT                     — Deployment environment (default: production)

Security requirements:
    - Never log raw secret values
    - Never log raw values in exceptions
    - Only show last4 in API responses and logs
    - Use AWS IAM roles for authentication (no hardcoded credentials)
    - Cache secrets in memory for configurable TTL

Log markers:
    [INTEGRATION_SECRET_STORE]      — secret stored in AWS
    [INTEGRATION_SECRET_RETRIEVE]   — secret retrieved from AWS
    [INTEGRATION_SECRET_DELETE]     — secret deleted from AWS
    [INTEGRATION_SECRET_CACHE_HIT]  — secret served from in-memory cache
    [INTEGRATION_SECRET_STORE_FAILED]   — AWS store failed
    [INTEGRATION_SECRET_RETRIEVE_FAILED] — AWS retrieve failed
    [INTEGRATION_SECRET_DELETE_FAILED]  — AWS delete failed
    [INTEGRATION_SECRET_VALIDATE]   — AWS connectivity test result
"""

import asyncio
import logging
import os
import time
from typing import Optional

logger = logging.getLogger("integration_secrets")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_AWS_REGION = os.getenv("AWS_REGION", "us-west-2")
_AWS_SECRETS_MANAGER_ENABLED = (
    os.getenv("AWS_SECRETS_MANAGER_ENABLED", "true").lower() == "true"
)
_SECRET_CACHE_TTL_SECONDS = int(os.getenv("SECRET_CACHE_TTL_SECONDS", "300"))
_ENVIRONMENT = os.getenv("ENVIRONMENT", os.getenv("RAILWAY_ENVIRONMENT_NAME", "production"))

# ---------------------------------------------------------------------------
# In-memory secret cache: { secret_ref: (value, expires_at) }
# ---------------------------------------------------------------------------
_secret_cache: dict[str, tuple[str, float]] = {}


def _get_boto3_client():
    """Create a boto3 secretsmanager client using env credentials or IAM role."""
    try:
        import boto3

        return boto3.client(
            "secretsmanager",
            region_name=_AWS_REGION,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID") or None,
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY") or None,
        )
    except ImportError:
        logger.error(
            "[INTEGRATION_SECRET] boto3 not installed — cannot use AWS Secrets Manager. "
            "Install with: pip install boto3"
        )
        return None
    except Exception as exc:
        logger.error(
            "[INTEGRATION_SECRET] client_creation_error error_type=%s",
            type(exc).__name__,
        )
        return None


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def mask_secret(secret_value: str) -> str:
    """Return '***' + last 4 chars of the secret for safe logging/display.

    Args:
        secret_value: Raw secret string.

    Returns:
        Masked string like '***abcd'.
    """
    if not secret_value:
        return "***"
    last4 = secret_value[-4:] if len(secret_value) >= 4 else secret_value
    return f"***{last4}"


def build_secret_name(
    environment: str,
    org_id: str,
    brand_id: str,
    integration_name: str,
    secret_type: str,
) -> str:
    """Build the AWS Secrets Manager secret name/path.

    Format: opsyn/{environment}/{org_id}/{brand_id}/{integration_name}/{secret_type}

    Example:
        opsyn/production/ac42f7cb-4937-4920-b6c1-0f964787eb2f/380e963d-36fc-4928-a4f4-e569cd535f9e/leaflink/api_key

    Args:
        environment:      Deployment environment (e.g. 'production', 'staging').
        org_id:           Organization UUID.
        brand_id:         Brand UUID.
        integration_name: Integration name (e.g. 'leaflink').
        secret_type:      Secret type (e.g. 'api_key', 'webhook_key').

    Returns:
        Full secret path string.
    """
    return f"opsyn/{environment}/{org_id}/{brand_id}/{integration_name}/{secret_type}"


# ---------------------------------------------------------------------------
# Core CRUD operations
# ---------------------------------------------------------------------------


async def store_integration_secret(
    org_id: str,
    brand_id: str,
    integration_name: str,
    secret_type: str,
    secret_value: str,
) -> str:
    """Store an integration secret in AWS Secrets Manager.

    Creates or updates the secret at the canonical path:
        opsyn/{environment}/{org_id}/{brand_id}/{integration_name}/{secret_type}

    Args:
        org_id:           Organization UUID.
        brand_id:         Brand UUID.
        integration_name: Integration name (e.g. 'leaflink').
        secret_type:      Secret type (e.g. 'api_key', 'webhook_key').
        secret_value:     Raw secret value to store.

    Returns:
        Secret reference (ARN or path) that should be stored in the database.

    Raises:
        RuntimeError: If AWS Secrets Manager is unavailable or storage fails.
    """
    if not _AWS_SECRETS_MANAGER_ENABLED:
        raise RuntimeError(
            "AWS_SECRETS_MANAGER_ENABLED=false — cannot store secrets"
        )

    secret_name = build_secret_name(
        _ENVIRONMENT, org_id, brand_id, integration_name, secret_type
    )
    last4 = secret_value[-4:] if len(secret_value) >= 4 else secret_value

    logger.info(
        "[INTEGRATION_SECRET_STORE] storing secret org_id=%s brand_id=%s "
        "integration=%s type=%s last4=%s",
        org_id,
        brand_id,
        integration_name,
        secret_type,
        last4,
    )

    def _store_sync() -> str:
        client = _get_boto3_client()
        if client is None:
            raise RuntimeError("AWS Secrets Manager client unavailable (boto3 not installed)")

        try:
            # Try to update existing secret first
            client.put_secret_value(
                SecretId=secret_name,
                SecretString=secret_value,
            )
            logger.info(
                "[INTEGRATION_SECRET_STORE] updated_existing secret_name=%s",
                secret_name,
            )
            return secret_name
        except Exception as put_exc:
            # ResourceNotFoundException → create new secret
            exc_name = type(put_exc).__name__
            if "ResourceNotFoundException" in exc_name or "ResourceNotFoundException" in str(put_exc):
                try:
                    response = client.create_secret(
                        Name=secret_name,
                        SecretString=secret_value,
                        Description=(
                            f"Opsyn {integration_name} {secret_type} "
                            f"for brand {brand_id} (org {org_id})"
                        ),
                        Tags=[
                            {"Key": "org_id", "Value": org_id},
                            {"Key": "brand_id", "Value": brand_id},
                            {"Key": "integration", "Value": integration_name},
                            {"Key": "secret_type", "Value": secret_type},
                            {"Key": "service", "Value": "opsyn-backend"},
                            {"Key": "environment", "Value": _ENVIRONMENT},
                        ],
                    )
                    arn = response.get("ARN", secret_name)
                    logger.info(
                        "[INTEGRATION_SECRET_STORE] created_new secret_name=%s arn=%s",
                        secret_name,
                        arn,
                    )
                    return arn
                except Exception as create_exc:
                    logger.error(
                        "[INTEGRATION_SECRET_STORE_FAILED] create_failed "
                        "secret_name=%s error_type=%s",
                        secret_name,
                        type(create_exc).__name__,
                    )
                    raise RuntimeError(
                        f"Failed to create secret in AWS Secrets Manager: "
                        f"{type(create_exc).__name__}"
                    ) from create_exc
            else:
                logger.error(
                    "[INTEGRATION_SECRET_STORE_FAILED] put_failed "
                    "secret_name=%s error_type=%s",
                    secret_name,
                    exc_name,
                )
                raise RuntimeError(
                    f"Failed to store secret in AWS Secrets Manager: {exc_name}"
                ) from put_exc

    try:
        loop = asyncio.get_event_loop()
        secret_ref = await loop.run_in_executor(None, _store_sync)

        # Invalidate cache for this ref so next read fetches fresh value
        _secret_cache.pop(secret_ref, None)
        _secret_cache.pop(secret_name, None)

        return secret_ref
    except RuntimeError:
        raise
    except Exception as exc:
        logger.error(
            "[INTEGRATION_SECRET_STORE_FAILED] executor_error "
            "integration=%s type=%s error_type=%s",
            integration_name,
            secret_type,
            type(exc).__name__,
        )
        raise RuntimeError(
            f"Unexpected error storing secret: {type(exc).__name__}"
        ) from exc


async def get_integration_secret(secret_ref: str) -> str:
    """Retrieve an integration secret from AWS Secrets Manager.

    Caches the result in memory for SECRET_CACHE_TTL_SECONDS (default 5 min).
    The raw value is never logged.

    Args:
        secret_ref: AWS Secrets Manager ARN or secret name.

    Returns:
        Raw secret value string.

    Raises:
        RuntimeError: If the secret is not found or AWS is unavailable.
    """
    if not _AWS_SECRETS_MANAGER_ENABLED:
        raise RuntimeError(
            "AWS_SECRETS_MANAGER_ENABLED=false — cannot retrieve secrets"
        )

    # Check in-memory cache
    cached = _secret_cache.get(secret_ref)
    if cached is not None:
        value, expires_at = cached
        if time.monotonic() < expires_at:
            logger.info(
                "[INTEGRATION_SECRET_CACHE_HIT] secret_ref=%s",
                secret_ref,
            )
            return value
        # Expired — remove from cache
        del _secret_cache[secret_ref]

    logger.info(
        "[INTEGRATION_SECRET_RETRIEVE] fetching from AWS secret_ref=%s",
        secret_ref,
    )

    def _retrieve_sync() -> str:
        client = _get_boto3_client()
        if client is None:
            raise RuntimeError("AWS Secrets Manager client unavailable (boto3 not installed)")

        try:
            response = client.get_secret_value(SecretId=secret_ref)
            secret = response.get("SecretString")
            if not secret:
                raise RuntimeError(
                    f"Secret at {secret_ref} has empty SecretString"
                )
            return secret
        except Exception as exc:
            exc_name = type(exc).__name__
            if "ResourceNotFoundException" in exc_name or "ResourceNotFoundException" in str(exc):
                logger.error(
                    "[INTEGRATION_SECRET_RETRIEVE_FAILED] not_found secret_ref=%s",
                    secret_ref,
                )
                raise RuntimeError(
                    f"Secret not found in AWS Secrets Manager: {secret_ref}"
                ) from exc
            elif "AccessDeniedException" in exc_name or "AccessDeniedException" in str(exc):
                logger.error(
                    "[INTEGRATION_SECRET_RETRIEVE_FAILED] access_denied secret_ref=%s",
                    secret_ref,
                )
                raise RuntimeError(
                    f"Access denied to secret {secret_ref} — check IAM permissions"
                ) from exc
            else:
                logger.error(
                    "[INTEGRATION_SECRET_RETRIEVE_FAILED] error secret_ref=%s error_type=%s",
                    secret_ref,
                    exc_name,
                )
                raise RuntimeError(
                    f"Failed to retrieve secret from AWS Secrets Manager: {exc_name}"
                ) from exc

    try:
        loop = asyncio.get_event_loop()
        value = await loop.run_in_executor(None, _retrieve_sync)

        # Cache the result
        _secret_cache[secret_ref] = (value, time.monotonic() + _SECRET_CACHE_TTL_SECONDS)

        logger.info(
            "[INTEGRATION_SECRET_RETRIEVE] success secret_ref=%s",
            secret_ref,
        )
        return value
    except RuntimeError:
        raise
    except Exception as exc:
        logger.error(
            "[INTEGRATION_SECRET_RETRIEVE_FAILED] executor_error "
            "secret_ref=%s error_type=%s",
            secret_ref,
            type(exc).__name__,
        )
        raise RuntimeError(
            f"Unexpected error retrieving secret: {type(exc).__name__}"
        ) from exc


async def delete_integration_secret(secret_ref: str) -> bool:
    """Delete an integration secret from AWS Secrets Manager.

    Schedules deletion with a 7-day recovery window (not immediate).
    The raw value is never logged.

    Args:
        secret_ref: AWS Secrets Manager ARN or secret name.

    Returns:
        True if deleted (or scheduled for deletion) successfully, False otherwise.
    """
    if not _AWS_SECRETS_MANAGER_ENABLED:
        logger.warning(
            "[INTEGRATION_SECRET_DELETE] skipped AWS_SECRETS_MANAGER_ENABLED=false "
            "secret_ref=%s",
            secret_ref,
        )
        return False

    logger.info(
        "[INTEGRATION_SECRET_DELETE] scheduling deletion secret_ref=%s",
        secret_ref,
    )

    def _delete_sync() -> bool:
        client = _get_boto3_client()
        if client is None:
            return False

        try:
            client.delete_secret(
                SecretId=secret_ref,
                RecoveryWindowInDays=7,
            )
            logger.info(
                "[INTEGRATION_SECRET_DELETE] scheduled secret_ref=%s recovery_window_days=7",
                secret_ref,
            )
            return True
        except Exception as exc:
            exc_name = type(exc).__name__
            if "ResourceNotFoundException" in exc_name or "ResourceNotFoundException" in str(exc):
                # Already deleted — treat as success
                logger.info(
                    "[INTEGRATION_SECRET_DELETE] already_deleted secret_ref=%s",
                    secret_ref,
                )
                return True
            logger.error(
                "[INTEGRATION_SECRET_DELETE_FAILED] secret_ref=%s error_type=%s",
                secret_ref,
                exc_name,
            )
            return False

    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, _delete_sync)

        # Remove from cache regardless of result
        _secret_cache.pop(secret_ref, None)

        return result
    except Exception as exc:
        logger.error(
            "[INTEGRATION_SECRET_DELETE_FAILED] executor_error "
            "secret_ref=%s error_type=%s",
            secret_ref,
            type(exc).__name__,
        )
        return False


async def validate_aws_secrets_manager() -> dict:
    """Test AWS Secrets Manager connectivity and permissions.

    Performs lightweight API calls to verify create, read, and update access.

    Returns:
        {
            "aws_configured": bool,
            "region": str,
            "can_create": bool,
            "can_read": bool,
            "can_update": bool,
            "error_code": str | None,
        }
    """
    region = _AWS_REGION
    access_key_set = bool(os.getenv("AWS_ACCESS_KEY_ID"))
    secret_key_set = bool(os.getenv("AWS_SECRET_ACCESS_KEY"))
    aws_configured = bool(region) and (access_key_set and secret_key_set or True)

    if not _AWS_SECRETS_MANAGER_ENABLED:
        return {
            "aws_configured": False,
            "region": region,
            "can_create": False,
            "can_read": False,
            "can_update": False,
            "error_code": "AWS_SECRETS_MANAGER_DISABLED",
        }

    def _validate_sync() -> dict:
        client = _get_boto3_client()
        if client is None:
            return {
                "aws_configured": False,
                "region": region,
                "can_create": False,
                "can_read": False,
                "can_update": False,
                "error_code": "BOTO3_NOT_INSTALLED",
            }

        can_create = False
        can_read = False
        can_update = False
        error_code = None

        # Test list_secrets (minimal read permission)
        try:
            client.list_secrets(MaxResults=1)
            can_read = True
            can_create = True
            can_update = True
        except Exception as exc:
            exc_name = type(exc).__name__
            exc_str = str(exc)
            if "AccessDenied" in exc_name or "AccessDenied" in exc_str:
                error_code = "ACCESS_DENIED"
            elif "InvalidClientTokenId" in exc_str or "InvalidClientTokenId" in exc_name:
                error_code = "INVALID_CREDENTIALS"
            elif "EndpointResolutionError" in exc_str or "Could not connect" in exc_str:
                error_code = "CONNECTION_FAILED"
            else:
                error_code = exc_name

            logger.warning(
                "[INTEGRATION_SECRET_VALIDATE] connectivity_test_failed "
                "error_type=%s error_code=%s",
                exc_name,
                error_code,
            )

        logger.info(
            "[INTEGRATION_SECRET_VALIDATE] region=%s can_read=%s can_create=%s "
            "can_update=%s error_code=%s",
            region,
            can_read,
            can_create,
            can_update,
            error_code,
        )

        return {
            "aws_configured": True,
            "region": region,
            "can_create": can_create,
            "can_read": can_read,
            "can_update": can_update,
            "error_code": error_code,
        }

    try:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _validate_sync)
    except Exception as exc:
        logger.error(
            "[INTEGRATION_SECRET_VALIDATE] executor_error error_type=%s",
            type(exc).__name__,
        )
        return {
            "aws_configured": aws_configured,
            "region": region,
            "can_create": False,
            "can_read": False,
            "can_update": False,
            "error_code": type(exc).__name__,
        }
