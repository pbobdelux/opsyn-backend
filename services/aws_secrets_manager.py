"""
AWS Secrets Manager integration for secure webhook key storage.

New high-level API (uses AWSSecretsConfig for explicit credential validation):
    store_webhook_secret(brand_id, webhook_key)   -> (success, secret_ref, error_code)
    retrieve_webhook_secret(secret_ref)            -> Optional[str]

Legacy low-level API (kept for backward compatibility):
    store_webhook_key_in_aws(brand_id, webhook_key) -> Optional[str]  (ARN)
    load_webhook_key_from_aws(secret_ref)           -> Optional[str]
    delete_webhook_key_from_aws(secret_ref)         -> bool
    validate_aws_credentials()                      -> bool
    get_webhook_key_for_brand(...)                  -> Optional[str]

---

AWS Secrets Manager integration for secure webhook key storage.

Provides functions to store, load, and delete per-brand webhook keys
in AWS Secrets Manager. Raw webhook keys are never stored in the database —
only the ARN/reference is persisted in brand_api_credentials.webhook_key_secret_ref.

Secret naming convention:
    leaflink/webhook/{brand_id}/{timestamp}

This versioned naming allows rotation without breaking existing references,
since the ARN is stored and used directly for retrieval.

Environment variables:
    AWS_ACCESS_KEY_ID       — AWS access key (required for non-IAM-role environments)
    AWS_SECRET_ACCESS_KEY   — AWS secret key (required for non-IAM-role environments)
    AWS_REGION              — AWS region (default: us-east-1)

Error handling:
    All AWS errors are logged and never re-raised. Functions return None/False
    on failure so callers can handle gracefully without crashing.

Log markers:
    [WEBHOOK_SECRET_STORE_FAILED] — emitted when storing a key in AWS fails
    [WEBHOOK_SECRET_LOAD_FAILED]  — emitted when loading a key from AWS fails
    [WEBHOOK_SECRET_FALLBACK]     — emitted when falling back to plaintext webhook_key
"""

import asyncio
import logging
import os
import time
from typing import Optional

logger = logging.getLogger("aws_secrets_manager")

# AWS region from environment, defaulting to us-east-1
_AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


def _get_boto3_client():
    """
    Create and return a boto3 secretsmanager client.

    Uses environment variables for credentials (AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY) or falls back to IAM role / instance profile.

    Returns:
        boto3 secretsmanager client, or None if boto3 is not available.
    """
    try:
        import boto3
        return boto3.client(
            "secretsmanager",
            region_name=_AWS_REGION,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
    except ImportError:
        logger.error(
            "[AWSSecretsManager] boto3 not installed — cannot use AWS Secrets Manager. "
            "Install with: pip install boto3"
        )
        return None
    except Exception as exc:
        logger.error(
            "[WEBHOOK_SECRET_STORE_FAILED] [AWSSecretsManager] client_creation_error "
            "error_type=%s error=%s",
            type(exc).__name__,
            exc,
        )
        return None


async def store_webhook_key_in_aws(brand_id: str, webhook_key: str) -> Optional[str]:
    """
    Store a webhook key in AWS Secrets Manager.

    Creates a new secret named leaflink/webhook/{brand_id}/{timestamp}.
    Returns the ARN of the created secret, or None on failure.

    Args:
        brand_id: Brand UUID (used in the secret name for namespacing).
        webhook_key: Raw webhook secret to store.

    Returns:
        ARN string if successful, None if AWS is unavailable or storage fails.
        Logs [WEBHOOK_SECRET_STORE_FAILED] on failure.
    """
    secret_name = f"leaflink/webhook/{brand_id}/{int(time.time())}"

    def _store_sync():
        client = _get_boto3_client()
        if client is None:
            return None
        try:
            response = client.create_secret(
                Name=secret_name,
                SecretString=webhook_key,
                Tags=[
                    {"Key": "brand_id", "Value": brand_id},
                    {"Key": "service", "Value": "opsyn-backend"},
                    {"Key": "purpose", "Value": "leaflink-webhook-key"},
                ],
            )
            arn = response.get("ARN")
            logger.info(
                "[AWSSecretsManager] stored webhook key brand_id=%s secret_name=%s arn=%s",
                brand_id,
                secret_name,
                arn,
            )
            return arn
        except Exception as exc:
            logger.error(
                "[WEBHOOK_SECRET_STORE_FAILED] brand_id=%s secret_name=%s error=%s",
                brand_id,
                secret_name,
                exc,
            )
            return None

    try:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _store_sync)
    except Exception as exc:
        logger.error(
            "[WEBHOOK_SECRET_STORE_FAILED] brand_id=%s executor_error=%s",
            brand_id,
            exc,
        )
        return None


async def load_webhook_key_from_aws(secret_ref: str) -> Optional[str]:
    """
    Load a webhook key from AWS Secrets Manager by ARN or secret name.

    Args:
        secret_ref: ARN or name of the secret to retrieve.

    Returns:
        The raw webhook key string if successful, None if AWS is unavailable
        or the secret cannot be retrieved.
        Logs [WEBHOOK_SECRET_LOAD_FAILED] on failure.
    """
    def _load_sync():
        client = _get_boto3_client()
        if client is None:
            return None
        try:
            response = client.get_secret_value(SecretId=secret_ref)
            secret = response.get("SecretString")
            if not secret:
                logger.warning(
                    "[WEBHOOK_SECRET_LOAD_FAILED] secret_ref=%s reason=empty_secret_string",
                    secret_ref,
                )
                return None
            logger.info(
                "[AWSSecretsManager] loaded webhook key secret_ref=%s",
                secret_ref,
            )
            return secret
        except Exception as exc:
            error_type = type(exc).__name__
            if "ResourceNotFoundException" in error_type or "ResourceNotFoundException" in str(exc):
                logger.error(
                    "[WEBHOOK_SECRET_LOAD_FAILED] secret_ref=%s error_type=ResourceNotFoundException "
                    "details=Secret not found in AWS Secrets Manager",
                    secret_ref,
                )
            elif "AccessDeniedException" in error_type or "AccessDeniedException" in str(exc):
                logger.error(
                    "[WEBHOOK_SECRET_LOAD_FAILED] secret_ref=%s error_type=AccessDeniedException "
                    "details=IAM permissions missing for secretsmanager:GetSecretValue",
                    secret_ref,
                )
            else:
                logger.error(
                    "[WEBHOOK_SECRET_LOAD_FAILED] secret_ref=%s error_type=%s error=%s",
                    secret_ref,
                    error_type,
                    str(exc)[:500],
                )
            return None

    try:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _load_sync)
    except Exception as exc:
        logger.error(
            "[WEBHOOK_SECRET_LOAD_FAILED] secret_ref=%s executor_error=%s",
            secret_ref,
            exc,
        )
        return None


async def delete_webhook_key_from_aws(secret_ref: str) -> bool:
    """
    Delete a webhook key from AWS Secrets Manager.

    Args:
        secret_ref: ARN or name of the secret to delete.

    Returns:
        True if deleted successfully, False otherwise.
    """
    def _delete_sync():
        client = _get_boto3_client()
        if client is None:
            return False
        try:
            client.delete_secret(
                SecretId=secret_ref,
                ForceDeleteWithoutRecovery=False,
            )
            logger.info(
                "[AWSSecretsManager] deleted webhook key secret_ref=%s",
                secret_ref,
            )
            return True
        except Exception as exc:
            logger.error(
                "[AWSSecretsManager] delete_failed secret_ref=%s error=%s",
                secret_ref,
                exc,
            )
            return False

    try:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _delete_sync)
    except Exception as exc:
        logger.error(
            "[AWSSecretsManager] delete_executor_error secret_ref=%s error=%s",
            secret_ref,
            exc,
        )
        return False


async def validate_aws_credentials() -> bool:
    """
    Validate that AWS credentials are configured and functional.

    Attempts a lightweight AWS API call (list_secrets with max_results=1)
    to verify that boto3 is installed and credentials are valid.

    Returns:
        True if AWS is accessible, False otherwise.
        Logs warnings if AWS is not configured.
    """
    def _validate_sync():
        client = _get_boto3_client()
        if client is None:
            logger.warning(
                "[AWSSecretsManager] validate_aws_credentials: boto3 not available — "
                "webhook-config endpoint will fail gracefully"
            )
            return False
        try:
            client.list_secrets(MaxResults=1)
            logger.info("[AWSSecretsManager] validate_aws_credentials: AWS credentials OK")
            return True
        except Exception as exc:
            logger.warning(
                "[AWSSecretsManager] validate_aws_credentials: AWS not accessible — "
                "webhook-config endpoint will fail gracefully. error=%s",
                exc,
            )
            return False

    try:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _validate_sync)
    except Exception as exc:
        logger.warning(
            "[AWSSecretsManager] validate_aws_credentials executor_error=%s",
            exc,
        )
        return False


async def get_webhook_key_for_brand(brand_id: str, secret_ref: Optional[str], plaintext_key: Optional[str]) -> Optional[str]:
    """
    Resolve the webhook key for a brand using the fallback chain:
      1. AWS Secrets Manager (via secret_ref / webhook_key_secret_ref)
      2. Plaintext webhook_key column (legacy fallback)
      3. None (no key configured — skip verification)

    Logs each fallback step for audit trail.

    Args:
        brand_id: Brand UUID (for logging).
        secret_ref: AWS Secrets Manager ARN/reference (webhook_key_secret_ref column).
        plaintext_key: Plaintext webhook key (deprecated webhook_key column).

    Returns:
        The resolved webhook key string, or None if no key is available.
    """
    # Step 1: Try AWS Secrets Manager
    if secret_ref:
        logger.info(
            "[AWSSecretsManager] resolving webhook key via AWS brand_id=%s",
            brand_id,
        )
        aws_key = await load_webhook_key_from_aws(secret_ref)
        if aws_key:
            return aws_key
        # AWS failed — log and fall through to plaintext fallback
        logger.warning(
            "[WEBHOOK_SECRET_FALLBACK] brand_id=%s reason=aws_load_failed "
            "falling_back_to=plaintext_webhook_key",
            brand_id,
        )

    # Step 2: Plaintext fallback (deprecated)
    if plaintext_key:
        logger.warning(
            "[WEBHOOK_SECRET_FALLBACK] brand_id=%s using=plaintext_webhook_key "
            "reason=no_secret_ref_or_aws_unavailable "
            "action=migrate_to_webhook_key_secret_ref",
            brand_id,
        )
        return plaintext_key

    # Step 3: No key available
    logger.warning(
        "[WEBHOOK_SECRET_FALLBACK] brand_id=%s reason=no_webhook_key_configured "
        "action=skip_signature_verification",
        brand_id,
    )
    return None


# =============================================================================
# High-level API — uses AWSSecretsConfig for explicit credential validation
# =============================================================================


async def store_webhook_secret(
    brand_id: str,
    webhook_key: str,
) -> tuple[bool, Optional[str], Optional[str]]:
    """Store webhook secret in AWS Secrets Manager.

    Uses AWSSecretsConfig for explicit credential validation before attempting
    any AWS API calls.  Supports create-or-update semantics via put_secret_value
    with a ResourceNotFoundException fallback to create_secret.

    Returns:
        (success: bool, secret_ref: Optional[str], error_code: Optional[str])
    """
    import json as _json

    from services.aws_secrets_config import get_aws_config

    config = get_aws_config()

    if not config.is_configured():
        logger.warning(
            "[AWS_SECRETS_STORE_FAILED] AWS Secrets Manager not configured brand=%s",
            brand_id,
        )
        return False, None, "AWS_SECRETS_NOT_CONFIGURED"

    try:
        import boto3

        logger.info("[AWS_SECRETS_STORE_START] storing webhook secret brand=%s", brand_id)

        client = boto3.client(
            "secretsmanager",
            region_name=config.region,
            aws_access_key_id=config.access_key_id,
            aws_secret_access_key=config.secret_access_key,
        )

        secret_name = config.get_secret_name(brand_id)
        secret_value = _json.dumps({
            "webhook_key": webhook_key,
            "brand_id": brand_id,
        })

        try:
            # Try to update existing secret
            client.put_secret_value(
                SecretId=secret_name,
                SecretString=secret_value,
            )
            logger.info("[AWS_SECRETS_STORE_SUCCESS] updated secret brand=%s", brand_id)
        except client.exceptions.ResourceNotFoundException:
            # Create new secret
            client.create_secret(
                Name=secret_name,
                SecretString=secret_value,
                Description=f"LeafLink webhook key for brand {brand_id}",
            )
            logger.info("[AWS_SECRETS_STORE_SUCCESS] created secret brand=%s", brand_id)

        return True, secret_name, None

    except Exception as e:
        error_msg = str(e)[:200]

        if "AccessDenied" in error_msg or "UnauthorizedOperation" in error_msg:
            logger.error(
                "[AWS_SECRETS_PERMISSION_DENIED] insufficient permissions brand=%s error=%s",
                brand_id,
                error_msg,
            )
            return False, None, "AWS_SECRETS_PERMISSION_DENIED"

        logger.error(
            "[AWS_SECRETS_STORE_FAILED] error storing secret brand=%s error=%s",
            brand_id,
            error_msg,
        )
        return False, None, "AWS_SECRETS_STORE_FAILED"


async def retrieve_webhook_secret(secret_ref: str) -> Optional[str]:
    """Retrieve webhook secret from AWS Secrets Manager.

    Uses AWSSecretsConfig for explicit credential validation.

    Returns:
        The webhook key string, or None if not found / AWS not configured.
    """
    import json as _json

    from services.aws_secrets_config import get_aws_config

    config = get_aws_config()

    if not config.is_configured():
        logger.warning("[AWS_SECRETS_RETRIEVE_FAILED] AWS not configured")
        return None

    try:
        import boto3

        client = boto3.client(
            "secretsmanager",
            region_name=config.region,
            aws_access_key_id=config.access_key_id,
            aws_secret_access_key=config.secret_access_key,
        )

        response = client.get_secret_value(SecretId=secret_ref)
        secret_value = _json.loads(response["SecretString"])
        return secret_value.get("webhook_key")

    except Exception as e:
        logger.error("[AWS_SECRETS_RETRIEVE_FAILED] error=%s", str(e)[:200])
        return None
