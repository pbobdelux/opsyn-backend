"""
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
        logger.error("[AWSSecretsManager] client_creation_error error=%s", exc)
        return None


async def store_webhook_key_in_aws(brand_id: str, webhook_key: str) -> Optional[str]:
    """
    Store a raw webhook key in AWS Secrets Manager.

    Creates a new secret with a versioned name so multiple rotations can
    coexist. The returned ARN is what should be stored in the database.

    Args:
        brand_id: Brand UUID (used in the secret name for namespacing).
        webhook_key: Raw webhook key string to store securely.

    Returns:
        The AWS Secrets Manager ARN string on success, or None on failure.
    """
    if not brand_id or not webhook_key:
        logger.error(
            "[AWSSecretsManager] store_webhook_key_invalid_args brand_id=%s key_present=%s",
            brand_id,
            bool(webhook_key),
        )
        return None

    timestamp = int(time.time())
    secret_name = f"leaflink/webhook/{brand_id}/{timestamp}"

    logger.info(
        "[AWSSecretsManager] storing_webhook_key brand_id=%s secret_name=%s",
        brand_id,
        secret_name,
    )

    try:
        client = _get_boto3_client()
        if client is None:
            return None

        # Run blocking boto3 call in a thread pool to avoid blocking the event loop
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: client.create_secret(
                Name=secret_name,
                SecretString=webhook_key,
                Description=f"LeafLink webhook key for brand {brand_id}",
                Tags=[
                    {"Key": "brand_id", "Value": brand_id},
                    {"Key": "service", "Value": "opsyn-backend"},
                    {"Key": "purpose", "Value": "leaflink-webhook"},
                ],
            ),
        )

        arn = response.get("ARN")
        if not arn:
            logger.error(
                "[AWSSecretsManager] store_webhook_key_no_arn brand_id=%s secret_name=%s",
                brand_id,
                secret_name,
            )
            return None

        logger.info(
            "[AWSSecretsManager] webhook_key_stored brand_id=%s arn=%s",
            brand_id,
            arn,
        )
        return arn

    except Exception as exc:
        logger.error(
            "[AWSSecretsManager] store_webhook_key_error brand_id=%s secret_name=%s error=%s",
            brand_id,
            secret_name,
            exc,
        )
        return None


async def load_webhook_key_from_aws(secret_ref: str) -> Optional[str]:
    """
    Load a raw webhook key from AWS Secrets Manager by ARN or secret name.

    Args:
        secret_ref: AWS Secrets Manager ARN or secret name.

    Returns:
        The raw webhook key string on success, or None on failure.
    """
    if not secret_ref:
        logger.error("[AWSSecretsManager] load_webhook_key_empty_ref")
        return None

    logger.info(
        "[AWSSecretsManager] loading_webhook_key secret_ref=%s",
        secret_ref,
    )

    try:
        client = _get_boto3_client()
        if client is None:
            return None

        # Run blocking boto3 call in a thread pool to avoid blocking the event loop
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: client.get_secret_value(SecretId=secret_ref),
        )

        secret_string = response.get("SecretString")
        if not secret_string:
            logger.warning(
                "[AWSSecretsManager] load_webhook_key_empty_value secret_ref=%s",
                secret_ref,
            )
            return None

        logger.info(
            "[AWSSecretsManager] webhook_key_loaded secret_ref=%s key_length=%s",
            secret_ref,
            len(secret_string),
        )
        return secret_string

    except Exception as exc:
        # Check for specific AWS errors to provide better logging
        exc_type = type(exc).__name__
        exc_str = str(exc)

        if "ResourceNotFoundException" in exc_type or "ResourceNotFoundException" in exc_str:
            logger.error(
                "[AWSSecretsManager] load_webhook_key_not_found secret_ref=%s",
                secret_ref,
            )
        elif "AccessDeniedException" in exc_type or "AccessDeniedException" in exc_str:
            logger.error(
                "[AWSSecretsManager] load_webhook_key_access_denied secret_ref=%s",
                secret_ref,
            )
        else:
            logger.error(
                "[AWSSecretsManager] load_webhook_key_error secret_ref=%s error=%s",
                secret_ref,
                exc,
            )
        return None


async def delete_webhook_key_from_aws(secret_ref: str) -> bool:
    """
    Delete a webhook key from AWS Secrets Manager.

    Used for cleanup when a credential is removed or rotated.
    Schedules deletion with a 7-day recovery window (AWS minimum).

    Args:
        secret_ref: AWS Secrets Manager ARN or secret name.

    Returns:
        True on success, False on failure.
    """
    if not secret_ref:
        logger.error("[AWSSecretsManager] delete_webhook_key_empty_ref")
        return False

    logger.info(
        "[AWSSecretsManager] deleting_webhook_key secret_ref=%s",
        secret_ref,
    )

    try:
        client = _get_boto3_client()
        if client is None:
            return False

        # Run blocking boto3 call in a thread pool to avoid blocking the event loop
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: client.delete_secret(
                SecretId=secret_ref,
                RecoveryWindowInDays=7,  # AWS minimum recovery window
            ),
        )

        logger.info(
            "[AWSSecretsManager] webhook_key_deleted secret_ref=%s",
            secret_ref,
        )
        return True

    except Exception as exc:
        logger.error(
            "[AWSSecretsManager] delete_webhook_key_error secret_ref=%s error=%s",
            secret_ref,
            exc,
        )
        return False
