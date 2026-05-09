"""AWS Secrets Manager configuration and validation."""

import logging
import os
from typing import Optional

logger = logging.getLogger("aws_secrets_config")


class AWSSecretsConfig:
    """Manages AWS Secrets Manager configuration and validation."""

    def __init__(self):
        self.access_key_id = os.getenv("AWS_ACCESS_KEY_ID", "").strip()
        self.secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY", "").strip()
        self.region = (
            os.getenv("AWS_REGION") or
            os.getenv("AWS_DEFAULT_REGION") or
            ""
        ).strip()
        self.prefix = os.getenv(
            "AWS_SECRETS_MANAGER_PREFIX",
            "opsyn/leaflink/webhooks"
        ).strip()
        self.allow_plaintext_fallback = (
            os.getenv("ALLOW_PLAINTEXT_WEBHOOK_SECRET_FALLBACK", "false").lower() == "true"
        )

        logger.info("[AWS_SECRETS_CONFIG_CHECK] checking AWS configuration")
        logger.info("[AWS_SECRETS_REGION] region=%s", self.region or "NOT_SET")

        self._is_configured = (
            bool(self.access_key_id) and
            bool(self.secret_access_key) and
            bool(self.region)
        )

        if self._is_configured:
            logger.info("[AWS_SECRETS_CONFIG_CHECK] AWS Secrets Manager configured")
        else:
            logger.warning(
                "[AWS_SECRETS_CONFIG_CHECK] AWS Secrets Manager NOT configured "
                "access_key_set=%s secret_key_set=%s region_set=%s",
                bool(self.access_key_id),
                bool(self.secret_access_key),
                bool(self.region),
            )

    def is_configured(self) -> bool:
        """Return True if AWS Secrets Manager is properly configured."""
        return self._is_configured

    def get_secret_name(self, brand_id: str) -> str:
        """Generate AWS Secrets Manager secret name for a brand."""
        return f"{self.prefix}/{brand_id}/webhook-key"

    async def test_connection(self) -> tuple[bool, Optional[str]]:
        """Test AWS Secrets Manager connection.

        Returns:
            (success: bool, error_message: Optional[str])
        """
        if not self._is_configured:
            return False, "AWS Secrets Manager not configured"

        try:
            import boto3
            client = boto3.client(
                "secretsmanager",
                region_name=self.region,
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
            )

            # Try to list secrets (minimal permission test)
            client.list_secrets(MaxResults=1)
            return True, None

        except Exception as e:
            error_msg = str(e)[:200]
            logger.error("[AWS_SECRETS_TEST_FAILED] error=%s", error_msg)
            return False, error_msg


# Global instance
_aws_config = AWSSecretsConfig()


def get_aws_config() -> AWSSecretsConfig:
    """Get the global AWS Secrets Manager configuration."""
    return _aws_config
