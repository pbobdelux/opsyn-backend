import bcrypt
import logging

logger = logging.getLogger("auth")


def hash_passcode(passcode: str) -> str:
    """Hash a passcode using bcrypt."""
    salt = bcrypt.gensalt(rounds=12)
    hashed = bcrypt.hashpw(passcode.encode(), salt)
    return hashed.decode()


def verify_passcode(passcode: str, hashed: str) -> bool:
    """Verify a passcode against its hash."""
    try:
        return bcrypt.checkpw(passcode.encode(), hashed.encode())
    except Exception as e:
        logger.error("passcode_verify_failed error=%s", str(e))
        return False
