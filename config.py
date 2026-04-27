import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    def __init__(self):
        self.API_KEY = os.getenv("OPSYN_API_KEY", "opsyn_dev_key_change_me")
        self.METRC_VENDOR_KEY = os.getenv("METRC_VENDOR_KEY", "")
        self.METRC_USER_KEY = os.getenv("METRC_USER_KEY", "")
        self.METRC_BASE_URL = os.getenv("METRC_BASE_URL", "https://api-ok.metrc.com")
        self.METRC_FACILITY_LICENSE = os.getenv("METRC_FACILITY_LICENSE", "")
        self.CORS_ORIGINS = os.getenv("CORS_ORIGINS", "*").split(",")
        self.SESSION_TIMEOUT_HOURS = int(os.getenv("SESSION_TIMEOUT_HOURS", "24"))

        # Opsyn Watchdog integration
        # OPSYN_WATCHDOG_SECRET: shared Bearer token for inbound webhook auth and
        #   outbound event signing.  Must be set in the environment — never hardcoded.
        # OPSYN_WATCHDOG_WEBHOOK_URL: optional outbound webhook destination.
        #   If unset, outbound events are silently skipped.
        self.OPSYN_WATCHDOG_SECRET = os.getenv("OPSYN_WATCHDOG_SECRET", "")
        self.OPSYN_WATCHDOG_WEBHOOK_URL = os.getenv("OPSYN_WATCHDOG_WEBHOOK_URL", "")

        # LeafLink OAuth (optional, for token refresh)
        self.LEAFLINK_AUTH_MODE = os.getenv("LEAFLINK_AUTH_MODE", "api_key")  # "api_key" or "oauth"
        self.LEAFLINK_ACCESS_TOKEN = os.getenv("LEAFLINK_ACCESS_TOKEN", "")
        self.LEAFLINK_REFRESH_TOKEN = os.getenv("LEAFLINK_REFRESH_TOKEN", "")
        self.LEAFLINK_CLIENT_ID = os.getenv("LEAFLINK_CLIENT_ID", "")
        self.LEAFLINK_CLIENT_SECRET = os.getenv("LEAFLINK_CLIENT_SECRET", "")
        self.LEAFLINK_TOKEN_ENDPOINT = os.getenv("LEAFLINK_TOKEN_ENDPOINT", "https://www.leaflink.com/api/oauth/token")

        # Sampling rules
        self.SAMPLING_RULES = {
            "concentrate": {"test_qty": 5.0, "reserve_qty": 5.0, "unit": "Grams"},
            "edible": {"test_qty": 1.0, "reserve_qty": 1.0, "unit": "Units"},
            "flower": {"test_qty": 10.0, "reserve_qty": 10.0, "unit": "Grams"},
            "vape": {"test_qty": 2.0, "reserve_qty": 2.0, "unit": "Units"},
            "topical": {"test_qty": 1.0, "reserve_qty": 1.0, "unit": "Units"},
            "preroll": {"test_qty": 2.0, "reserve_qty": 2.0, "unit": "Units"},
        }

        # Mock package data
        self.MOCK_PACKAGES = [
            {
                "tag": "1A4000000012345",
                "item": "Jefe 5G Geekbar - Watermelon Zkittlez",
                "qty": 50.0,
                "uom": "Units",
                "category": "vape",
                "batch": "GEEK-WZ-20",
            },
            {
                "tag": "1A4000000012346",
                "item": "Drizzle Solventless Badder 1g",
                "qty": 30.0,
                "uom": "Grams",
                "category": "concentrate",
                "batch": "TDB3-R",
            },
            {
                "tag": "1A4000000012347",
                "item": "Jefe BOSS NANO Gummies - Blueberry French Toast",
                "qty": 100.0,
                "uom": "Units",
                "category": "edible",
                "batch": "NN-25K01AB027H",
            },
            {
                "tag": "1A4000000012348",
                "item": "Jefe 5G Geekbar - Skywalker OG",
                "qty": 45.0,
                "uom": "Units",
                "category": "vape",
                "batch": "GEEK-SWOG-21",
            },
            {
                "tag": "1A4000000012349",
                "item": "Drizzle Live Rosin 1G Disposable",
                "qty": 25.0,
                "uom": "Units",
                "category": "vape",
                "batch": "1G-R-GG01",
            },
        ]


config = Config()