from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional

app = FastAPI()


# -------------------------
# TEMP IN-MEMORY DATA
# -------------------------

PINS = {
    "1234": {
        "is_super_admin": False,
        "org_id": "org_onboarding",
        "allowed_brands": ["noble-nectar"],
    },
    "1263": {
        "is_super_admin": True,
        "org_id": "org_onboarding",
        "allowed_brands": ["noble-nectar", "test-brand"],
    },
    "0420": {
        "is_super_admin": True,
        "org_id": "org_onboarding",
        "allowed_brands": ["noble-nectar", "test-brand"],
    },
}


ACTIVE_BRAND = {
    "org_onboarding": "noble-nectar"
}


# -------------------------
# MODELS
# -------------------------

class PinLoginRequest(BaseModel):
    pin: str
    org_id: str


class BrandSelectRequest(BaseModel):
    brand_id: str
    org_id: str


# -------------------------
# ROUTES
# -------------------------

@app.post("/auth/pin-login")
def pin_login(data: PinLoginRequest):
    user = PINS.get(data.pin)

    if not user:
        raise HTTPException(status_code=401, detail="Invalid PIN")

    return {
        "ok": True,
        "is_super_admin": user["is_super_admin"],
        "signed_in_org_id": data.org_id,
        "resolved_brand_id": ACTIVE_BRAND.get(data.org_id, None),
        "allowed_brands": user["allowed_brands"],
    }


@app.get("/auth/brand-context")
def brand_context(org_id: str):
    active = ACTIVE_BRAND.get(org_id)

    return {
        "ok": True,
        "signed_in_org_id": org_id,
        "resolved_brand_id": active,
        "source": "backend_binding",
        "available_brands": [
            {"brand_id": "noble-nectar", "brand_name": "Noble Nectar"},
            {"brand_id": "test-brand", "brand_name": "Test Brand"},
        ],
        "active_binding": {
            "brand_id": active,
            "brand_name": "Noble Nectar",
            "is_default": True,
            "is_active": True,
        },
    }


@app.post("/auth/brand-context/select")
def select_brand(data: BrandSelectRequest):
    ACTIVE_BRAND[data.org_id] = data.brand_id

    return {
        "ok": True,
        "resolved_brand_id": data.brand_id,
        "source": "manual_switch",
    }