from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict, Any


app = FastAPI(title="Opsyn Backend", version="1.0.0")


# -------------------------
# CORS
# -------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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

BRANDS = {
    "noble-nectar": {
        "brand_id": "noble-nectar",
        "brand_name": "Noble Nectar",
    },
    "test-brand": {
        "brand_id": "test-brand",
        "brand_name": "Test Brand",
    },
}

ORDERS = [
    {
        "id": "ord_1001",
        "order_number": "NN-1001",
        "org_id": "org_onboarding",
        "brand_id": "noble-nectar",
        "customer_name": "Green Leaf Wellness",
        "status": "ready",
        "review_status": "ready",
        "amount": 2450.00,
        "currency": "USD",
        "created_at": "2026-04-20T09:15:00Z",
        "updated_at": "2026-04-20T10:00:00Z",
    },
    {
        "id": "ord_1002",
        "order_number": "NN-1002",
        "org_id": "org_onboarding",
        "brand_id": "noble-nectar",
        "customer_name": "High Plains Dispensary",
        "status": "needs_review",
        "review_status": "needs_review",
        "amount": 1875.50,
        "currency": "USD",
        "created_at": "2026-04-20T11:30:00Z",
        "updated_at": "2026-04-20T11:45:00Z",
    },
    {
        "id": "ord_1003",
        "order_number": "NN-1003",
        "org_id": "org_onboarding",
        "brand_id": "noble-nectar",
        "customer_name": "Red River Relief",
        "status": "blocked",
        "review_status": "blocked",
        "amount": 920.00,
        "currency": "USD",
        "created_at": "2026-04-21T08:10:00Z",
        "updated_at": "2026-04-21T08:12:00Z",
    },
    {
        "id": "ord_2001",
        "order_number": "TB-2001",
        "org_id": "org_onboarding",
        "brand_id": "test-brand",
        "customer_name": "Test Account",
        "status": "ready",
        "review_status": "ready",
        "amount": 500.00,
        "currency": "USD",
        "created_at": "2026-04-21T12:00:00Z",
        "updated_at": "2026-04-21T12:05:00Z",
    },
]


# -------------------------
# MODELS
# -------------------------

class PinLoginRequest(BaseModel):
    pin: str
    org_id: str


class BrandSelectRequest(BaseModel):
    brand_id: str
    org_id: str


class SyncLeaflinkRequest(BaseModel):
    org_id: Optional[str] = None
    brand_id: Optional[str] = None


# -------------------------
# HELPERS
# -------------------------

def get_active_brand_for_org(org_id: str) -> Optional[str]:
    return ACTIVE_BRAND.get(org_id)


def get_brand_name(brand_id: Optional[str]) -> Optional[str]:
    if not brand_id:
        return None
    brand = BRANDS.get(brand_id)
    return brand["brand_name"] if brand else brand_id


def filter_orders(
    org_id: Optional[str] = None,
    brand_id: Optional[str] = None,
    status: Optional[str] = None,
    q: Optional[str] = None,
) -> List[Dict[str, Any]]:
    results = ORDERS

    if org_id:
        results = [o for o in results if o["org_id"] == org_id]

    if brand_id:
        results = [o for o in results if o["brand_id"] == brand_id]

    if status and status.lower() != "all":
        normalized = status.lower()
        results = [
            o for o in results
            if o["status"].lower() == normalized or o["review_status"].lower() == normalized
        ]

    if q:
        q_lower = q.lower().strip()
        results = [
            o for o in results
            if q_lower in o["order_number"].lower()
            or q_lower in o["customer_name"].lower()
            or q_lower in o["status"].lower()
            or q_lower in o["review_status"].lower()
        ]

    results = sorted(results, key=lambda x: x["updated_at"], reverse=True)
    return results


def build_orders_response(
    org_id: Optional[str] = None,
    brand_id: Optional[str] = None,
    status: Optional[str] = None,
    q: Optional[str] = None,
) -> Dict[str, Any]:
    effective_brand_id = brand_id

    if not effective_brand_id and org_id:
        effective_brand_id = get_active_brand_for_org(org_id)

    results = filter_orders(
        org_id=org_id,
        brand_id=effective_brand_id,
        status=status,
        q=q,
    )

    ready_count = len([o for o in results if o["review_status"] == "ready"])
    needs_review_count = len([o for o in results if o["review_status"] == "needs_review"])
    blocked_count = len([o for o in results if o["review_status"] == "blocked"])
    total_amount = round(sum(o["amount"] for o in results), 2)

    return {
        "ok": True,
        "org_id": org_id,
        "brand_id": effective_brand_id,
        "brand_name": get_brand_name(effective_brand_id),
        "count": len(results),
        "summary": {
            "all": len(results),
            "ready": ready_count,
            "needs_review": needs_review_count,
            "blocked": blocked_count,
            "total_amount": total_amount,
            "currency": "USD",
        },
        "orders": results,
    }


# -------------------------
# BASIC ROUTES
# -------------------------

@app.get("/")
def root():
    return {
        "ok": True,
        "service": "opsyn-backend",
        "version": "1.0.0",
        "message": "Opsyn backend is running",
    }


@app.get("/health")
def health():
    return {
        "ok": True,
        "status": "healthy",
    }


# -------------------------
# AUTH ROUTES
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
        "resolved_brand_id": ACTIVE_BRAND.get(data.org_id),
        "allowed_brands": user["allowed_brands"],
    }


@app.get("/auth/brand-context")
def brand_context(org_id: str):
    active = ACTIVE_BRAND.get(org_id)
    active_name = get_brand_name(active)

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
            "brand_name": active_name,
            "is_default": True,
            "is_active": True,
        },
    }


@app.post("/auth/brand-context/select")
def select_brand(data: BrandSelectRequest):
    if data.brand_id not in BRANDS:
        raise HTTPException(status_code=404, detail="Brand not found")

    ACTIVE_BRAND[data.org_id] = data.brand_id

    return {
        "ok": True,
        "resolved_brand_id": data.brand_id,
        "source": "manual_switch",
    }


# -------------------------
# ORDERS ROUTES
# -------------------------

@app.get("/orders")
def get_orders(
    org_id: Optional[str] = Query(default=None),
    brand_id: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
):
    return build_orders_response(
        org_id=org_id,
        brand_id=brand_id,
        status=status,
        q=q,
    )


@app.get("/api/orders")
def get_orders_api(
    org_id: Optional[str] = Query(default=None),
    brand_id: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
):
    return build_orders_response(
        org_id=org_id,
        brand_id=brand_id,
        status=status,
        q=q,
    )


@app.get("/api/v1/orders")
def get_orders_api_v1(
    org_id: Optional[str] = Query(default=None),
    brand_id: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
):
    return build_orders_response(
        org_id=org_id,
        brand_id=brand_id,
        status=status,
        q=q,
    )


# -------------------------
# LEAFLINK SYNC ROUTES
# -------------------------

@app.get("/sync/leaflink")
def sync_leaflink_status(
    org_id: Optional[str] = Query(default=None),
    brand_id: Optional[str] = Query(default=None),
):
    effective_brand_id = brand_id or (get_active_brand_for_org(org_id) if org_id else None)

    return {
        "ok": True,
        "status": "idle",
        "message": "LeafLink sync route is live",
        "org_id": org_id,
        "brand_id": effective_brand_id,
        "brand_name": get_brand_name(effective_brand_id),
        "last_synced_at": None,
    }


@app.post("/sync/leaflink")
def sync_leaflink(data: SyncLeaflinkRequest):
    effective_brand_id = data.brand_id or (
        get_active_brand_for_org(data.org_id) if data.org_id else None
    )

    orders = filter_orders(
        org_id=data.org_id,
        brand_id=effective_brand_id,
    )

    return {
        "ok": True,
        "status": "completed",
        "message": "Temporary mock sync completed",
        "org_id": data.org_id,
        "brand_id": effective_brand_id,
        "brand_name": get_brand_name(effective_brand_id),
        "synced_order_count": len(orders),
    }