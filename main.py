import os
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException, Query, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict, Any

from leaflink_client import LeafLinkClient


app = FastAPI(title="Opsyn Backend", version="1.0.0")


# -------------------------
# CONFIG
# -------------------------

OPSYN_SYNC_SECRET = os.getenv("OPSYN_SYNC_SECRET", "8fj29f8j29fj29fj2fj29f").strip()


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

ORDERS: List[Dict[str, Any]] = [
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
        "source": "mock",
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
        "source": "mock",
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
        "source": "mock",
    },
]

SYNC_STATE: Dict[str, Dict[str, Any]] = {}


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

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_active_brand_for_org(org_id: str) -> Optional[str]:
    return ACTIVE_BRAND.get(org_id)


def get_brand_name(brand_id: Optional[str]) -> Optional[str]:
    if not brand_id:
        return None
    brand = BRANDS.get(brand_id)
    return brand["brand_name"] if brand else brand_id


def sync_key(org_id: str, brand_id: str) -> str:
    return f"{org_id}:{brand_id}"


def normalize_leaflink_order(raw: Dict[str, Any], org_id: str, brand_id: str) -> Dict[str, Any]:
    external_id = str(
        raw.get("id")
        or raw.get("uuid")
        or raw.get("number")
        or raw.get("short_id")
        or now_iso()
    )

    customer = raw.get("buyer") or raw.get("customer") or {}
    customer_name = (
        customer.get("display_name")
        or customer.get("name")
        or raw.get("customer_name")
        or raw.get("dispensary_name")
        or "Unknown Customer"
    )

    raw_status = str(raw.get("status") or "unknown").lower()

    if raw_status in {"draft", "submitted", "accepted", "confirmed", "ready"}:
        review_status = "ready"
    elif raw_status in {"hold", "review", "pending", "needs_review"}:
        review_status = "needs_review"
    elif raw_status in {"cancelled", "rejected", "blocked"}:
        review_status = "blocked"
    else:
        review_status = "ready"

    amount = (
        raw.get("total")
        or raw.get("total_amount")
        or raw.get("subtotal")
        or raw.get("amount")
        or 0
    )

    created_at = raw.get("created_at") or raw.get("created") or now_iso()
    updated_at = raw.get("updated_at") or raw.get("modified") or created_at
    order_number = str(raw.get("number") or raw.get("short_id") or external_id)

    return {
        "id": f"leaflink_{external_id}",
        "order_number": order_number,
        "org_id": org_id,
        "brand_id": brand_id,
        "customer_name": customer_name,
        "status": raw_status,
        "review_status": review_status,
        "amount": float(amount or 0),
        "currency": raw.get("currency") or "USD",
        "created_at": created_at,
        "updated_at": updated_at,
        "source": "leaflink",
        "raw": raw,
    }


def replace_orders_for_brand(org_id: str, brand_id: str, new_orders: List[Dict[str, Any]]) -> int:
    global ORDERS

    kept = [
        o for o in ORDERS
        if not (
            o.get("org_id") == org_id
            and o.get("brand_id") == brand_id
            and o.get("source") == "leaflink"
        )
    ]

    ORDERS = kept + new_orders
    return len(new_orders)


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
            if str(o["status"]).lower() == normalized
            or str(o["review_status"]).lower() == normalized
        ]

    if q:
        q_lower = q.lower().strip()
        results = [
            o for o in results
            if q_lower in str(o["order_number"]).lower()
            or q_lower in str(o["customer_name"]).lower()
            or q_lower in str(o["status"]).lower()
            or q_lower in str(o["review_status"]).lower()
        ]

    results = sorted(results, key=lambda x: str(x["updated_at"]), reverse=True)
    return results


def build_orders_response(
    org_id: Optional[str] = None,
    brand_id: Optional[str] = None,
    status: Optional[str] = None,
    q: Optional[str] = None,
) -> Dict[str, Any]:
    if not org_id:
        org_id = "org_onboarding"

    if not brand_id:
        brand_id = get_active_brand_for_org(org_id)

    results = filter_orders(
        org_id=org_id,
        brand_id=brand_id,
        status=status,
        q=q,
    )

    ready_count = len([o for o in results if o["review_status"] == "ready"])
    needs_review_count = len([o for o in results if o["review_status"] == "needs_review"])
    blocked_count = len([o for o in results if o["review_status"] == "blocked"])
    total_amount = round(sum(float(o["amount"]) for o in results), 2)

    sync_meta = SYNC_STATE.get(
        sync_key(org_id, brand_id),
        {
            "status": "idle",
            "message": "No sync has run yet",
            "last_synced_at": None,
        },
    )

    return {
        "ok": True,
        "org_id": org_id,
        "brand_id": brand_id,
        "brand_name": get_brand_name(brand_id),
        "count": len(results),
        "summary": {
            "all": len(results),
            "ready": ready_count,
            "needs_review": needs_review_count,
            "blocked": blocked_count,
            "total_amount": total_amount,
            "currency": "USD",
        },
        "sync": sync_meta,
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
    return build_orders_response(org_id=org_id, brand_id=brand_id, status=status, q=q)


@app.get("/api/orders")
def get_orders_api(
    org_id: Optional[str] = Query(default=None),
    brand_id: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
):
    return build_orders_response(org_id=org_id, brand_id=brand_id, status=status, q=q)


@app.get("/api/v1/orders")
def get_orders_api_v1(
    org_id: Optional[str] = Query(default=None),
    brand_id: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
):
    return build_orders_response(org_id=org_id, brand_id=brand_id, status=status, q=q)


# -------------------------
# LEAFLINK SYNC ROUTES
# -------------------------

@app.get("/sync/leaflink")
def sync_leaflink_status(
    org_id: Optional[str] = Query(default=None),
    brand_id: Optional[str] = Query(default=None),
):
    effective_org_id = org_id or "org_onboarding"
    effective_brand_id = brand_id or get_active_brand_for_org(effective_org_id)

    state = SYNC_STATE.get(
        sync_key(effective_org_id, effective_brand_id),
        {
            "status": "idle",
            "message": "LeafLink sync route is live",
            "last_synced_at": None,
        },
    )

    return {
        "ok": True,
        "org_id": effective_org_id,
        "brand_id": effective_brand_id,
        "brand_name": get_brand_name(effective_brand_id),
        **state,
    }


@app.post("/sync/leaflink")
def sync_leaflink(data: SyncLeaflinkRequest):
    effective_org_id = data.org_id or "org_onboarding"
    effective_brand_id = data.brand_id or get_active_brand_for_org(effective_org_id)

    orders = filter_orders(
        org_id=effective_org_id,
        brand_id=effective_brand_id,
    )

    return {
        "ok": True,
        "status": "completed",
        "message": "Temporary mock sync completed",
        "org_id": effective_org_id,
        "brand_id": effective_brand_id,
        "brand_name": get_brand_name(effective_brand_id),
        "synced_order_count": len(orders),
    }


@app.post("/sync/leaflink/run")
def run_leaflink_sync(
    org_id: str = Query(default="org_onboarding"),
    brand_id: Optional[str] = Query(default=None),
    x_opsyn_secret: Optional[str] = Header(default=None),
):
    if x_opsyn_secret != OPSYN_SYNC_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    effective_brand_id = brand_id or get_active_brand_for_org(org_id)
    key = sync_key(org_id, effective_brand_id)

    SYNC_STATE[key] = {
        "status": "syncing",
        "message": "LeafLink sync in progress",
        "last_synced_at": SYNC_STATE.get(key, {}).get("last_synced_at"),
    }

    try:
        client = LeafLinkClient()
        raw_orders = client.fetch_recent_orders(max_pages=5)

        normalized = [
            normalize_leaflink_order(raw, org_id=org_id, brand_id=effective_brand_id)
            for raw in raw_orders
        ]

        synced_count = replace_orders_for_brand(org_id, effective_brand_id, normalized)
        finished_at = now_iso()

        SYNC_STATE[key] = {
            "status": "ok",
            "message": f"LeafLink sync completed ({synced_count} orders)",
            "last_synced_at": finished_at,
            "fetched_count": len(raw_orders),
            "synced_count": synced_count,
        }

        return {
            "ok": True,
            "status": "ok",
            "message": "LeafLink sync completed",
            "org_id": org_id,
            "brand_id": effective_brand_id,
            "brand_name": get_brand_name(effective_brand_id),
            "fetched_count": len(raw_orders),
            "synced_count": synced_count,
            "timestamp": finished_at,
        }

    except Exception as e:
        SYNC_STATE[key] = {
            "status": "error",
            "message": str(e),
            "last_synced_at": SYNC_STATE.get(key, {}).get("last_synced_at"),
        }
        raise HTTPException(status_code=500, detail=f"LeafLink sync failed: {e}")
from fastapi import Request

@app.post("/ingest/twin/orders")
async def ingest_twin_orders(
    request: Request,
    x_opsyn_secret: Optional[str] = Header(default=None),
):
    if x_opsyn_secret != OPSYN_SYNC_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    payload = await request.json()

    orders = payload.get("orders", [])

    inserted = []

    for o in orders:
        normalized = {
            "id": f"twin_{o.get('id')}",
            "order_number": o.get("order_number") or o.get("id"),
            "org_id": o.get("org_id", "org_onboarding"),
            "brand_id": o.get("brand_id", "noble-nectar"),
            "customer_name": o.get("customer_name", "Unknown"),
            "status": o.get("status", "ready"),
            "review_status": o.get("review_status", "ready"),
            "amount": float(o.get("amount", 0)),
            "currency": o.get("currency", "USD"),
            "created_at": o.get("created_at", now_iso()),
            "updated_at": o.get("updated_at", now_iso()),
            "source": "twin",
            "raw": o,
        }

        inserted.append(normalized)

    # Replace existing twin orders for this brand/org
    global ORDERS
    ORDERS = [
        o for o in ORDERS
        if o.get("source") != "twin"
    ] + inserted

    return {
        "ok": True,
        "ingested": len(inserted),
    }