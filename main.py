from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime

app = FastAPI(title="Opsyn Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Multi-tenant in-memory stores for testing
ORDERS_STORE: Dict[str, Dict[str, dict]] = {}
ROUTES_STORE: Dict[str, Dict[str, dict]] = {}
SNAPSHOT_STORE: Dict[str, dict] = {}
ERROR_LOGS: List[dict] = []


def now_iso() -> str:
    return datetime.utcnow().isoformat()


def ensure_brand_partition(brand_id: str):
    if brand_id not in ORDERS_STORE:
        ORDERS_STORE[brand_id] = {}
    if brand_id not in ROUTES_STORE:
        ROUTES_STORE[brand_id] = {}


def require_brand_id(brand_id: Optional[str]) -> str:
    if not brand_id or not brand_id.strip():
        raise HTTPException(status_code=400, detail="brand_id is required")
    return brand_id.strip()


class OrderItem(BaseModel):
    product_name: str = ""
    sku: str = ""
    quantity: float = 0
    price: float = 0


class Order(BaseModel):
    brand_id: str
    order_id: str
    dispensary_name: str = ""
    address: str = ""
    city: str = ""
    state: str = ""
    zip: str = ""
    contact_name: str = ""
    contact_phone: str = ""
    delivery_date: str = ""
    status: str = "pending"
    total: float = 0
    items: List[OrderItem] = Field(default_factory=list)
    updated_at: Optional[str] = None


class RouteStop(BaseModel):
    order_id: str
    dispensary_name: str = ""
    address: str = ""
    city: str = ""
    state: str = ""
    zip: str = ""
    eta: str = ""
    status: str = "pending"


class Route(BaseModel):
    brand_id: str
    route_id: str
    driver_name: str = "Unassigned"
    date: str = ""
    stops: List[RouteStop] = Field(default_factory=list)
    updated_at: Optional[str] = None


class SnapshotPayload(BaseModel):
    brand_id: str
    orders: List[Order] = Field(default_factory=list)
    routes: List[Route] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ErrorPayload(BaseModel):
    brand_id: Optional[str] = None
    source: str = "twin"
    message: str
    details: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[str] = None


class RouteUpdatePayload(BaseModel):
    brand_id: str
    route_id: str
    order_id: str
    status: str


@app.get("/")
def root():
    return {
        "ok": True,
        "service": "opsyn-backend",
        "message": "Opsyn backend is running",
        "brands_loaded": list(ORDERS_STORE.keys()),
    }


@app.get("/health")
def health():
    return {
        "ok": True,
        "service": "opsyn-backend",
    }


@app.get("/brands")
def get_brands():
    brands = sorted(set(list(ORDERS_STORE.keys()) + list(ROUTES_STORE.keys()) + list(SNAPSHOT_STORE.keys())))
    return {
        "count": len(brands),
        "brands": brands,
    }


@app.get("/dashboard")
def dashboard(brand_id: str = Query(...)):
    brand_id = require_brand_id(brand_id)
    ensure_brand_partition(brand_id)

    return {
        "ok": True,
        "brand_id": brand_id,
        "orders_count": len(ORDERS_STORE.get(brand_id, {})),
        "routes_count": len(ROUTES_STORE.get(brand_id, {})),
        "last_snapshot": SNAPSHOT_STORE.get(brand_id),
    }


@app.post("/api/twin/leaflink/snapshot")
def twin_leaflink_snapshot(payload: SnapshotPayload):
    brand_id = require_brand_id(payload.brand_id)
    ensure_brand_partition(brand_id)
    received_at = now_iso()

    SNAPSHOT_STORE[brand_id] = {
        "brand_id": brand_id,
        "received_at": received_at,
        "order_count": len(payload.orders),
        "route_count": len(payload.routes),
        "metadata": payload.metadata,
    }

    for order in payload.orders:
        if order.brand_id != brand_id:
            raise HTTPException(status_code=400, detail="Order brand_id does not match snapshot brand_id")
        order_data = order.model_dump()
        order_data["updated_at"] = received_at
        ORDERS_STORE[brand_id][order.order_id] = order_data

    for route in payload.routes:
        if route.brand_id != brand_id:
            raise HTTPException(status_code=400, detail="Route brand_id does not match snapshot brand_id")
        route_data = route.model_dump()
        route_data["updated_at"] = received_at
        ROUTES_STORE[brand_id][route.route_id] = route_data

    return {
        "success": True,
        "brand_id": brand_id,
        "message": "Snapshot processed",
        "orders_upserted": len(payload.orders),
        "routes_upserted": len(payload.routes),
        "received_at": received_at,
    }


@app.get("/api/twin/leaflink/snapshot")
def get_last_snapshot(brand_id: str = Query(...)):
    brand_id = require_brand_id(brand_id)
    return SNAPSHOT_STORE.get(brand_id, {
        "brand_id": brand_id,
        "received_at": None,
        "order_count": 0,
        "route_count": 0,
        "metadata": {}
    })


@app.post("/api/twin/leaflink/error")
def twin_leaflink_error(payload: ErrorPayload):
    error_entry = {
        "brand_id": payload.brand_id,
        "source": payload.source,
        "message": payload.message,
        "details": payload.details,
        "created_at": payload.created_at or now_iso(),
    }
    ERROR_LOGS.append(error_entry)

    return {
        "success": True,
        "logged": True,
        "error": error_entry,
    }


@app.post("/orders")
def upsert_order(order: Order):
    brand_id = require_brand_id(order.brand_id)
    ensure_brand_partition(brand_id)

    order_data = order.model_dump()
    order_data["updated_at"] = now_iso()
    ORDERS_STORE[brand_id][order.order_id] = order_data

    return {
        "success": True,
        "brand_id": brand_id,
        "order_id": order.order_id,
        "updated_at": order_data["updated_at"],
    }


@app.get("/orders")
def get_orders(brand_id: str = Query(...)):
    brand_id = require_brand_id(brand_id)
    ensure_brand_partition(brand_id)

    return {
        "brand_id": brand_id,
        "count": len(ORDERS_STORE[brand_id]),
        "orders": list(ORDERS_STORE[brand_id].values())
    }


@app.get("/orders/{order_id}")
def get_order(order_id: str, brand_id: str = Query(...)):
    brand_id = require_brand_id(brand_id)
    ensure_brand_partition(brand_id)

    order = ORDERS_STORE[brand_id].get(order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order


@app.get("/api/orders")
def get_orders_api_alias(brand_id: str = Query(...)):
    return get_orders(brand_id=brand_id)


@app.post("/routes")
def upsert_route(route: Route):
    brand_id = require_brand_id(route.brand_id)
    ensure_brand_partition(brand_id)

    route_data = route.model_dump()
    route_data["updated_at"] = now_iso()
    ROUTES_STORE[brand_id][route.route_id] = route_data

    return {
        "success": True,
        "brand_id": brand_id,
        "route_id": route.route_id,
        "updated_at": route_data["updated_at"],
    }


@app.get("/routes")
def get_routes(brand_id: str = Query(...)):
    brand_id = require_brand_id(brand_id)
    ensure_brand_partition(brand_id)

    return {
        "brand_id": brand_id,
        "count": len(ROUTES_STORE[brand_id]),
        "routes": list(ROUTES_STORE[brand_id].values())
    }


@app.get("/routes/{route_id}")
def get_route(route_id: str, brand_id: str = Query(...)):
    brand_id = require_brand_id(brand_id)
    ensure_brand_partition(brand_id)

    route = ROUTES_STORE[brand_id].get(route_id)
    if not route:
        raise HTTPException(status_code=404, detail="Route not found")
    return route


@app.get("/api/routes")
def get_routes_api_alias(brand_id: str = Query(...)):
    return get_routes(brand_id=brand_id)


@app.post("/routes/update")
def update_route_status(payload: RouteUpdatePayload):
    brand_id = require_brand_id(payload.brand_id)
    ensure_brand_partition(brand_id)

    route = ROUTES_STORE[brand_id].get(payload.route_id)
    if not route:
        raise HTTPException(status_code=404, detail="Route not found")

    stop_found = False
    for stop in route.get("stops", []):
        if stop.get("order_id") == payload.order_id:
            stop["status"] = payload.status
            stop_found = True
            break

    if not stop_found:
        raise HTTPException(status_code=404, detail="Stop/order not found in route")

    route["updated_at"] = now_iso()
    ROUTES_STORE[brand_id][payload.route_id] = route

    if payload.order_id in ORDERS_STORE[brand_id]:
        ORDERS_STORE[brand_id][payload.order_id]["status"] = payload.status
        ORDERS_STORE[brand_id][payload.order_id]["updated_at"] = now_iso()

    return {
        "success": True,
        "brand_id": brand_id,
        "route_id": payload.route_id,
        "order_id": payload.order_id,
        "status": payload.status,
        "updated_at": route["updated_at"],
    }


@app.post("/api/ops/routes")
def api_ops_routes_alias(route: Route):
    return upsert_route(route)


@app.get("/api/ops/routes")
def api_ops_routes_get_alias(brand_id: str = Query(...)):
    return get_routes(brand_id=brand_id)


@app.get("/api/driver/route")
def api_driver_route_alias(brand_id: str = Query(...)):
    return get_routes(brand_id=brand_id)


@app.get("/debug/errors")
def get_errors():
    return {
        "count": len(ERROR_LOGS),
        "errors": ERROR_LOGS[-100:]
    }


@app.get("/debug/seed")
def seed_demo_data(brand_id: str = Query("demo_brand")):
    brand_id = require_brand_id(brand_id)
    ensure_brand_partition(brand_id)

    sample_order = {
        "brand_id": brand_id,
        "order_id": "demo_order_1",
        "dispensary_name": "Demo Dispensary",
        "address": "123 Main St",
        "city": "Oklahoma City",
        "state": "OK",
        "zip": "73101",
        "contact_name": "Store Manager",
        "contact_phone": "405-555-1111",
        "delivery_date": "2026-04-13",
        "status": "scheduled",
        "total": 2500,
        "items": [
            {
                "product_name": "Jefe 5G Disposable",
                "sku": "JEFE-5G-001",
                "quantity": 25,
                "price": 100,
            }
        ],
        "updated_at": now_iso(),
    }

    sample_route = {
        "brand_id": brand_id,
        "route_id": "route_20260413_okc",
        "driver_name": "Unassigned",
        "date": "2026-04-13",
        "stops": [
            {
                "order_id": "demo_order_1",
                "dispensary_name": "Demo Dispensary",
                "address": "123 Main St",
                "city": "Oklahoma City",
                "state": "OK",
                "zip": "73101",
                "eta": "10:00 AM",
                "status": "pending",
            }
        ],
        "updated_at": now_iso(),
    }

    ORDERS_STORE[brand_id][sample_order["order_id"]] = sample_order
    ROUTES_STORE[brand_id][sample_route["route_id"]] = sample_route

    SNAPSHOT_STORE[brand_id] = {
        "brand_id": brand_id,
        "received_at": now_iso(),
        "order_count": len(ORDERS_STORE[brand_id]),
        "route_count": len(ROUTES_STORE[brand_id]),
        "metadata": {"source": "debug_seed"},
    }

    return {
        "success": True,
        "brand_id": brand_id,
        "message": "Demo data seeded",
        "orders_count": len(ORDERS_STORE[brand_id]),
        "routes_count": len(ROUTES_STORE[brand_id]),
    }
