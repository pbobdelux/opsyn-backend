# app/routes/brand_context.py
from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

# Reuse whatever auth dependency you already use on /orders.
# If you have something like `get_current_session` that returns { user_id, org_id, role },
# import and use it. If not, fall back to headers for now.
try:
from app.auth.deps import get_current_session  # preferred
except Exception:  # pragma: no cover
get_current_session = None  # type: ignore

# Reuse the same DB accessor /orders uses.
from app.db import db  # expects an async client with .table(...) or equivalent

router = APIRouter(prefix="/auth", tags=["brand-context"])

# ---------- Schemas ----------

class BrandContextOverrideIn(BaseModel):
override_brand_id: str = Field(..., min_length=1)
org_id: Optional[str] = None

class BrandContextOverrideOut(BaseModel):
id: Optional[str] = None
org_id: Optional[str] = None
user_id: Optional[str] = None
override_brand_id: Optional[str] = None
is_active: Optional[bool] = None
created_at: Optional[str] = None
updated_at: Optional[str] = None

class BrandContextOut(BaseModel):
ok: bool = True
signed_in_org_id: Optional[str] = None
resolved_brand_id: Optional[str] = None
source: Optional[str] = None  # "override" | "signed_in_org" | "none"
override: Optional[BrandContextOverrideOut] = None

# ---------- Session resolution ----------

async def _resolve_session(request: Request):
"""
Return a dict-like { user_id, org_id, role } using whatever auth
infra this app already has. Falls back to headers for now.
"""
if get_current_session is not None:
    try:
        return await get_current_session(request)  # type: ignore
    except Exception:
        pass
# Fallback — match whatever the /orders route trusts.
org_id = request.headers.get("x-org-id") or None
user_id = request.headers.get("x-user-id") or None
role = request.headers.get("x-role") or None
return {"user_id": user_id, "org_id": org_id, "role": role}

def _is_admin(session: dict) -> bool:
role = (session or {}).get("role") or ""
return role.lower() in {"admin", "owner"}

# ---------- Storage ----------
#
# Table: brand_context_overrides
#   id               uuid primary key default gen_random_uuid()
#   org_id           text
#   user_id          text
#   override_brand_id text not null
#   is_active        boolean not null default true
#   created_at       timestamptz not null default now()
#   updated_at       timestamptz not null default now()
#
#   unique (org_id) where is_active = true
#
# Supabase SQL migration at the bottom of this file.

async def _load_active_override(org_id: Optional[str], user_id: Optional[str]):
if not org_id and not user_id:
    return None
q = db.table("brand_context_overrides").select("*").eq("is_active", True)
if user_id:
    q = q.eq("user_id", user_id)
elif org_id:
    q = q.eq("org_id", org_id)
rows = (await q.limit(1).execute()).data or []
return rows[0] if rows else None

async def _upsert_override(org_id: Optional[str], user_id: Optional[str], brand: str):
now = datetime.utcnow().isoformat() + "Z"
existing = await _load_active_override(org_id, user_id)
if existing:
    updated = (
        await db.table("brand_context_overrides")
        .update({"override_brand_id": brand, "is_active": True, "updated_at": now})
        .eq("id", existing["id"])
        .execute()
    )
    return (updated.data or [None])[0] or {**existing, "override_brand_id": brand}
inserted = (
    await db.table("brand_context_overrides")
    .insert({
        "org_id": org_id,
        "user_id": user_id,
        "override_brand_id": brand,
        "is_active": True,
        "created_at": now,
        "updated_at": now,
    })
    .execute()
)
return (inserted.data or [None])[0]

async def _deactivate_override(org_id: Optional[str], user_id: Optional[str]) -> bool:
existing = await _load_active_override(org_id, user_id)
if not existing:
    return False
now = datetime.utcnow().isoformat() + "Z"
await (
    db.table("brand_context_overrides")
    .update({"is_active": False, "updated_at": now})
    .eq("id", existing["id"])
    .execute()
)
return True

# ---------- Endpoints ----------

@router.get("/brand-context", response_model=BrandContextOut)
async def get_brand_context(request: Request):
session = await _resolve_session(request)
org_id = (session or {}).get("org_id")
user_id = (session or {}).get("user_id")

override = await _load_active_override(org_id, user_id)
if override and override.get("override_brand_id"):
    return BrandContextOut(
        ok=True,
        signed_in_org_id=org_id,
        resolved_brand_id=override["override_brand_id"],
        source="override",
        override=BrandContextOverrideOut(**override),
    )
return BrandContextOut(
    ok=True,
    signed_in_org_id=org_id,
    resolved_brand_id=org_id,
    source="signed_in_org" if org_id else "none",
    override=None,
)

@router.post("/brand-context/override", response_model=BrandContextOut)
async def set_brand_context_override(body: BrandContextOverrideIn, request: Request):
session = await _resolve_session(request)
if not _is_admin(session):
    raise HTTPException(status_code=403, detail="Admin only")

org_id = body.org_id or (session or {}).get("org_id")
user_id = (session or {}).get("user_id")
if not org_id and not user_id:
    raise HTTPException(status_code=400, detail="No org_id or user_id on session")

row = await _upsert_override(org_id, user_id, body.override_brand_id.strip())
return BrandContextOut(
    ok=True,
    signed_in_org_id=org_id,
    resolved_brand_id=body.override_brand_id.strip(),
    source="override",
    override=BrandContextOverrideOut(**(row or {})),
)

@router.delete("/brand-context/override")
async def clear_brand_context_override(request: Request):
session = await _resolve_session(request)
if not _is_admin(session):
    raise HTTPException(status_code=403, detail="Admin only")
org_id = (session or {}).get("org_id")
user_id = (session or {}).get("user_id")
cleared = await _deactivate_override(org_id, user_id)
return {"ok": True, "cleared": cleared}

# ---------- main.py registration ----------
#
# from app.routes.brand_context import router as brand_context_router
# app.include_router(brand_context_router)