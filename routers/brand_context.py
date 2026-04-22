from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from database import get_db
from models import OrganizationBrandBinding

router = APIRouter(prefix="/auth/brand-context", tags=["brand-context"])


@router.get("")
def get_brand_context(
    org_id: str = Query(...),
    db: Session = Depends(get_db),
):
    bindings = (
        db.query(OrganizationBrandBinding)
        .filter(
            OrganizationBrandBinding.org_id == org_id,
            OrganizationBrandBinding.is_active == True,
        )
        .all()
    )

    active = next((b for b in bindings if b.is_default), None)

    return {
        "ok": True,
        "org_id": org_id,
        "resolved_brand_id": active.brand_id if active else None,
        "active_binding": {
            "brand_id": active.brand_id,
            "brand_name": active.brand_name,
            "is_default": active.is_default,
            "is_active": active.is_active,
        } if active else None,
        "available_brands": [
            {
                "brand_id": b.brand_id,
                "brand_name": b.brand_name,
                "is_default": b.is_default,
                "is_active": b.is_active,
            }
            for b in bindings
        ],
    }


@router.get("/available")
def get_available_brands(
    org_id: str = Query(...),
    db: Session = Depends(get_db),
):
    bindings = (
        db.query(OrganizationBrandBinding)
        .filter(
            OrganizationBrandBinding.org_id == org_id,
            OrganizationBrandBinding.is_active == True,
        )
        .all()
    )

    return {
        "ok": True,
        "org_id": org_id,
        "available_brands": [
            {
                "brand_id": b.brand_id,
                "brand_name": b.brand_name,
                "is_default": b.is_default,
                "is_active": b.is_active,
            }
            for b in bindings
        ],
    }


@router.post("/default")
def set_default_brand(
    org_id: str,
    brand_id: str,
    db: Session = Depends(get_db),
):
    bindings = (
        db.query(OrganizationBrandBinding)
        .filter(OrganizationBrandBinding.org_id == org_id)
        .all()
    )

    for b in bindings:
        b.is_default = (b.brand_id == brand_id)

    db.commit()

    active = next((b for b in bindings if b.brand_id == brand_id), None)

    return {
        "ok": True,
        "resolved_brand_id": brand_id,
        "active_binding": {
            "brand_id": active.brand_id,
            "brand_name": active.brand_name,
            "is_default": True,
            "is_active": active.is_active,
        } if active else None,
    }