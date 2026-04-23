from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import Driver

router = APIRouter(tags=["drivers"])


def serialize_driver(driver: Driver) -> dict:
    return {
        "id": str(driver.id),
        "org_id": driver.org_id,
        "name": driver.name,
        "email": driver.email,
        "phone": driver.phone,
        "active": driver.active,
        "created_at": driver.created_at.isoformat() if driver.created_at else None,
        "updated_at": driver.updated_at.isoformat() if driver.updated_at else None,
    }


@router.post("/organizations/{org_id}/drivers", status_code=201)
async def create_driver(
    org_id: str,
    data: dict,
    db: AsyncSession = Depends(get_db),
):
    if not org_id or not org_id.strip():
        raise HTTPException(status_code=400, detail="org_id is required")

    name = data.get("name", "").strip() if data.get("name") else ""
    if not name:
        raise HTTPException(status_code=400, detail="name is required and must be non-empty")

    driver = Driver(
        org_id=org_id,
        name=name,
        email=data.get("email"),
        phone=data.get("phone"),
        active=data.get("active", True),
    )

    db.add(driver)
    await db.commit()
    await db.refresh(driver)

    return serialize_driver(driver)


@router.get("/organizations/{org_id}/drivers")
async def list_drivers(
    org_id: str,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    count_stmt = select(func.count(Driver.id)).where(Driver.org_id == org_id)
    count_result = await db.execute(count_stmt)
    total = count_result.scalar_one()

    query_stmt = (
        select(Driver)
        .where(Driver.org_id == org_id)
        .order_by(Driver.created_at.desc())
        .limit(limit)
        .offset(offset)
    )
    result = await db.execute(query_stmt)
    drivers = result.scalars().all()

    return {
        "drivers": [serialize_driver(d) for d in drivers],
        "total": total,
        "limit": limit,
        "offset": offset,
    }
