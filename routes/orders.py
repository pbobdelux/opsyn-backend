from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from database import get_db

router = APIRouter(prefix="/orders", tags=["orders"])

@router.get("")
def get_orders(db: Session = Depends(get_db)):
    return {
        "items": [],
        "count": 0,
        "message": "Orders endpoint is live"
    }