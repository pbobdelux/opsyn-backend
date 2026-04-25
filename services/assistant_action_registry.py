"""
Action registry for the Opsyn AI assistant.

Each registered action declares its name, description, risk level,
confirmation requirement, allowed roles, and async handler function.

Risk levels:
  safe   — read-only, no side effects; executed immediately
  medium — sends notifications, creates records, changes non-critical state
  high   — submits to external systems, voids/transfers inventory, deletes records

Handlers that are not yet wired to a real implementation return a
structured stub response so the registry is always complete and callable.
"""

import logging
from typing import Any, Callable

from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger("assistant_action_registry")

# ---------------------------------------------------------------------------
# Stub handler
# ---------------------------------------------------------------------------

_STUB = {
    "ok": False,
    "needs_integration": True,
    "message": "This action is registered but not connected yet.",
}


async def _stub_handler(db: AsyncSession, org_id: str, payload: dict) -> dict:
    return _STUB.copy()


# ---------------------------------------------------------------------------
# Wired handlers
# ---------------------------------------------------------------------------


async def _handle_sync_leaflink_orders(
    db: AsyncSession, org_id: str, payload: dict
) -> dict:
    """Delegate to the real LeafLink sync service."""
    try:
        from services.leaflink_sync import sync_leaflink_orders

        brand_id = payload.get("brand_id") or org_id
        return await sync_leaflink_orders(db, brand_id)
    except Exception as exc:
        logger.exception("sync_leaflink_orders handler failed")
        return {"ok": False, "error": str(exc)}


async def _handle_get_orders_needing_review(
    db: AsyncSession, org_id: str, payload: dict
) -> dict:
    """Return orders with review_status = 'needs_review' or 'blocked'."""
    try:
        from sqlalchemy import select

        from models import Order

        result = await db.execute(
            select(Order)
            .where(
                Order.brand_id == org_id,
                Order.review_status.in_(["needs_review", "blocked"]),
            )
            .order_by(Order.updated_at.desc())
            .limit(50)
        )
        orders = result.scalars().all()
        return {
            "ok": True,
            "count": len(orders),
            "orders": [
                {
                    "id": o.id,
                    "order_number": o.order_number,
                    "customer_name": o.customer_name,
                    "status": o.status,
                    "review_status": o.review_status,
                    "amount": float(o.amount) if o.amount else None,
                }
                for o in orders
            ],
        }
    except Exception as exc:
        logger.exception("get_orders_needing_review handler failed")
        return {"ok": False, "error": str(exc)}


async def _handle_get_orders_needing_packed(
    db: AsyncSession, org_id: str, payload: dict
) -> dict:
    """Return orders with status = 'approved' that need packing."""
    try:
        from sqlalchemy import select

        from models import Order

        result = await db.execute(
            select(Order)
            .where(
                Order.brand_id == org_id,
                Order.status == "approved",
            )
            .order_by(Order.updated_at.desc())
            .limit(50)
        )
        orders = result.scalars().all()
        return {
            "ok": True,
            "count": len(orders),
            "orders": [
                {
                    "id": o.id,
                    "order_number": o.order_number,
                    "customer_name": o.customer_name,
                    "status": o.status,
                    "review_status": o.review_status,
                    "amount": float(o.amount) if o.amount else None,
                }
                for o in orders
            ],
        }
    except Exception as exc:
        logger.exception("get_orders_needing_packed handler failed")
        return {"ok": False, "error": str(exc)}


async def _handle_get_blocked_orders(
    db: AsyncSession, org_id: str, payload: dict
) -> dict:
    """Return orders with review_status = 'blocked'."""
    try:
        from sqlalchemy import select

        from models import Order

        result = await db.execute(
            select(Order)
            .where(
                Order.brand_id == org_id,
                Order.review_status == "blocked",
            )
            .order_by(Order.updated_at.desc())
            .limit(50)
        )
        orders = result.scalars().all()
        return {
            "ok": True,
            "count": len(orders),
            "orders": [
                {
                    "id": o.id,
                    "order_number": o.order_number,
                    "customer_name": o.customer_name,
                    "status": o.status,
                    "review_status": o.review_status,
                    "amount": float(o.amount) if o.amount else None,
                }
                for o in orders
            ],
        }
    except Exception as exc:
        logger.exception("get_blocked_orders handler failed")
        return {"ok": False, "error": str(exc)}


async def _handle_summarize_sales(
    db: AsyncSession, org_id: str, payload: dict
) -> dict:
    """Return a basic sales summary for the org."""
    try:
        from sqlalchemy import func, select

        from models import Order

        result = await db.execute(
            select(
                func.count(Order.id).label("total_orders"),
                func.sum(Order.total_cents).label("total_cents"),
            ).where(Order.brand_id == org_id)
        )
        row = result.one()
        total_orders = row.total_orders or 0
        total_cents = row.total_cents or 0
        return {
            "ok": True,
            "total_orders": total_orders,
            "total_revenue": round(total_cents / 100, 2),
            "currency": "USD",
        }
    except Exception as exc:
        logger.exception("summarize_sales handler failed")
        return {"ok": False, "error": str(exc)}


async def _handle_add_driver(
    db: AsyncSession, org_id: str, payload: dict
) -> dict:
    """Create a new driver for the org."""
    try:
        from models import Driver

        required = ["name", "email", "pin"]
        missing = [f for f in required if not payload.get(f)]
        if missing:
            return {
                "ok": False,
                "error": f"Missing required fields: {', '.join(missing)}",
            }

        pin = str(payload["pin"])
        if len(pin) != 4 or not pin.isdigit():
            return {"ok": False, "error": "PIN must be exactly 4 numeric digits"}

        driver = Driver(
            org_id=org_id,
            name=payload["name"],
            email=payload["email"],
            phone=payload.get("phone"),
            license_plate=payload.get("license_plate"),
            status=payload.get("status", "available"),
            pin=pin,
        )
        db.add(driver)
        await db.flush()
        await db.refresh(driver)
        return {
            "ok": True,
            "driver": {
                "id": driver.id,
                "name": driver.name,
                "email": driver.email,
                "status": driver.status,
            },
        }
    except Exception as exc:
        logger.exception("add_driver handler failed")
        return {"ok": False, "error": str(exc)}


# ---------------------------------------------------------------------------
# Action registry definition
# ---------------------------------------------------------------------------


class ActionDefinition:
    """Descriptor for a single registered assistant action."""

    __slots__ = (
        "name",
        "description",
        "risk_level",
        "requires_confirmation",
        "allowed_roles",
        "handler",
        "audit_required",
    )

    def __init__(
        self,
        name: str,
        description: str,
        risk_level: str,
        requires_confirmation: bool,
        allowed_roles: list[str],
        handler: Callable,
        audit_required: bool = True,
    ):
        self.name = name
        self.description = description
        self.risk_level = risk_level
        self.requires_confirmation = requires_confirmation
        self.allowed_roles = allowed_roles
        self.handler = handler
        self.audit_required = audit_required

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "risk_level": self.risk_level,
            "requires_confirmation": self.requires_confirmation,
            "allowed_roles": self.allowed_roles,
            "audit_required": self.audit_required,
        }


_REGISTRY: dict[str, ActionDefinition] = {}


def _register(action: ActionDefinition) -> None:
    _REGISTRY[action.name] = action


# ---------------------------------------------------------------------------
# Register all actions
# ---------------------------------------------------------------------------

_register(ActionDefinition(
    name="sync_leaflink_orders",
    description="Pull the latest orders from LeafLink and sync them to the database.",
    risk_level="high",
    requires_confirmation=True,
    allowed_roles=["admin", "manager", "operator"],
    handler=_handle_sync_leaflink_orders,
))

_register(ActionDefinition(
    name="get_orders_needing_review",
    description="List orders that need review or have mapping issues.",
    risk_level="safe",
    requires_confirmation=False,
    allowed_roles=["admin", "manager", "operator", "viewer"],
    handler=_handle_get_orders_needing_review,
))

_register(ActionDefinition(
    name="get_orders_needing_packed",
    description="List approved orders that are ready to be packed.",
    risk_level="safe",
    requires_confirmation=False,
    allowed_roles=["admin", "manager", "operator", "viewer"],
    handler=_handle_get_orders_needing_packed,
))

_register(ActionDefinition(
    name="get_blocked_orders",
    description="List orders that are blocked due to compliance or mapping issues.",
    risk_level="safe",
    requires_confirmation=False,
    allowed_roles=["admin", "manager", "operator", "viewer"],
    handler=_handle_get_blocked_orders,
))

_register(ActionDefinition(
    name="summarize_sales",
    description="Provide a summary of sales totals and order counts.",
    risk_level="safe",
    requires_confirmation=False,
    allowed_roles=["admin", "manager", "operator", "viewer"],
    handler=_handle_summarize_sales,
))

_register(ActionDefinition(
    name="build_route_plan",
    description="Build an optimised delivery route plan for today's orders.",
    risk_level="medium",
    requires_confirmation=False,
    allowed_roles=["admin", "manager", "operator"],
    handler=_stub_handler,
))

_register(ActionDefinition(
    name="assign_driver",
    description="Assign a driver to a delivery route.",
    risk_level="medium",
    requires_confirmation=True,
    allowed_roles=["admin", "manager"],
    handler=_stub_handler,
))

_register(ActionDefinition(
    name="add_driver",
    description="Create a new driver record for the organisation.",
    risk_level="medium",
    requires_confirmation=True,
    allowed_roles=["admin", "manager"],
    handler=_handle_add_driver,
))

_register(ActionDefinition(
    name="update_driver_pin",
    description="Update the PIN for an existing driver.",
    risk_level="medium",
    requires_confirmation=True,
    allowed_roles=["admin", "manager"],
    handler=_stub_handler,
))

_register(ActionDefinition(
    name="email_driver_pin",
    description="Email a driver their current PIN.",
    risk_level="medium",
    requires_confirmation=True,
    allowed_roles=["admin", "manager"],
    handler=_stub_handler,
))

_register(ActionDefinition(
    name="contact_driver",
    description="Send a message or notification to a driver.",
    risk_level="medium",
    requires_confirmation=False,
    allowed_roles=["admin", "manager", "operator"],
    handler=_stub_handler,
))

_register(ActionDefinition(
    name="notify_route_change",
    description="Notify a driver of a change to their route.",
    risk_level="medium",
    requires_confirmation=True,
    allowed_roles=["admin", "manager"],
    handler=_stub_handler,
))

_register(ActionDefinition(
    name="check_metrc_package",
    description="Look up a METRC package by tag or ID.",
    risk_level="safe",
    requires_confirmation=False,
    allowed_roles=["admin", "manager", "operator", "compliance", "viewer"],
    handler=_stub_handler,
))

_register(ActionDefinition(
    name="prepare_manifest",
    description="Prepare and submit a METRC transfer manifest.",
    risk_level="high",
    requires_confirmation=True,
    allowed_roles=["admin", "manager", "compliance"],
    handler=_stub_handler,
))

_register(ActionDefinition(
    name="run_inspection_readiness_check",
    description="Run a compliance inspection readiness check and return issues.",
    risk_level="safe",
    requires_confirmation=False,
    allowed_roles=["admin", "manager", "operator", "compliance", "viewer"],
    handler=_stub_handler,
))

_register(ActionDefinition(
    name="create_compliance_task_list",
    description="Generate a prioritised compliance task list for the organisation.",
    risk_level="safe",
    requires_confirmation=False,
    allowed_roles=["admin", "manager", "compliance", "viewer"],
    handler=_stub_handler,
))

_register(ActionDefinition(
    name="draft_corrective_action",
    description="Draft a corrective action plan (CAPA) for a compliance issue.",
    risk_level="medium",
    requires_confirmation=False,
    allowed_roles=["admin", "manager", "compliance"],
    handler=_stub_handler,
))

_register(ActionDefinition(
    name="explain_order_status",
    description="Explain the current status and any blockers for a specific order.",
    risk_level="safe",
    requires_confirmation=False,
    allowed_roles=["admin", "manager", "operator", "viewer"],
    handler=_stub_handler,
))

_register(ActionDefinition(
    name="explain_compliance_risk",
    description="Explain a compliance risk or regulatory requirement in plain language.",
    risk_level="safe",
    requires_confirmation=False,
    allowed_roles=["admin", "manager", "operator", "compliance", "viewer"],
    handler=_stub_handler,
))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_action(name: str) -> ActionDefinition | None:
    """Return the ActionDefinition for the given name, or None."""
    return _REGISTRY.get(name)


def list_actions() -> list[dict[str, Any]]:
    """Return all registered actions as serialisable dicts."""
    return [action.to_dict() for action in _REGISTRY.values()]


def get_registry() -> dict[str, ActionDefinition]:
    """Return the raw registry dict (name → ActionDefinition)."""
    return _REGISTRY


async def execute_action(
    db: AsyncSession,
    action_name: str,
    org_id: str,
    payload: dict,
) -> dict[str, Any]:
    """
    Execute a registered action by name.

    Returns the handler result dict. If the action is not found, returns
    an error dict. Never raises.
    """
    action = get_action(action_name)
    if action is None:
        return {"ok": False, "error": f"Unknown action: {action_name}"}
    try:
        return await action.handler(db, org_id, payload)
    except Exception as exc:
        logger.exception("execute_action: handler for %s raised", action_name)
        return {"ok": False, "error": str(exc)}
