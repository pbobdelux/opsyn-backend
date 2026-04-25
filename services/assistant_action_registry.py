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
        result = await sync_leaflink_orders(db, brand_id)
        # Enrich with data_source and standard fields
        result.setdefault("data_source", "leaflink")
        result.setdefault("errors", [])
        if result.get("ok") and "count" not in result:
            result["count"] = result.get("fetched", 0)
        if result.get("ok") and "summary" not in result:
            fetched = result.get("fetched", 0)
            result["summary"] = f"Synced {fetched} orders from LeafLink."
        logger.info(
            "sync_leaflink_orders org_id=%s fetched=%s data_source=leaflink",
            org_id, result.get("fetched", 0),
        )
        return result
    except Exception as exc:
        logger.exception("sync_leaflink_orders handler failed org_id=%s", org_id)
        return {"ok": False, "error": str(exc), "data_source": "leaflink", "errors": [str(exc)]}


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
        count = len(orders)
        logger.info("get_orders_needing_review org_id=%s count=%d data_source=database", org_id, count)
        return {
            "ok": True,
            "count": count,
            "data_source": "database",
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
            "summary": f"{count} order{'s' if count != 1 else ''} need{'s' if count == 1 else ''} review.",
            "errors": [],
        }
    except Exception as exc:
        logger.exception("get_orders_needing_review handler failed org_id=%s", org_id)
        return {"ok": False, "error": str(exc), "data_source": "database", "errors": [str(exc)]}


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
        count = len(orders)
        logger.info("get_orders_needing_packed org_id=%s count=%d data_source=database", org_id, count)
        return {
            "ok": True,
            "count": count,
            "data_source": "database",
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
            "summary": f"{count} order{'s' if count != 1 else ''} ready to pack.",
            "errors": [],
        }
    except Exception as exc:
        logger.exception("get_orders_needing_packed handler failed org_id=%s", org_id)
        return {"ok": False, "error": str(exc), "data_source": "database", "errors": [str(exc)]}


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
        count = len(orders)
        logger.info("get_blocked_orders org_id=%s count=%d data_source=database", org_id, count)
        return {
            "ok": True,
            "count": count,
            "data_source": "database",
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
            "summary": f"{count} order{'s' if count != 1 else ''} blocked.",
            "errors": [],
        }
    except Exception as exc:
        logger.exception("get_blocked_orders handler failed org_id=%s", org_id)
        return {"ok": False, "error": str(exc), "data_source": "database", "errors": [str(exc)]}


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
        total_revenue = round(total_cents / 100, 2)
        logger.info(
            "summarize_sales org_id=%s total_orders=%d data_source=database",
            org_id, total_orders,
        )
        return {
            "ok": True,
            "total_orders": total_orders,
            "total_revenue": total_revenue,
            "currency": "USD",
            "data_source": "database",
            "summary": f"{total_orders} total orders, ${total_revenue:.2f} revenue.",
            "errors": [],
        }
    except Exception as exc:
        logger.exception("summarize_sales handler failed org_id=%s", org_id)
        return {"ok": False, "error": str(exc), "data_source": "database", "errors": [str(exc)]}


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


async def _handle_update_driver_pin(
    db: AsyncSession, org_id: str, payload: dict
) -> dict:
    """Update the PIN for an existing driver."""
    try:
        from sqlalchemy import select

        from models import Driver

        driver_id = payload.get("driver_id")
        new_pin = str(payload.get("pin") or payload.get("new_pin") or "")

        if not driver_id:
            return {"ok": False, "error": "driver_id is required"}

        if len(new_pin) != 4 or not new_pin.isdigit():
            return {"ok": False, "error": "PIN must be exactly 4 numeric digits"}

        result = await db.execute(
            select(Driver).where(
                Driver.id == driver_id,
                Driver.org_id == org_id,
            )
        )
        driver = result.scalar_one_or_none()

        if driver is None:
            return {"ok": False, "error": f"Driver {driver_id} not found for org {org_id}"}

        driver.pin = new_pin
        await db.flush()

        logger.info(
            "update_driver_pin org_id=%s driver_id=%s — PIN updated (value not logged)",
            org_id, driver_id,
        )
        return {
            "ok": True,
            "driver_id": driver_id,
            "message": "Driver PIN updated successfully.",
        }
    except Exception as exc:
        logger.exception("update_driver_pin handler failed org_id=%s driver_id=%s", org_id, payload.get("driver_id"))
        return {"ok": False, "error": str(exc)}


async def _handle_email_driver_pin(
    db: AsyncSession, org_id: str, payload: dict
) -> dict:
    """Email a driver their current PIN via the email service."""
    try:
        from sqlalchemy import select

        from models import Driver
        from services.email_service import send_driver_pin_email

        driver_id = payload.get("driver_id")
        if not driver_id:
            return {"ok": False, "error": "driver_id is required"}

        result = await db.execute(
            select(Driver).where(
                Driver.id == driver_id,
                Driver.org_id == org_id,
            )
        )
        driver = result.scalar_one_or_none()

        if driver is None:
            return {"ok": False, "error": f"Driver {driver_id} not found for org {org_id}"}

        if not driver.email:
            return {"ok": False, "error": "Driver has no email address on file"}

        if not driver.pin:
            return {"ok": False, "error": "Driver has no PIN set"}

        logger.info(
            "email_driver_pin org_id=%s driver_id=%s email=***",
            org_id, driver_id,
        )
        email_result = await send_driver_pin_email(
            driver_email=driver.email,
            driver_name=driver.name,
            org_code=org_id,
            pin=driver.pin,
        )
        return email_result
    except Exception as exc:
        logger.exception("email_driver_pin handler failed org_id=%s", org_id)
        return {"ok": False, "error": str(exc)}


async def _handle_contact_driver(
    db: AsyncSession, org_id: str, payload: dict
) -> dict:
    """Send an SMS message to a driver."""
    try:
        from sqlalchemy import select

        from models import Driver
        from services.sms_service import send_driver_sms

        driver_id = payload.get("driver_id")
        message = payload.get("message", "")

        if not driver_id:
            return {"ok": False, "error": "driver_id is required"}

        if not message:
            return {"ok": False, "error": "message is required"}

        result = await db.execute(
            select(Driver).where(
                Driver.id == driver_id,
                Driver.org_id == org_id,
            )
        )
        driver = result.scalar_one_or_none()

        if driver is None:
            return {"ok": False, "error": f"Driver {driver_id} not found for org {org_id}"}

        if not driver.phone:
            return {"ok": False, "error": "Driver has no phone number on file"}

        logger.info(
            "contact_driver org_id=%s driver_id=%s phone=***",
            org_id, driver_id,
        )
        sms_result = await send_driver_sms(phone=driver.phone, message=message)
        return sms_result
    except Exception as exc:
        logger.exception("contact_driver handler failed org_id=%s", org_id)
        return {"ok": False, "error": str(exc)}


async def _handle_notify_route_change(
    db: AsyncSession, org_id: str, payload: dict
) -> dict:
    """Notify a driver of a route change via SMS."""
    try:
        from sqlalchemy import select

        from models import Driver
        from services.sms_service import send_driver_sms

        driver_id = payload.get("driver_id")
        change_detail = payload.get("change_detail") or payload.get("message") or "Your route has been updated."

        if not driver_id:
            return {"ok": False, "error": "driver_id is required"}

        result = await db.execute(
            select(Driver).where(
                Driver.id == driver_id,
                Driver.org_id == org_id,
            )
        )
        driver = result.scalar_one_or_none()

        if driver is None:
            return {"ok": False, "error": f"Driver {driver_id} not found for org {org_id}"}

        if not driver.phone:
            return {"ok": False, "error": "Driver has no phone number on file"}

        sms_message = f"Route update: {change_detail}"
        logger.info(
            "notify_route_change org_id=%s driver_id=%s phone=***",
            org_id, driver_id,
        )
        sms_result = await send_driver_sms(phone=driver.phone, message=sms_message)
        return sms_result
    except Exception as exc:
        logger.exception("notify_route_change handler failed org_id=%s", org_id)
        return {"ok": False, "error": str(exc)}


# ---------------------------------------------------------------------------
# METRC handlers (Phase 7 — safe MVP stubs)
# ---------------------------------------------------------------------------

_METRC_NOT_CONNECTED = {
    "ok": False,
    "needs_integration": True,
    "integration": "metrc",
    "message": "METRC integration not yet connected",
}


async def _handle_check_metrc_package(
    db: AsyncSession, org_id: str, payload: dict
) -> dict:
    """Return needs_integration until METRC is wired."""
    logger.info("check_metrc_package org_id=%s — needs_integration=metrc", org_id)
    return _METRC_NOT_CONNECTED.copy()


async def _handle_prepare_manifest(
    db: AsyncSession, org_id: str, payload: dict
) -> dict:
    """Return needs_integration until METRC is wired."""
    logger.info("prepare_manifest org_id=%s — needs_integration=metrc", org_id)
    return _METRC_NOT_CONNECTED.copy()


# ---------------------------------------------------------------------------
# Compliance handler (Phase 8)
# ---------------------------------------------------------------------------


async def _handle_run_inspection_readiness_check(
    db: AsyncSession, org_id: str, payload: dict
) -> dict:
    """Run a compliance inspection readiness check."""
    try:
        from services.compliance_service import run_inspection_readiness_check

        return await run_inspection_readiness_check(db, org_id)
    except Exception as exc:
        logger.exception("run_inspection_readiness_check handler failed org_id=%s", org_id)
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
    handler=_handle_update_driver_pin,
))

_register(ActionDefinition(
    name="email_driver_pin",
    description="Email a driver their current PIN.",
    risk_level="medium",
    requires_confirmation=True,
    allowed_roles=["admin", "manager"],
    handler=_handle_email_driver_pin,
))

_register(ActionDefinition(
    name="contact_driver",
    description="Send a message or notification to a driver.",
    risk_level="medium",
    requires_confirmation=False,
    allowed_roles=["admin", "manager", "operator"],
    handler=_handle_contact_driver,
))

_register(ActionDefinition(
    name="notify_route_change",
    description="Notify a driver of a change to their route.",
    risk_level="medium",
    requires_confirmation=True,
    allowed_roles=["admin", "manager"],
    handler=_handle_notify_route_change,
))

_register(ActionDefinition(
    name="check_metrc_package",
    description="Look up a METRC package by tag or ID.",
    risk_level="safe",
    requires_confirmation=False,
    allowed_roles=["admin", "manager", "operator", "compliance", "viewer"],
    handler=_handle_check_metrc_package,
))

_register(ActionDefinition(
    name="prepare_manifest",
    description="Prepare and submit a METRC transfer manifest.",
    risk_level="high",
    requires_confirmation=True,
    allowed_roles=["admin", "manager", "compliance"],
    handler=_handle_prepare_manifest,
))

_register(ActionDefinition(
    name="run_inspection_readiness_check",
    description="Run a compliance inspection readiness check and return issues.",
    risk_level="safe",
    requires_confirmation=False,
    allowed_roles=["admin", "manager", "operator", "compliance", "viewer"],
    handler=_handle_run_inspection_readiness_check,
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
