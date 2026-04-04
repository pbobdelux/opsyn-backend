import uuid
import time
from typing import Dict, Any

from config import config
from sessions import store, Session


def get_mock_package(tag: str) -> Dict[str, Any]:
    for p in config.MOCK_PACKAGES:
        if p["tag"] == tag:
            return p
    return {
        "tag": tag,
        "item": f"Unknown Package {tag[-5:]}",
        "qty": 20.0,
        "uom": "Units",
        "category": "vape",
        "batch": "UNKNOWN",
    }


def get_sampling_rule(category: str) -> Dict[str, Any]:
    return config.SAMPLING_RULES.get(
        category,
        {"test_qty": 1.0, "reserve_qty": 1.0, "unit": "Units"},
    )


def plan_testing_session(session: Session) -> None:
    tags = session.data.get("scanned_tags", [])
    lab = session.answers.get("q_lab", "Scissortail Labs")
    driver = session.answers.get("q_driver", "Preston Anderson")

    steps = []
    warnings = []
    step_id = 1

    for tag in tags:
        pkg = get_mock_package(tag)
        rule = get_sampling_rule(pkg["category"])
        total_sample = rule["test_qty"] + rule["reserve_qty"]

        if pkg["qty"] < total_sample:
            warnings.append(
                {
                    "type": "low_inventory",
                    "message": (
                        f"Package {tag} has {pkg['qty']} {pkg['uom']} "
                        f"but needs {total_sample} {rule['unit']} for testing/reserve"
                    ),
                    "severity": "warning",
                    "affected_item_ids": [tag],
                    "recommended_action": "Reduce sample size or skip",
                }
            )

        steps.append(
            {
                "step_id": step_id,
                "action": "create_parent_tag",
                "details": {
                    "source_tag": tag,
                    "item": pkg["item"],
                    "qty": f"{pkg['qty']} {pkg['uom']}",
                    "category": pkg["category"],
                },
                "status": "pending",
            }
        )
        step_id += 1

        steps.append(
            {
                "step_id": step_id,
                "action": "create_test_sample",
                "details": {
                    "parent_tag": tag,
                    "sample_tag": "auto_assigned",
                    "qty": f"{rule['test_qty']} {rule['unit']}",
                    "lab": lab,
                },
                "status": "pending",
            }
        )
        step_id += 1

        steps.append(
            {
                "step_id": step_id,
                "action": "create_reserve_sample",
                "details": {
                    "parent_tag": tag,
                    "sample_tag": "auto_assigned",
                    "qty": f"{rule['reserve_qty']} {rule['unit']}",
                },
                "status": "pending",
            }
        )
        step_id += 1

    steps.append(
        {
            "step_id": step_id,
            "action": "generate_chain_of_custody",
            "details": {
                "lab": lab,
                "items": len(tags),
                "driver": driver,
            },
            "status": "pending",
        }
    )
    step_id += 1

    steps.append(
        {
            "step_id": step_id,
            "action": "create_transfer_to_lab",
            "details": {
                "destination": lab,
                "packages": len(tags),
            },
            "status": "pending",
        }
    )

    session.steps = steps
    session.warnings = warnings
    session.status = "plan_ready"
    session.total_steps = len(steps)
    session.data["lab"] = lab
    session.data["driver"] = driver


def execute_testing_session(session_id: str) -> None:
    session = store.get(session_id)
    if not session:
        return

    session.status = "executing"

    for i, step in enumerate(session.steps):
        time.sleep(0.3)
        step["status"] = "completed"
        step["completed_at"] = str(int(time.time()))
        session.current_step = i + 1
        store.update(session_id, current_step=i + 1, steps=session.steps)

    coc_id = str(uuid.uuid4())[:8]
    man_id = str(uuid.uuid4())[:8]

    session.documents = [
        {
            "id": f"doc_{coc_id}",
            "type": "chain_of_custody",
            "url": f"/api/documents/doc_{coc_id}/download",
            "name": f"CoC_{session.data.get('lab', 'lab')}.pdf",
        },
        {
            "id": f"doc_{man_id}",
            "type": "manifest",
            "url": f"/api/documents/doc_{man_id}/download",
            "name": f"Manifest_Transfer.pdf",
        },
    ]

    tags = session.data.get("scanned_tags", [])
    session.result = {
        "parent_tags_created": len(tags),
        "test_samples_created": len(tags),
        "reserve_samples_created": len(tags),
        "chain_of_custody_generated": True,
        "transfer_created": True,
        "lab": session.data.get("lab"),
    }
    session.status = "completed"

    store.update(
        session_id,
        status="completed",
        result=session.result,
        documents=session.documents,
    )


def plan_routing(session: Session) -> None:
    steps = [
        {
            "step_id": 1,
            "action": "route_assignment",
            "details": {
                "route": "OKC Metro",
                "driver": None,
                "stops": 5,
                "miles": 90,
                "hours": 3.2,
                "value": 9710,
                "cost": 68,
                "depart": "8:30 AM",
            },
            "status": "pending",
        },
        {
            "step_id": 2,
            "action": "route_assignment",
            "details": {
                "route": "Tulsa Metro",
                "driver": None,
                "stops": 6,
                "miles": 280,
                "hours": 6.8,
                "value": 28500,
                "cost": 210,
                "depart": "8:30 AM",
            },
            "status": "pending",
        },
        {
            "step_id": 3,
            "action": "route_assignment",
            "details": {
                "route": "Tulsa East",
                "driver": None,
                "stops": 3,
                "miles": 240,
                "hours": 5.5,
                "value": 9600,
                "cost": 180,
                "depart": "8:30 AM",
            },
            "status": "pending",
        },
    ]

    warnings = [
        {
            "type": "production_hold",
            "message": "9 Mango orders ($49.8K) on hold until Apr 8",
            "severity": "warning",
        },
        {
            "type": "stronghold_recommendation",
            "message": (
                "Blackwell ($1,125) + Guthrie ($2,750) + Northern corridor 4 stops, "
                "recommend Stronghold"
            ),
            "severity": "info",
        },
    ]

    session.steps = steps
    session.warnings = warnings
    session.status = "plan_ready"
    session.total_steps = len(steps)
    session.data["plan_summary"] = "3 routes optimized"


def execute_routing(session_id: str) -> None:
    session = store.get(session_id)
    if not session:
        return

    session.status = "executing"

    for i, step in enumerate(session.steps):
        time.sleep(0.5)
        step["status"] = "completed"
        session.current_step = i + 1

    session.result = {
        "routes_created": 3,
        "total_stops": 14,
        "total_miles": 610,
        "total_value": 47810,
    }
    session.status = "completed"

    store.update(session_id, status="completed", result=session.result)


def plan_audit(session: Session) -> None:
    tags = session.data.get("scanned_tags", [])
    expected = session.data.get("expected_counts", {})

    steps = []
    warnings = []
    step_id = 1

    for tag in tags:
        pkg = get_mock_package(tag)
        exp = expected.get(tag, pkg["qty"])
        diff = exp - pkg["qty"]

        if abs(diff) > 0.01:
            steps.append(
                {
                    "step_id": step_id,
                    "action": "package_adjustment",
                    "details": {
                        "tag": tag,
                        "item": pkg["item"],
                        "metrc_qty": pkg["qty"],
                        "scanned_qty": exp,
                        "difference": diff,
                    },
                    "status": "pending",
                }
            )
            warnings.append(
                {
                    "type": "audit_mismatch",
                    "message": f"{tag}: METRC {pkg['qty']} vs shelf {exp}, diff {diff}",
                    "severity": "warning",
                    "affected_item_ids": [tag],
                }
            )
            step_id += 1

    if not steps:
        steps.append(
            {
                "step_id": 1,
                "action": "audit_complete",
                "details": {"message": "All packages match"},
                "status": "completed",
            }
        )

    session.steps = steps
    session.warnings = warnings
    session.status = "plan_ready"
    session.total_steps = len(steps)


def analyze_inventory(tag: str) -> Dict[str, Any]:
    pkg = get_mock_package(tag)
    issues = []

    if pkg["qty"] < 10:
        issues.append(
            {
                "type": "low_inventory",
                "message": f"Only {pkg['qty']} {pkg['uom']} remaining",
                "severity": "warning",
            }
        )

    return {
        "package": pkg,
        "issues": issues,
        "recommendations": [
            "Consider reorder" if pkg["qty"] < 10 else "Stock level adequate"
        ],
    }