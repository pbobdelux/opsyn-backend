from typing import Dict, Any

async def process_brain_request(ctx: Dict[str, Any]) -> Dict[str, Any]:
    
    suggestions = []

    # 🔥 BLOCKERS
    blockers = ctx.get("blockers", [])
    for b in blockers:
        suggestions.append({
            "type": "fix_it_path",
            "title": f"Fix: {b.replace('_', ' ')}",
            "body": "This is blocking your workflow",
            "action_label": "Fix Now",
            "urgency": "high",
            "confidence": 90,
            "why_this_matters": "This is blocking progress"
        })

    # 🔥 NEXT STEP
    if ctx.get("workflow_state"):
        suggestions.append({
            "type": "next_best_action",
            "title": f"Next step: {ctx['workflow_state']}",
            "body": "Continue your workflow",
            "action_label": "Continue",
            "urgency": "medium",
            "confidence": 80
        })

    # 🔥 QUICK WIN
    if ctx.get("available_actions"):
        suggestions.append({
            "type": "quick_win",
            "title": "Quick win available",
            "body": "Complete this instantly",
            "action_label": "Do it",
            "urgency": "low",
            "confidence": 75
        })

    # 🔥 Sort by urgency
    urgency_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    suggestions.sort(key=lambda x: urgency_order.get(x["urgency"], 3))

    primary = suggestions[0] if suggestions else None
    additional = suggestions[1:4]

    return {
        "primary": primary,
        "additional": additional,
        "confidence": 85,
        "system_status": "healthy"
    }