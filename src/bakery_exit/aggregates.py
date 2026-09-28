"""聚合状态机：把事件流折叠为当前状态，状态判断全部基于已提交事件。"""

from __future__ import annotations

from typing import Any, Optional

from .store import StoredEvent


def _new_state(event: StoredEvent) -> dict[str, Any]:
    return {"aggregate_type": event.aggregate_type, "aggregate_id": event.aggregate_id, "version": 0}


def fold(events: list[StoredEvent]) -> Optional[dict[str, Any]]:
    if not events:
        return None
    state: Optional[dict[str, Any]] = None
    for event in events:
        if state is None:
            state = _new_state(event)
        reducer = _REDUCERS.get(event.aggregate_type)
        if reducer is not None:
            reducer(state, event.payload, event.event_type)
        state["version"] = event.version
    return state


# ------------------------------------------------------------------ store_period


def _case(state: dict[str, Any], p: dict[str, Any], event_type: str) -> None:
    state.setdefault("status", "opened")
    if event_type == "EXIT_ANNOUNCED":
        state["status"] = "announced"
        state["store_code"] = p["store_code"]
        state["announced_at"] = p["announced_at"]
        state["effective_at"] = p.get("effective_at")
        state["announced_by"] = p.get("announced_by")
    elif event_type == "OBLIGATION_FROZEN":
        state["status"] = "frozen"
        state["cutoff_at"] = p["cutoff_at"]
        state["snapshot_hash"] = p["snapshot_hash"]
        state["snapshot"] = p.get("snapshot", {})
    elif event_type == "PRODUCTION_HALTED":
        state["production_halted_at"] = p["halted_at"]
    elif event_type == "CASE_SUSPENDED":
        state["status"] = "suspended"
        state.setdefault("suspensions", []).append(
            {"reason": p["reason"], "conflict_refs": list(p.get("conflict_refs", []))}
        )
    elif event_type == "CASE_RESUMED":
        state["status"] = "resumed"
        state["resumed_by"] = p["resumed_by"]
        state["resumed_at"] = p["resumed_at"]
    elif event_type == "CASE_CLOSED":
        state["status"] = "closed"
        state["closed_at"] = p["closed_at"]
        state["closure_ref"] = p.get("closure_ref")


# ------------------------------------------------------------------ customer_order


def _order(state: dict[str, Any], p: dict[str, Any], event_type: str) -> None:
    if event_type == "ORDER_REGISTERED":
        state.update(
            case_ref=p["case_ref"],
            order_ref=p["order_ref"],
            customer_id=p["customer_id"],
            source=p["source"],
            scheduled_pickup_at=p["scheduled_pickup_at"],
            items=list(p["items"]),
            payment=dict(p.get("payment", {"method": p["source"]})),
            status="registered",
        )
    elif event_type == "ORDER_FULFILLED":
        state["status"] = "fulfilled"
        state["fulfilled_at"] = p["fulfilled_at"]
        if p.get("picked_by"):
            state["picked_by"] = p["picked_by"]
    elif event_type == "ORDER_MARKED_UNFULFILLABLE":
        state["status"] = "unfulfillable"
        state["unfulfillable_reason"] = p["reason"]
        state["decided_at"] = p["decided_at"]
    elif event_type == "ORDER_TRANSFERRED":
        state["status"] = "transferred"
        state["successor_ref"] = p["successor_ref"]
        state["order_scope"] = p.get("order_scope")
    elif event_type == "ENTITLEMENT_RESTORED":
        state.setdefault("restorations", []).append(p.get("restoration", {}))
    elif event_type == "ENTITLEMENT_SETTLED":
        state.setdefault("refunds", []).append({"source_ref": p["source_ref"], "amount": p["amount"]})


# ------------------------------------------------------------------ balance


def _balance(state: dict[str, Any], p: dict[str, Any], event_type: str) -> None:
    state.setdefault("holds", {})
    state.setdefault("total", 0)
    state.setdefault("consumed", 0)
    state.setdefault("transferred_out", 0)
    state.setdefault("refunded", 0)
    if event_type == "BALANCE_LEDGER_OPENED":
        state["case_ref"] = p["case_ref"]
        state["customer_id"] = p["customer_id"]
        state["currency"] = p["currency"]
        state["total"] = state["total"] + p["amount"]
    elif event_type == "BALANCE_HELD":
        state["holds"][p["hold_ref"]] = {"order_ref": p["order_ref"], "amount": p["amount"], "released": False}
    elif event_type == "BALANCE_HOLD_RELEASED":
        hold = state["holds"].get(p["hold_ref"])
        if hold:
            hold["released"] = True
    elif event_type == "BALANCE_CONSUMED":
        state["consumed"] += p["amount"]
    elif event_type == "BALANCE_TRANSFERRED":
        state["transferred_out"] += p["amount"]
        state["last_successor_store_code"] = p["successor_store_code"]
    elif event_type == "BALANCE_REFUNDED":
        state["refunded"] += p["amount"]


def balance_available(state: dict[str, Any]) -> float:
    held = sum(h["amount"] for h in state["holds"].values() if not h["released"])
    return state["total"] - held - state["consumed"] - state["transferred_out"] - state["refunded"]


# ------------------------------------------------------------------ voucher


def _voucher(state: dict[str, Any], p: dict[str, Any], event_type: str) -> None:
    state.setdefault("redemptions", {})
    if event_type == "VOUCHER_REGISTERED":
        state.update(
            voucher_code=p["voucher_code"],
            case_ref=p["case_ref"],
            source=p["source"],
            face_value=p["face_value"],
            currency=p["currency"],
            status="registered",
        )
    elif event_type == "VOUCHER_REDEEMED":
        state["status"] = "redeemed"
        state["redemptions"][p["redemption_ref"]] = {
            "store_code": p["store_code"],
            "redeemed_at": p["redeemed_at"],
        }
    elif event_type == "VOUCHER_RESTORED":
        state["status"] = "restored"
        state["restore_reason"] = p["reason"]
    elif event_type == "VOUCHER_TRANSFERRED":
        state["status"] = "transferred"
        state["successor_store_code"] = p["successor_store_code"]


# ------------------------------------------------------------------ inventory batch


def _batch(state: dict[str, Any], p: dict[str, Any], event_type: str) -> None:
    state.setdefault("loss_requests", {})
    state.setdefault("written_off", 0)
    state.setdefault("transferred", 0)
    state.setdefault("returned", 0)
    state.setdefault("counts", [])
    if event_type == "BATCH_DELIVERED":
        state.update(
            case_ref=p["case_ref"],
            batch_ref=p["batch_ref"],
            sku=p["sku"],
            delivered=p["quantity"],
            origin=p["origin"],
        )
    elif event_type == "ON_SITE_COUNTED":
        state["counts"].append(
            {"count_ref": p["count_ref"], "counted_by": p["counted_by"], "quantity": p["quantity"], "counted_at": p["counted_at"]}
        )
        state["last_count_quantity"] = p["quantity"]
    elif event_type == "LOSS_REQUESTED":
        state["loss_requests"][p["request_ref"]] = {
            "requested_by": p["requested_by"],
            "quantity": p["quantity"],
            "reason": p["reason"],
            "status": "requested",
        }
    elif event_type == "LOSS_APPROVED":
        request = state["loss_requests"][p["request_ref"]]
        request["status"] = "approved"
        request["approved_by"] = p["approved_by"]
        request["approved_at"] = p["approved_at"]
    elif event_type == "BATCH_WRITTEN_OFF":
        state["written_off"] += p["quantity"]
    elif event_type == "BATCH_TRANSFERRED":
        state["transferred"] += p["quantity"]
        state["successor_store_code"] = p["successor_store_code"]
    elif event_type == "CONSIGNMENT_RETURNED":
        state["returned"] += p["quantity"]
        state["supplier_id"] = p["supplier_id"]


def batch_remaining(state: dict[str, Any]) -> float:
    return state["delivered"] - state["written_off"] - state["transferred"] - state["returned"]


# ------------------------------------------------------------------ supplier


def _supplier(state: dict[str, Any], p: dict[str, Any], event_type: str) -> None:
    state.setdefault("lines", {})
    state.setdefault("settlements", {})
    if event_type == "SUPPLIER_LEDGER_OPENED":
        state.update(case_ref=p["case_ref"], supplier_id=p["supplier_id"], supplier_name=p["supplier_name"])
    elif event_type == "CONSIGNMENT_RECEIVED":
        state["lines"][p["batch_ref"]] = {"sku": p["sku"], "quantity": p["quantity"], "unit_amount": p["unit_amount"]}
    elif event_type == "SETTLEMENT_PROPOSED":
        state["settlements"][p["settlement_ref"]] = {
            "amount": p["amount"],
            "proposed_by": p["proposed_by"],
            "status": "proposed",
        }
    elif event_type == "SETTLEMENT_CONFIRMED":
        settlement = state["settlements"][p["settlement_ref"]]
        settlement["status"] = "confirmed"
        settlement["confirmed_by"] = p["confirmed_by"]
        settlement["confirmed_at"] = p["confirmed_at"]
    elif event_type == "SETTLEMENT_PAID":
        settlement = state["settlements"][p["settlement_ref"]]
        settlement["status"] = "paid"
        settlement["paid_at"] = p["paid_at"]


# ------------------------------------------------------------------ lease


def _lease(state: dict[str, Any], p: dict[str, Any], event_type: str) -> None:
    if event_type == "LEASE_REGISTERED":
        state.update(
            case_ref=p["case_ref"], equipment_ref=p["equipment_ref"], lessor=p["lessor"], lease_end_at=p["lease_end_at"],
            status="registered",
        )
    elif event_type == "LEASE_TERMINATED":
        state["status"] = "terminated"
        state["terminated_at"] = p["terminated_at"]
        state["terminated_by"] = p["terminated_by"]
    elif event_type == "EQUIPMENT_REMOVED":
        state["status"] = "removed"
        state["removed_at"] = p["removed_at"]
        state["witness_by"] = p["witness_by"]
    elif event_type == "DEPOSIT_SETTLED":
        state["deposit_amount"] = p["amount"]
        state["deposit_settled_at"] = p["settled_at"]


# ------------------------------------------------------------------ handover


def _handover(state: dict[str, Any], p: dict[str, Any], event_type: str) -> None:
    if event_type == "HANDOVER_ASSIGNED":
        state.update(
            case_ref=p["case_ref"],
            task_ref=p["task_ref"],
            category=p["category"],
            title=p["title"],
            owner_role=p["owner_role"],
            visibility=list(p["visibility"]),
            due_at=p["due_at"],
            status="assigned",
        )
    elif event_type == "HANDOVER_ACKNOWLEDGED":
        state["status"] = "acknowledged"
        state["acknowledged_by"] = p["acknowledged_by"]
        state["acknowledged_at"] = p["acknowledged_at"]
    elif event_type == "HANDOVER_CLOSED":
        state["status"] = "closed"
        state["closed_at"] = p["closed_at"]
        state["closed_by"] = p["closed_by"]


# ------------------------------------------------------------------ exit plan


def _plan(state: dict[str, Any], p: dict[str, Any], event_type: str) -> None:
    state.setdefault("funds_confirmed", False)
    state.setdefault("successor_confirmed", False)
    if event_type == "PLAN_PROPOSED":
        state.update(
            case_ref=p["case_ref"],
            plan_ref=p["plan_ref"],
            successor_store_code=p["successor_store_code"],
            order_scope=p.get("order_scope"),
            proposed_by=p["proposed_by"],
            status="proposed",
        )
    elif event_type == "FUNDS_CONFIRMED":
        state["funds_confirmed"] = True
        state["funds_confirmed_by"] = p["confirmed_by"]
        state["funds_confirmed_at"] = p["confirmed_at"]
    elif event_type == "SUCCESSOR_CONFIRMED":
        state["successor_confirmed"] = True
        state["successor_ref"] = p["successor_ref"]
        state["successor_confirmed_by"] = p["confirmed_by"]
        state["successor_confirmed_at"] = p["confirmed_at"]
    elif event_type == "PLAN_COMPLETED":
        state["status"] = "completed"
        state["completed_at"] = p["completed_at"]


# ------------------------------------------------------------------ scheduler


def _scheduler(state: dict[str, Any], p: dict[str, Any], event_type: str) -> None:
    state.setdefault("stages", {})
    state.setdefault("order", [])
    if event_type == "STAGE_SCHEDULED":
        if p["stage"] not in state["stages"]:
            state["order"].append(p["stage"])
        state["stages"][p["stage"]] = {"deadline_at": p["deadline_at"], "advanced_at": None}
    elif event_type == "STAGE_ADVANCED":
        state["stages"][p["stage"]]["advanced_at"] = p["advanced_at"]
    elif event_type == "RECOVERY_RESUMED":
        state["resumed_at"] = p["resumed_at"]
        state["resumed_stage"] = p.get("stage")
        state["recovery_count"] = state.get("recovery_count", 0) + 1


_REDUCERS: dict[str, Any] = {
    "store_period": _case,
    "customer_order": _order,
    "customer_entitlement": _balance,
    "voucher": _voucher,
    "inventory_position": _batch,
    "supplier_account": _supplier,
    "equipment_lease": _lease,
    "handover_task": _handover,
    "exit_plan": _plan,
    "scheduler": _scheduler,
}
