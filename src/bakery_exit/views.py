"""只读视图：消费者、供应商、员工各见其份；总部可沿事件链追溯。

视图不做任何写入，全部从事件日志折叠构建。
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Optional

from . import aggregates as agg
from .errors import AuthorizationError
from .store import EventStore


class ReadModel:
    """一次性折叠全部事件，按类型与 case_ref 建索引供各视图复用。"""

    def __init__(self, store: EventStore) -> None:
        self.store = store
        self.by_type: dict[str, list[Any]] = defaultdict(list)
        self.case_index: dict[str, list[Any]] = defaultdict(list)
        self._states: dict[tuple[str, str], dict[str, Any]] = {}
        for event in store.all_events():
            key = (event.aggregate_type, event.aggregate_id)
            if key not in self._states:
                self._states[key] = agg.fold(store.events_for(*key))  # type: ignore[assignment]
        for (aggregate_type, _aggregate_id), state in self._states.items():
            self.by_type[aggregate_type].append(state)
            case_ref = state.get("case_ref")
            if case_ref:
                self.case_index[case_ref].append(state)

    def state(self, aggregate_type: str, aggregate_id: str) -> Optional[dict[str, Any]]:
        return self._states.get((aggregate_type, aggregate_id))

    def states_of(self, aggregate_type: str, case_ref: Optional[str] = None) -> list[dict[str, Any]]:
        rows = [s for s in self.by_type.get(aggregate_type, [])]
        if case_ref is not None:
            rows = [s for s in rows if s.get("case_ref") == case_ref]
        return rows


# ------------------------------------------------------------------ 消费者

class CustomerView:
    """消费者只能看到自己的订单、储值余额与券的去向。"""

    def __init__(self, model: ReadModel, customer_id: str) -> None:
        self.model = model
        self.customer_id = customer_id

    def _own(self, state: dict[str, Any]) -> bool:
        return state.get("customer_id") == self.customer_id

    def orders(self) -> list[dict[str, Any]]:
        result = []
        for order in self.model.states_of("customer_order"):
            if not self._own(order):
                continue
            result.append({
                "order_ref": order["order_ref"],
                "case_ref": order["case_ref"],
                "source": order["source"],
                "scheduled_pickup_at": order["scheduled_pickup_at"],
                "status": order["status"],
                "destination": self._destination(order),
            })
        return result

    def _destination(self, order: dict[str, Any]) -> dict[str, Any]:
        status = order["status"]
        if status == "fulfilled":
            return {"outcome": "picked_up", "at": order.get("fulfilled_at")}
        if status == "transferred":
            return {"outcome": "transferred", "successor_ref": order.get("successor_ref")}
        if status == "unfulfillable":
            restorations = order.get("restorations", [])
            return {"outcome": "restored", "restoration": restorations[0] if restorations else None}
        return {"outcome": "pending"}

    def balances(self) -> list[dict[str, Any]]:
        rows = []
        for ledger in self.model.states_of("customer_entitlement"):
            if not self._own(ledger):
                continue
            rows.append({
                "ledger_ref": ledger["aggregate_id"],
                "currency": ledger.get("currency", "CNY"),
                "original": ledger["total"],
                "consumed": ledger.get("consumed", 0),
                "refunded": ledger.get("refunded", 0),
                "transferred_to": ledger.get("last_successor_store_code"),
                "transferred": ledger.get("transferred_out", 0),
                "available": agg.balance_available(ledger),
            })
        return rows

    def vouchers(self) -> list[dict[str, Any]]:
        def _belongs(voucher: dict[str, Any]) -> bool:
            refs = {voucher.get("aggregate_id"), voucher.get("voucher_code")}
            return any(
                o.get("customer_id") == self.customer_id
                and (o.get("payment") or {}).get("voucher_code") in refs
                for o in self.model.states_of("customer_order")
            )

        return [
            {
                "voucher_ref": v["aggregate_id"],
                "code": v.get("voucher_code"),
                "source": v.get("source"),
                "face_value": v.get("face_value"),
                "status": v.get("status"),
                "transferred_to": v.get("successor_store_code"),
                "redemptions": list(v.get("redemptions", {}).keys()),
            }
            for v in self.model.states_of("voucher")
            if _belongs(v)
        ]


# ------------------------------------------------------------------ 供应商

class SupplierView:
    """供应商只能看到自己的寄售入库与结算单。"""

    def __init__(self, model: ReadModel, supplier_id: str) -> None:
        self.model = model
        self.supplier_id = supplier_id

    def consignments(self) -> list[dict[str, Any]]:
        supplier = self.model.state("supplier_account", self.supplier_id)
        if supplier is None:
            return []
        rows = []
        for batch_ref, line in supplier.get("lines", {}).items():
            batch = self.model.state("inventory_position", batch_ref)
            rows.append({
                "batch_ref": batch_ref,
                "sku": line["sku"],
                "quantity": line["quantity"],
                "unit_amount": line["unit_amount"],
                "returned": batch.get("returned", 0) if batch else 0,
                "written_off": batch.get("written_off", 0) if batch else 0,
            })
        return rows

    def settlements(self) -> list[dict[str, Any]]:
        supplier = self.model.state("supplier_account", self.supplier_id)
        if supplier is None:
            return []
        return [
            {"settlement_ref": ref, **detail}
            for ref, detail in supplier.get("settlements", {}).items()
        ]


# ------------------------------------------------------------------ 员工

class EmployeeView:
    """员工按角色查看交接事项；visibility 未包含其角色则不可见。"""

    def __init__(self, model: ReadModel, employee_id: str, role: str) -> None:
        self.model = model
        self.employee_id = employee_id
        self.role = role

    def handovers(self, case_ref: Optional[str] = None) -> list[dict[str, Any]]:
        rows = []
        for task in self.model.states_of("handover_task", case_ref):
            if self.role not in task.get("visibility", []):
                continue
            rows.append({
                "task_ref": task["task_ref"],
                "category": task["category"],
                "title": task["title"],
                "owner_role": task["owner_role"],
                "due_at": task["due_at"],
                "status": task["status"],
                "acknowledged_by": task.get("acknowledged_by"),
            })
        return rows

    def require_visible(self, task_ref: str) -> dict[str, Any]:
        task = self.model.state("handover_task", task_ref)
        if task is None:
            raise AuthorizationError("交接事项不存在", field="task_ref")
        if self.role not in task.get("visibility", []):
            raise AuthorizationError("该交接事项不在你的可见范围内", field="task_ref")
        return task


# ------------------------------------------------------------------ 总部追溯

class HeadquartersView:
    """总部从任一清算结果追到原门店、商品批次、责任人与未完成义务。"""

    def __init__(self, model: ReadModel) -> None:
        self.model = model

    def case_overview(self, case_ref: str) -> dict[str, Any]:
        case = self.model.state("store_period", case_ref)
        if case is None:
            raise AuthorizationError(f"案件 {case_ref} 不存在", field="case_ref")
        return {
            "case_ref": case_ref,
            "store_code": case.get("store_code"),
            "status": case.get("status"),
            "announced_at": case.get("announced_at"),
            "cutoff_at": case.get("cutoff_at"),
            "snapshot_hash": case.get("snapshot_hash"),
            "closed_at": case.get("closed_at"),
            "counts": {
                "orders": len(self.model.states_of("customer_order", case_ref)),
                "batches": len(self.model.states_of("inventory_position", case_ref)),
                "suppliers": len(self.model.states_of("supplier_account", case_ref)),
                "leases": len(self.model.states_of("equipment_lease", case_ref)),
                "handovers": len(self.model.states_of("handover_task", case_ref)),
                "plans": len(self.model.states_of("exit_plan", case_ref)),
            },
        }

    def trace(self, aggregate_type: str, aggregate_id: str) -> dict[str, Any]:
        """从任一聚合出发，沿 case_ref/batch_ref/plan_ref 等引用还原完整责任链。"""
        start = self.model.state(aggregate_type, aggregate_id)
        if start is None:
            raise AuthorizationError(f"{aggregate_type}/{aggregate_id} 不存在", field=aggregate_id)
        case_ref = start.get("case_ref")
        case = self.model.state("store_period", case_ref) if case_ref else None

        batches: list[dict[str, Any]] = []
        responsible: set[str] = set()
        chain: list[dict[str, Any]] = []

        def walk(kind: str, ident: str, depth: int = 0) -> None:
            if depth > 8:
                return
            state = self.model.state(kind, ident)
            if state is None:
                return
            chain.append({"aggregate_type": kind, "aggregate_id": ident, "status": state.get("status")})
            if kind == "inventory_position" and not any(b["batch_ref"] == ident for b in batches):
                batches.append({
                    "batch_ref": ident,
                    "sku": state.get("sku"),
                    "origin": state.get("origin"),
                    "delivered": state.get("delivered"),
                    "written_off": state.get("written_off", 0),
                    "transferred": state.get("transferred", 0),
                    "returned": state.get("returned", 0),
                })
            person_keys = (
                "requested_by", "approved_by", "confirmed_by", "funds_confirmed_by",
                "successor_confirmed_by", "proposed_by", "closed_by", "decided_by",
                "terminated_by", "witness_by", "acknowledged_by", "picked_by",
                "announced_by", "frozen_by", "halted_by", "commanded_by",
            )
            for key in person_keys:
                if state.get(key):
                    responsible.add(state[key])
            # 嵌套结构：供应商结算单、损耗申请
            for collection in ("settlements", "loss_requests"):
                for detail in (state.get(collection) or {}).values():
                    for key in person_keys:
                        if isinstance(detail, dict) and detail.get(key):
                            responsible.add(detail[key])
            linked = state.get("batch_ref")
            if linked:
                walk("inventory_position", linked, depth + 1)
            # 供应商台账按 batch_ref 记录寄售明细
            for linked in (state.get("lines") or {}).keys():
                walk("inventory_position", linked, depth + 1)
            if state.get("plan_ref"):
                walk("exit_plan", state["plan_ref"], depth + 1)
            if state.get("case_ref") and kind != "store_period":
                walk("store_period", state["case_ref"], depth + 1)

        walk(aggregate_type, aggregate_id)

        # 订单场景下经由 items/payment 无法直接定位批次时，仍列出本案全部批次供总部核对。
        if not batches and case_ref:
            for batch in self.model.states_of("inventory_position", case_ref):
                batches.append({
                    "batch_ref": batch["aggregate_id"],
                    "sku": batch.get("sku"),
                    "origin": batch.get("origin"),
                    "delivered": batch.get("delivered"),
                    "written_off": batch.get("written_off", 0),
                    "transferred": batch.get("transferred", 0),
                    "returned": batch.get("returned", 0),
                })

        return {
            "origin_store": case.get("store_code") if case else None,
            "case_ref": case_ref,
            "case_status": case.get("status") if case else None,
            "frozen_snapshot_hash": case.get("snapshot_hash") if case else None,
            "batches": batches,
            "responsible_persons": sorted(responsible),
            "chain": chain,
        }
