"""状态投影：从事件流重建门店清算所需的全部只读状态。"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Optional


def _new_store() -> dict[str, Any]:
    return {
        "opened_at": None,
        "announced": None,
        "frozen": None,
        "stage": "init",
        "closed_at": None,
        "confirmations": {},
        "suspensions": {},
        "stage_history": [],
    }


class Projection:
    """内存投影；进程恢复时对事件日志重放即可重建。"""

    def __init__(self) -> None:
        self.stores: dict[str, dict[str, Any]] = {}
        # (store_id, batch_id) -> 批次台账
        self.batches: dict[tuple[str, str], dict[str, Any]] = {}
        # (store_id, order_no) -> 订单履约状态
        self.orders: dict[tuple[str, str], dict[str, Any]] = {}
        # (store_id, customer_ref) -> 储值义务
        self.balances: dict[tuple[str, str], dict[str, Any]] = {}
        # 团购/活动券核销编号 -> 已登记事实（跨承接合并去重）
        self.coupon_redemptions: dict[str, dict[str, Any]] = {}
        # (store_id, supplier_ref) -> 寄售与结算
        self.suppliers: dict[tuple[str, str], dict[str, Any]] = {}
        # (store_id, equipment_no) -> 租赁设备
        self.equipment: dict[tuple[str, str], dict[str, Any]] = {}
        # (store_id, staff_id) -> 员工交接
        self.staff: dict[tuple[str, str], dict[str, Any]] = {}
        # 业务回执编号 -> 幂等事实
        self.receipts: dict[str, dict[str, Any]] = {}
        # 事件标识 -> 事件（event_id 全局唯一）
        self.event_ids: dict[str, dict[str, Any]] = {}

    # -- 查询辅助 -------------------------------------------------------

    def store(self, store_id: str) -> dict[str, Any]:
        return self.stores.setdefault(store_id, _new_store())

    def require_store(self, store_id: str) -> dict[str, Any]:
        store = self.stores.get(store_id)
        if store is None or store["opened_at"] is None:
            raise KeyError(f"门店经营期不存在：{store_id}")
        return store

    def order(self, store_id: str, order_no: str) -> Optional[dict[str, Any]]:
        return self.orders.get((store_id, order_no))

    def balance(self, store_id: str, customer_ref: str) -> dict[str, Any]:
        return self.balances.setdefault(
            (store_id, customer_ref), {"customer_ref": customer_ref, "amount": 0, "postings": []}
        )

    def open_obligations(self, store_id: str) -> list[dict[str, Any]]:
        return [
            deepcopy(order)
            for (sid, _), order in self.orders.items()
            if sid == store_id and order["status"] in ("registered", "transferred_out")
        ]

    # -- 应用事件 -------------------------------------------------------

    def apply(self, event: dict[str, Any]) -> None:
        etype = event["event_type"]
        body = event["payload"]
        handler = getattr(self, f"_on_{etype.lower()}", None)
        if handler is not None:
            handler(event, body)
        self.event_ids[event["event_id"]] = event

    # 经营期
    def _on_store_period_opened(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        store = self.store(p["store_id"])
        store["opened_at"] = p["opened_at"]
        store["stage"] = "operating"

    def _on_exit_announced(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        store = self.require_store(p["store_id"])
        store["announced"] = {"decided_at": p["decided_at"], "effective_at": p["effective_at"]}
        store["stage"] = "announced"

    def _on_obligation_frozen(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        store = self.require_store(p["store_id"])
        store["frozen"] = {
            "cutoff_at": p["cutoff_at"],
            "snapshot_hash": p["snapshot_hash"],
            "deadlines": p.get("deadlines", {}),
        }
        store["stage"] = "frozen"

    def _on_stage_advanced(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        store = self.require_store(p["store_id"])
        store["stage"] = p["to_stage"]
        store["stage_history"].append(
            {"from": p["from_stage"], "to": p["to_stage"], "at": p["advanced_at"]}
        )

    def _on_handover_closed(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        store = self.require_store(p["store_id"])
        store["stage"] = "closed"
        store["closed_at"] = p["closed_at"]

    # 批次与损耗
    def _on_batch_delivered(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        key = (p["store_id"], p["batch_id"])
        batch = self.batches.setdefault(
            key,
            {"store_id": p["store_id"], "batch_id": p["batch_id"], "sku": p["sku"],
             "source_kind": p["source_kind"], "delivered": 0, "losses": {}},
        )
        batch["delivered"] += int(p["quantity"])

    def _on_fresh_loss_reported(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        batch = self.batches[(p["store_id"], p["batch_id"])]
        batch["losses"][p["loss_id"]] = {
            "loss_id": p["loss_id"],
            "reported_quantity": int(p["quantity"]),
            "reporter_id": p["reporter_id"],
            "reported_at": event["occurred_at"],
            "verified_quantity": None,
            "verifier_id": None,
            "decision": None,
            "approver_id": None,
        }

    def _on_fresh_loss_verified(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        loss = self.batches[(p["store_id"], p["batch_id"])]["losses"][p["loss_id"]]
        loss["verified_quantity"] = int(p["verified_quantity"])
        loss["verifier_id"] = p["verifier_id"]
        loss["verified_at"] = event["occurred_at"]

    def _on_fresh_loss_decided(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        loss = self.batches[(p["store_id"], p["batch_id"])]["losses"][p["loss_id"]]
        loss["decision"] = p["decision"]
        loss["approver_id"] = p["approver_id"]
        loss["decided_at"] = event["occurred_at"]

    # 订单
    def _on_order_registered(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        key = (p["store_id"], p["order_no"])
        self.orders[key] = {
            "store_id": p["store_id"],
            "order_no": p["order_no"],
            "source_kind": p["source_kind"],
            "entitlement_ref": p["entitlement_ref"],
            "amount": p["amount"],
            "customer_ref": p.get("customer_ref"),
            "status": "registered",
            "dispositions": [],
        }

    def _on_order_fulfilled(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        order = self.orders[(p["store_id"], p["order_no"])]
        order["status"] = "fulfilled"
        order["dispositions"].append({"kind": "fulfilled", "at": event["occurred_at"]})

    def _on_order_transferred(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        order = self.orders[(p["store_id"], p["order_no"])]
        order["status"] = "transferred_out"
        order["successor_ref"] = p["successor_ref"]
        order["dispositions"].append(
            {"kind": "transferred", "successor_ref": p["successor_ref"],
             "scope": p["order_scope"], "at": event["occurred_at"]}
        )

    def _on_entitlement_settled(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        # 券核销登记（successor 侧或本店）；按核销编号幂等去重
        redemption = {
            "redemption_no": p["source_ref"],
            "amount": p["amount"],
            "store_id": p.get("store_id"),
            "scope": p.get("scope"),
            "at": event["occurred_at"],
            "event_id": event["event_id"],
        }
        self.coupon_redemptions.setdefault(p["source_ref"], redemption)

    def _on_entitlement_restored(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        order = self.orders[(p["store_id"], p["order_no"])]
        order["status"] = "restored"
        order["dispositions"].append(
            {"kind": "restored", "source_kind": p["source_kind"],
             "restore_kind": p["restore_kind"], "amount": p["amount"],
             "at": event["occurred_at"]}
        )

    def _on_refund_issued(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        order = self.orders[(p["store_id"], p["order_no"])]
        order["status"] = "refunded"
        order["dispositions"].append(
            {"kind": "refund", "amount": p["amount"], "receipt_no": p["receipt_no"],
             "at": event["occurred_at"]}
        )

    def _on_balance_transferred(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        balance = self.balance(p["store_id"], p["customer_ref"])
        balance["amount"] -= p["amount"]
        balance["postings"].append(
            {"kind": "transfer_out", "amount": p["amount"], "successor_ref": p["successor_ref"],
             "receipt_no": p["receipt_no"], "at": event["occurred_at"]}
        )

    def _on_stored_balance_posted(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        balance = self.balance(p["store_id"], p["customer_ref"])
        balance["amount"] += p["delta"]
        balance["postings"].append(
            {"kind": p["reason"], "delta": p["delta"], "receipt_no": p["receipt_no"],
             "at": event["occurred_at"]}
        )

    # 承接方案
    def _on_successor_planned(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        store = self.require_store(p["store_id"])
        store["successor"] = {
            "successor_ref": p["successor_ref"], "scope": p["scope"], "planned_at": p["planned_at"]
        }

    def _on_independent_confirmation(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        store = self.require_store(p["store_id"])
        if p["confirmed"]:
            store["confirmations"][p["party"]] = {
                "confirmer_id": p["confirmer_id"], "at": event["occurred_at"]
            }
        else:
            store["confirmations"].pop(p["party"], None)

    def _on_clearing_suspended(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        store = self.require_store(p["store_id"])
        store["suspensions"][p["receipt_no"]] = {
            "receipt_no": p["receipt_no"], "reason": p["reason"], "at": event["occurred_at"],
            "status": "suspended", "conflict": p.get("conflict"),
        }

    def _on_clearing_resumed(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        store = self.require_store(p["store_id"])
        suspension = store["suspensions"].get(p["receipt_no"])
        if suspension is not None:
            suspension["status"] = "resolved"
            suspension["resolution"] = p["resolution"]
            suspension["resolver_id"] = p["resolver_id"]
            suspension["resolved_at"] = event["occurred_at"]

    # 交接
    def _on_lease_equipment_recorded(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        self.equipment[(p["store_id"], p["equipment_no"])] = {
            "store_id": p["store_id"], "equipment_no": p["equipment_no"],
            "lessor_ref": p["lessor_ref"], "status": "leased",
        }

    def _on_lease_closed(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        self.equipment[(p["store_id"], p["equipment_no"])]["status"] = "closed"

    def _on_staff_handover_recorded(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        self.staff[(p["store_id"], p["staff_id"])] = {
            "store_id": p["store_id"], "staff_id": p["staff_id"],
            "items": list(p["items"]), "confirmed": False,
        }

    def _on_staff_handover_confirmed(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        self.staff[(p["store_id"], p["staff_id"])]["confirmed"] = True
        self.staff[(p["store_id"], p["staff_id"])]["confirmer_id"] = p["confirmer_id"]

    # 供应商
    def _on_supplier_consignment_recorded(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        key = (p["store_id"], p["supplier_ref"])
        ledger = self.suppliers.setdefault(
            key, {"store_id": p["store_id"], "supplier_ref": p["supplier_ref"],
                  "lines": [], "settled_amount": 0, "settled": False}
        )
        ledger["lines"].append({
            "batch_id": p["batch_id"], "quantity": p["quantity"], "amount": p["amount"],
        })

    def _on_supplier_settled(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        ledger = self.suppliers[(p["store_id"], p["supplier_ref"])]
        ledger["settled_amount"] += p["amount"]
        ledger["settled"] = True
        ledger["settled_at"] = event["occurred_at"]
        ledger["settle_receipt"] = p["receipt_no"]


def rebuild(events: list[dict[str, Any]]) -> Projection:
    projection = Projection()
    for event in events:
        projection.apply(event)
    return projection
