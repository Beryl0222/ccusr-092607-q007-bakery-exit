"""角色视图：按权限过滤清算事实。

- 消费者：只能看到自己的订单与余额去向。
- 供应商：只能看到自己的寄售台账与结算。
- 员工：只能看到自己的交接事项。
- 总部：可从任一清算结果追到原门店、商品批次、责任人和未完成义务。
"""

from __future__ import annotations

from typing import Any, Optional

from .projection import Projection

_TERMINAL = {"fulfilled", "transferred_out", "restored", "refunded"}


class Views:
    def __init__(self, projection: Projection) -> None:
        self.p = projection

    # ------------------------------------------------------------- 消费者

    def customer_view(self, customer_ref: str, store_id: Optional[str] = None) -> dict[str, Any]:
        orders = []
        for (sid, order_no), order in self.p.orders.items():
            if order.get("customer_ref") != customer_ref:
                continue
            if store_id is not None and sid != store_id:
                continue
            orders.append({
                "store_id": sid,
                "order_no": order_no,
                "source_kind": order["source_kind"],
                "amount": order["amount"],
                "status": order["status"],
                "whereabouts": [self._describe_disposition(d) for d in order["dispositions"]],
            })
        balances = []
        for (sid, cust), balance in self.p.balances.items():
            if cust != customer_ref or (store_id is not None and sid != store_id):
                continue
            balances.append({
                "store_id": sid,
                "remaining": balance["amount"],
                "movements": [
                    {"kind": post["kind"], "at": post["at"],
                     **({"amount": post["amount"], "successor_ref": post["successor_ref"]}
                        if post["kind"] == "transfer_out" else {"delta": post["delta"]})}
                    for post in balance["postings"]
                ],
            })
        return {"customer_ref": customer_ref, "orders": orders, "balances": balances}

    @staticmethod
    def _describe_disposition(d: dict[str, Any]) -> dict[str, Any]:
        kind = d["kind"]
        if kind == "fulfilled":
            return {"kind": "picked_up", "at": d["at"], "detail": "已到店提货完成"}
        if kind == "transferred":
            return {"kind": "moved_store", "successor_ref": d["successor_ref"],
                    "scope": d["scope"], "at": d["at"], "detail": "订单已转承接门店"}
        if kind == "restored":
            return {"kind": "restored", "restore_kind": d["restore_kind"],
                    "amount": d["amount"], "at": d["at"],
                    "detail": "门店无法履行，已按订单来源恢复权益"}
        if kind == "refund":
            return {"kind": "refunded", "amount": d["amount"], "at": d["at"],
                    "detail": "已退款"}
        return {"kind": kind, "at": d.get("at")}

    # ------------------------------------------------------------- 供应商

    def supplier_view(self, supplier_ref: str, store_id: Optional[str] = None) -> dict[str, Any]:
        ledgers = []
        for (sid, sup), ledger in self.p.suppliers.items():
            if sup != supplier_ref or (store_id is not None and sid != store_id):
                continue
            total = sum(line["amount"] for line in ledger["lines"])
            ledgers.append({
                "store_id": sid,
                "consignments": ledger["lines"],
                "total_amount": total,
                "settled_amount": ledger["settled_amount"],
                "outstanding": total - ledger["settled_amount"],
                "settled": ledger["settled"],
                "settle_receipt": ledger.get("settle_receipt"),
            })
        return {"supplier_ref": supplier_ref, "ledgers": ledgers}

    # ------------------------------------------------------------- 员工

    def staff_view(self, staff_id: str, store_id: Optional[str] = None) -> dict[str, Any]:
        handovers = []
        for (sid, employee), handover in self.p.staff.items():
            if employee != staff_id or (store_id is not None and sid != store_id):
                continue
            handovers.append({
                "store_id": sid,
                "items": handover["items"],
                "confirmed": handover["confirmed"],
                "confirmer_id": handover.get("confirmer_id"),
            })
        return {"staff_id": staff_id, "handovers": handovers}

    # ------------------------------------------------------------- 总部

    def headquarters_store_view(self, store_id: str) -> dict[str, Any]:
        store = self.p.require_store(store_id)
        return {
            "store_id": store_id,
            "stage": store["stage"],
            "announced": store.get("announced"),
            "frozen": store.get("frozen"),
            "successor": store.get("successor"),
            "confirmations": store.get("confirmations"),
            "suspensions": list(store.get("suspensions", {}).values()),
            "batches": [
                {"batch_id": key[1], "sku": b["sku"], "source_kind": b["source_kind"],
                 "delivered": b["delivered"], "losses": list(b["losses"].values())}
                for key, b in self.p.batches.items() if key[0] == store_id
            ],
            "orders": [
                {"order_no": key[1], **{k: o[k] for k in
                 ("source_kind", "entitlement_ref", "amount", "customer_ref", "status")},
                 "successor_ref": o.get("successor_ref"), "dispositions": o["dispositions"]}
                for key, o in self.p.orders.items() if key[0] == store_id
            ],
            "balances": [
                {"customer_ref": key[1], "amount": b["amount"], "postings": b["postings"]}
                for key, b in self.p.balances.items() if key[0] == store_id
            ],
            "suppliers": [
                {"supplier_ref": key[1], "lines": s["lines"],
                 "settled_amount": s["settled_amount"], "settled": s["settled"]}
                for key, s in self.p.suppliers.items() if key[0] == store_id
            ],
            "equipment": [
                {"equipment_no": key[1], "lessor_ref": e["lessor_ref"], "status": e["status"]}
                for key, e in self.p.equipment.items() if key[0] == store_id
            ],
            "staff": [
                {"staff_id": key[1], "items": s["items"], "confirmed": s["confirmed"],
                 "confirmer_id": s.get("confirmer_id")}
                for key, s in self.p.staff.items() if key[0] == store_id
            ],
        }

    def trace_receipt(self, receipt_no: str) -> dict[str, Any]:
        """从任一清算回执出发，串联原门店、商品批次、责任人与最终去向。"""
        chain = [event for event in self.p.event_ids.values()
                 if event["payload"].get("receipt_no") == receipt_no]
        if not chain:
            raise KeyError(f"未找到回执：{receipt_no}")
        store_ids = {e["payload"].get("store_id") for e in chain if "store_id" in e["payload"]}
        traces = []
        for event in sorted(chain, key=lambda e: (e["occurred_at"], e["version"])):
            p = event["payload"]
            trace = {
                "event_type": event["event_type"],
                "at": event["occurred_at"],
                "store_id": p.get("store_id"),
            }
            for key in ("order_no", "batch_id", "sku", "supplier_ref", "customer_ref",
                        "successor_ref", "equipment_no", "staff_id", "amount", "decision",
                        "reporter_id", "verifier_id", "approver_id", "confirmer_id",
                        "restore_kind", "source_kind", "reason", "resolution"):
                if key in p:
                    trace[key] = p[key]
            traces.append(trace)
        return {"receipt_no": receipt_no, "store_ids": sorted(s for s in store_ids if s),
                "chain": traces}
