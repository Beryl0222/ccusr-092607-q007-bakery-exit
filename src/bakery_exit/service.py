"""门店退场义务清算服务。

规则要点：
- 停业决定发布时冻结债权债务快照；冻结后不得新增债权债务。
- 损耗报告（店长/店员）、现场核实、审批三权分立，报告人不能批准自己的减免。
- 资金退款与储值转移须资金清算独立确认；订单转承接门店须承接方独立确认。
- 相同业务回执重放幂等；编号相同而日期/范围/承接方不同则暂停，裁决前不转移资产。
- 退款、换店、提货互斥：同一份订单权益只能被消耗一次；储值余额不可超转。
- 生效日前可履行的订单正常完成；生效日后无法履行的订单按来源恢复权益或退款。
- 券核销按核销编号去重，门店合并不重复计算。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Mapping, Optional

from .canonical import canonical, snapshot_hash
from .clock import ControllableClock
from .errors import (
    ConfirmationRequired,
    EntitlementAlreadyConsumed,
    ResponsibilityViolation,
    SettlementBlocked,
    StageViolation,
    SuspendedError,
    ValidationError,
)
from .projection import Projection, rebuild
from .store import EventStore

# 阶段顺序：只允许向前推进
STAGE_ORDER = [
    "init",
    "operating",
    "announced",
    "frozen",
    "production_stopped",
    "pickup_refund_closed",
    "lease_returned",
    "settlement",
    "closed",
]

_FULFILLABLE_STAGES = ("frozen", "production_stopped")
_DISPOSITION_STAGES = (
    "frozen",
    "production_stopped",
    "pickup_refund_closed",
    "lease_returned",
)
_TERMINAL_ORDER_STATUS = {"fulfilled", "transferred_out", "restored", "refunded"}


@dataclass
class CommandResult:
    events: list[dict[str, Any]] = field(default_factory=list)
    replayed: bool = False

    @property
    def changed(self) -> bool:
        return bool(self.events) and not self.replayed


class ExitClearingService:
    def __init__(
        self,
        event_store: EventStore,
        clock: ControllableClock,
        schema: Optional[Mapping[str, Any]] = None,
    ) -> None:
        self.store = event_store
        self.clock = clock
        self.schema = schema
        self.projection: Projection = rebuild(event_store.read_all())
        last_at = max((e["occurred_at"] for e in event_store.read_all()), default=None)
        if last_at is not None:
            self.clock.restore_to(datetime.fromisoformat(last_at))

    # ------------------------------------------------------------------ 基础

    def _at(self) -> str:
        return self.clock.now().isoformat()

    def _require_stage(self, store_id: str, *allowed: str) -> None:
        stage = self.projection.require_store(store_id)["stage"]
        if stage not in allowed:
            raise StageViolation(
                f"当前阶段 {stage} 不允许该操作，允许阶段：{', '.join(allowed)}"
            )

    def _emit(
        self,
        event_type: str,
        aggregate_type: str,
        aggregate_id: str,
        payload: dict[str, Any],
        event_id: str,
    ) -> dict[str, Any]:
        existing = self.projection.event_ids.get(event_id)
        if existing is not None:
            if (
                existing["event_type"] == event_type
                and existing["aggregate_type"] == aggregate_type
                and existing["aggregate_id"] == aggregate_id
                and canonical(existing["payload"]) == canonical(payload)
            ):
                return existing
            raise ValidationError(f"事件标识 {event_id} 已用于不同内容，拒绝写入")
        event = {
            "event_id": event_id,
            "event_type": event_type,
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "occurred_at": self._at(),
            "version": self.store.next_version(aggregate_type, aggregate_id),
            "payload": payload,
        }
        if self.schema is not None:
            from .contracts import validate_event

            issues = validate_event(event, self.schema)
            if issues:
                raise ValidationError("；".join(f"{i.field}:{i.code}" for i in issues))
        self.store.append(event)
        self.projection.apply(event)
        return event

    def _plan_aggregate_id(
        self, event_type: str, payload: dict[str, Any]
    ) -> tuple[str, str]:
        sid = payload["store_id"]
        if event_type in ("STORE_PERIOD_OPENED", "EXIT_ANNOUNCED", "OBLIGATION_FROZEN"):
            return "store_period", sid
        if event_type in (
            "HANDOVER_CLOSED",
            "STAGE_ADVANCED",
            "SUCCESSOR_PLANNED",
            "INDEPENDENT_CONFIRMATION",
            "CLEARING_SUSPENDED",
            "CLEARING_RESUMED",
        ):
            return "exit_plan", f"plan:{sid}"
        if event_type in (
            "BATCH_DELIVERED",
            "FRESH_LOSS_REPORTED",
            "FRESH_LOSS_VERIFIED",
            "FRESH_LOSS_DECIDED",
        ):
            return "inventory_position", f"inv:{sid}:{payload['batch_id']}"
        if event_type in (
            "ORDER_REGISTERED",
            "ORDER_FULFILLED",
            "ORDER_TRANSFERRED",
            "ENTITLEMENT_RESTORED",
            "REFUND_ISSUED",
        ):
            return "customer_entitlement", f"order:{sid}:{payload['order_no']}"
        if event_type in ("BALANCE_TRANSFERRED", "STORED_BALANCE_POSTED"):
            return "customer_entitlement", f"balance:{sid}:{payload['customer_ref']}"
        if event_type == "ENTITLEMENT_SETTLED":
            if payload.get("order_no"):
                return "customer_entitlement", f"order:{sid}:{payload['order_no']}"
            return "customer_entitlement", f"redemption:{sid}:{payload['source_ref']}"
        if event_type in ("LEASE_EQUIPMENT_RECORDED", "LEASE_CLOSED"):
            return "handover_task", f"handover:{sid}:lease:{payload['equipment_no']}"
        if event_type in ("STAFF_HANDOVER_RECORDED", "STAFF_HANDOVER_CONFIRMED"):
            return "handover_task", f"handover:{sid}:staff:{payload['staff_id']}"
        if event_type in ("SUPPLIER_CONSIGNMENT_RECORDED", "SUPPLIER_SETTLED"):
            return "supplier_consignment", f"sup:{sid}:{payload['supplier_ref']}"
        raise ValidationError(f"未知事件类型：{event_type}")

    def _commit(
        self,
        receipt_no: str,
        planned: list[tuple[str, dict[str, Any]]],
        validate: Optional[Callable[[], None]] = None,
    ) -> CommandResult:
        """按业务回执提交。

        闸门顺序：暂停检查 → 回执幂等/冲突检查 → 业务校验 → 写入。
        重放绝不执行业务规则，因此阶段已推进、权益已消耗后重放仍安全。
        """
        store_id = planned[0][1]["store_id"]
        store = self.projection.require_store(store_id)

        suspension = store["suspensions"].get(receipt_no)
        if suspension is not None and suspension["status"] == "suspended":
            raise SuspendedError(
                f"回执 {receipt_no} 已暂停：{suspension['reason']}，裁决恢复后才能继续"
            )

        event_ids = [f"{receipt_no}#{i}" for i in range(len(planned))]
        stored = [self.projection.event_ids.get(eid) for eid in event_ids]
        if any(stored):
            same = (
                all(stored)
                and len(stored) == len(planned)
                and all(
                    existing["event_type"] == etype
                    and existing["aggregate_type"] == self._plan_aggregate_id(etype, p)[0]
                    and existing["aggregate_id"] == self._plan_aggregate_id(etype, p)[1]
                    and canonical(existing["payload"]) == canonical(p)
                    for existing, (etype, p) in zip(stored, planned)
                )
            )
            if same:
                return CommandResult(events=list(stored), replayed=True)
            return self._suspend(
                store_id,
                receipt_no,
                f"同一回执编号 {receipt_no} 的日期、范围或承接方与既有记录不一致",
                conflict={
                    "existing": next((e for e in stored if e is not None), None),
                    "submitted": planned[0],
                },
            )

        if validate is not None:
            validate()

        events = []
        for (event_type, payload), event_id in zip(planned, event_ids):
            agg_type, agg_id = self._plan_aggregate_id(event_type, payload)
            events.append(self._emit(event_type, agg_type, agg_id, payload, event_id))
        return CommandResult(events=events)

    def _suspend(
        self,
        store_id: str,
        receipt_no: str,
        reason: str,
        conflict: Optional[dict] = None,
    ) -> CommandResult:
        store = self.projection.require_store(store_id)
        existing = store["suspensions"].get(receipt_no)
        if existing is not None and existing["status"] == "suspended":
            raise SuspendedError(f"回执 {receipt_no} 已暂停：{existing['reason']}")
        payload: dict[str, Any] = {
            "store_id": store_id,
            "receipt_no": receipt_no,
            "reason": reason,
        }
        if conflict is not None:
            payload["conflict"] = {
                "submitted_event": conflict["submitted"][0],
                "submitted_payload": conflict["submitted"][1],
            }
            if conflict["existing"] is not None:
                payload["conflict"]["existing_payload"] = conflict["existing"]["payload"]
        self._emit(
            "CLEARING_SUSPENDED",
            "exit_plan",
            f"plan:{store_id}",
            payload,
            f"suspend:{receipt_no}",
        )
        raise SuspendedError(reason)

    def _require_confirmation(self, store_id: str, party: str) -> None:
        store = self.projection.require_store(store_id)
        if party not in store["confirmations"]:
            who = {"finance": "资金清算方", "successor": "承接门店"}[party]
            raise ConfirmationRequired(f"{who}尚未独立确认，不得转移资产或余额")

    # ---------------------------------------------------------- 经营期与冻结

    def open_store_period(
        self, store_id: str, manager_id: str, opened_at: Optional[str] = None
    ) -> str:
        existing = self.projection.stores.get(store_id)
        if existing is not None and existing["opened_at"]:
            raise ValidationError(f"门店经营期已存在：{store_id}")
        event = self._emit(
            "STORE_PERIOD_OPENED",
            "store_period",
            store_id,
            {
                "store_id": store_id,
                "manager_id": manager_id,
                "opened_at": opened_at or self._at(),
            },
            f"open:{store_id}",
        )
        return event["event_id"]

    def manager_id(self, store_id: str) -> Optional[str]:
        event = self.projection.event_ids.get(f"open:{store_id}")
        return event["payload"]["manager_id"] if event else None

    def announce_exit(self, store_id: str, effective_at: str) -> str:
        self._require_stage(store_id, "operating")
        effective = datetime.fromisoformat(effective_at)
        if effective.tzinfo is None:
            raise ValidationError("生效时间必须携带时区")
        if effective <= self.clock.now():
            raise ValidationError("生效日必须晚于停业决定发布时间")
        event = self._emit(
            "EXIT_ANNOUNCED",
            "store_period",
            store_id,
            {
                "store_id": store_id,
                "decided_at": self._at(),
                "effective_at": effective_at,
            },
            f"announce:{store_id}",
        )
        return event["event_id"]

    def freeze_obligations(
        self,
        store_id: str,
        cutoff_at: str,
        production_stop_at: str,
        pickup_refund_deadline: str,
        lease_return_deadline: str,
        settlement_deadline: str,
    ) -> dict[str, Any]:
        """停业决定发布时冻结债权债务快照，同时登记调度期限。"""
        self._require_stage(store_id, "announced")
        deadlines = {
            "production_stop_at": production_stop_at,
            "pickup_refund_deadline": pickup_refund_deadline,
            "lease_return_deadline": lease_return_deadline,
            "settlement_deadline": settlement_deadline,
        }
        moments = {"cutoff_at": cutoff_at, **deadlines}
        parsed = {key: datetime.fromisoformat(value) for key, value in moments.items()}
        for value in parsed.values():
            if value.tzinfo is None:
                raise ValidationError("期限必须携带时区")
        if not (
            parsed["cutoff_at"] <= parsed["production_stop_at"]
            <= parsed["pickup_refund_deadline"] <= parsed["lease_return_deadline"]
            <= parsed["settlement_deadline"]
        ):
            raise ValidationError("期限顺序必须为：截止≤停产≤取货退款≤租约≤结算")

        snapshot = self._build_snapshot(store_id, cutoff_at)
        self._emit(
            "OBLIGATION_FROZEN",
            "store_period",
            store_id,
            {
                "store_id": store_id,
                "cutoff_at": cutoff_at,
                "snapshot_hash": snapshot["snapshot_hash"],
                "deadlines": deadlines,
            },
            f"freeze:{store_id}",
        )
        return snapshot

    def _build_snapshot(self, store_id: str, cutoff_at: str) -> dict[str, Any]:
        orders = [
            {
                k: o[k]
                for k in (
                    "order_no",
                    "source_kind",
                    "entitlement_ref",
                    "amount",
                    "customer_ref",
                    "status",
                )
            }
            for (sid, _), o in self.projection.orders.items()
            if sid == store_id
        ]
        balances = [
            {"customer_ref": b["customer_ref"], "amount": b["amount"]}
            for (sid, _), b in self.projection.balances.items()
            if sid == store_id
        ]
        suppliers = [
            {
                "supplier_ref": s["supplier_ref"],
                "lines": s["lines"],
                "settled_amount": s["settled_amount"],
            }
            for (sid, _), s in self.projection.suppliers.items()
            if sid == store_id
        ]
        equipment = [
            {"equipment_no": e["equipment_no"], "lessor_ref": e["lessor_ref"], "status": e["status"]}
            for (sid, _), e in self.projection.equipment.items()
            if sid == store_id
        ]
        staff = [
            {"staff_id": s["staff_id"], "items": s["items"], "confirmed": s["confirmed"]}
            for (sid, _), s in self.projection.staff.items()
            if sid == store_id
        ]
        batches = [
            {
                "batch_id": key[1],
                "sku": b["sku"],
                "source_kind": b["source_kind"],
                "delivered": b["delivered"],
            }
            for key, b in self.projection.batches.items()
            if key[0] == store_id
        ]
        redemptions = [
            r
            for r in self.projection.coupon_redemptions.values()
            if r.get("store_id") == store_id
        ]
        body = {
            "store_id": store_id,
            "cutoff_at": cutoff_at,
            "orders": sorted(orders, key=lambda x: x["order_no"]),
            "balances": sorted(balances, key=lambda x: x["customer_ref"]),
            "suppliers": sorted(suppliers, key=lambda x: x["supplier_ref"]),
            "equipment": sorted(equipment, key=lambda x: x["equipment_no"]),
            "staff": sorted(staff, key=lambda x: x["staff_id"]),
            "batches": sorted(batches, key=lambda x: x["batch_id"]),
            "redemptions": sorted(redemptions, key=lambda x: x["redemption_no"]),
        }
        return {**body, "snapshot_hash": snapshot_hash(body)}

    # ------------------------------------------------------------- 商品与损耗

    def deliver_batch(
        self,
        store_id: str,
        batch_id: str,
        sku: str,
        quantity: int,
        source_kind: str = "central_factory",
        receipt_no: Optional[str] = None,
    ) -> CommandResult:
        """中央工厂配送（source_kind=central_factory）或其他来源批次入库。"""
        receipt_no = receipt_no or (
            f"deliver:{store_id}:{batch_id}:"
            f"{self.store.next_version('inventory_position', f'inv:{store_id}:{batch_id}')}"
        )
        planned = [
            (
                "BATCH_DELIVERED",
                {
                    "store_id": store_id,
                    "batch_id": batch_id,
                    "sku": sku,
                    "quantity": quantity,
                    "source_kind": source_kind,
                    "receipt_no": receipt_no,
                },
            )
        ]

        def validate() -> None:
            self._require_stage(store_id, "operating", "announced")
            if quantity <= 0:
                raise ValidationError("配送数量必须为正数")

        return self._commit(receipt_no, planned, validate)

    def report_fresh_loss(
        self,
        store_id: str,
        batch_id: str,
        loss_id: str,
        quantity: int,
        reporter_id: str,
    ) -> CommandResult:
        receipt_no = f"loss-report:{store_id}:{loss_id}"
        planned = [
            (
                "FRESH_LOSS_REPORTED",
                {
                    "store_id": store_id,
                    "batch_id": batch_id,
                    "loss_id": loss_id,
                    "quantity": quantity,
                    "reporter_id": reporter_id,
                },
            )
        ]

        def validate() -> None:
            self._require_stage(
                store_id, "frozen", "production_stopped", "pickup_refund_closed"
            )
            if (store_id, batch_id) not in self.projection.batches:
                raise ValidationError(f"批次不存在：{batch_id}")
            if quantity <= 0:
                raise ValidationError("损耗数量必须为正数")

        return self._commit(receipt_no, planned, validate)

    def verify_fresh_loss(
        self,
        store_id: str,
        batch_id: str,
        loss_id: str,
        verified_quantity: int,
        verifier_id: str,
    ) -> CommandResult:
        """店长核实现场数量。核实不是审批。"""
        receipt_no = f"loss-verify:{store_id}:{loss_id}"
        planned = [
            (
                "FRESH_LOSS_VERIFIED",
                {
                    "store_id": store_id,
                    "batch_id": batch_id,
                    "loss_id": loss_id,
                    "verified_quantity": verified_quantity,
                    "verifier_id": verifier_id,
                },
            )
        ]

        def validate() -> None:
            loss = self._require_loss(store_id, batch_id, loss_id)
            if loss["verified_quantity"] is not None:
                raise ValidationError("该损耗已现场核实")
            if verified_quantity < 0 or verified_quantity > loss["reported_quantity"]:
                raise ValidationError("核实数量必须介于 0 与上报数量之间")

        return self._commit(receipt_no, planned, validate)

    def decide_fresh_loss(
        self,
        store_id: str,
        batch_id: str,
        loss_id: str,
        decision: str,
        approver_id: str,
    ) -> CommandResult:
        """独立审批：减免（waived）或自担（charged）。报告人不能批准自己的损耗。"""
        receipt_no = f"loss-decide:{store_id}:{loss_id}"
        planned = [
            (
                "FRESH_LOSS_DECIDED",
                {
                    "store_id": store_id,
                    "batch_id": batch_id,
                    "loss_id": loss_id,
                    "decision": decision,
                    "approver_id": approver_id,
                },
            )
        ]

        def validate() -> None:
            loss = self._require_loss(store_id, batch_id, loss_id)
            if decision not in ("waived", "charged"):
                raise ValidationError("审批结论只能是 waived 或 charged")
            if loss["verified_quantity"] is None:
                raise ValidationError("损耗尚未经现场核实，不能审批")
            if loss["decision"] is not None:
                raise ValidationError("该损耗已审批")
            if approver_id == loss["reporter_id"]:
                raise ResponsibilityViolation("报告人不能批准自己上报的损耗减免")

        return self._commit(receipt_no, planned, validate)

    def _require_loss(self, store_id: str, batch_id: str, loss_id: str) -> dict[str, Any]:
        batch = self.projection.batches.get((store_id, batch_id))
        if batch is None or loss_id not in batch["losses"]:
            raise ValidationError(f"损耗单不存在：{loss_id}")
        return batch["losses"][loss_id]

    # --------------------------------------------------------------- 订单与储值

    def register_order(
        self,
        store_id: str,
        order_no: str,
        source_kind: str,
        entitlement_ref: str,
        amount: float,
        customer_ref: str,
        receipt_no: str,
    ) -> CommandResult:
        """登记订单（stored_value 储值 / group_coupon 团购券 / gift_coupon 赠券 / payment 普通支付）。

        储值订单下单时即从储值余额划出在途：履约后消耗，无法履行时经恢复退回，
        因此余额不会虚高，也不会重复恢复。
        """
        planned: list[tuple[str, dict[str, Any]]] = [
            (
                "ORDER_REGISTERED",
                {
                    "store_id": store_id,
                    "order_no": order_no,
                    "source_kind": source_kind,
                    "entitlement_ref": entitlement_ref,
                    "amount": amount,
                    "customer_ref": customer_ref,
                    "receipt_no": receipt_no,
                },
            )
        ]
        if source_kind == "stored_value":
            planned.append(
                (
                    "STORED_BALANCE_POSTED",
                    {
                        "store_id": store_id,
                        "customer_ref": customer_ref,
                        "delta": -amount,
                        "reason": "order_payment",
                        "receipt_no": receipt_no,
                    },
                )
            )

        def validate() -> None:
            self._require_stage(store_id, "operating", "announced")
            if self.projection.order(store_id, order_no) is not None:
                raise ValidationError(f"订单已存在：{order_no}")
            if amount <= 0:
                raise ValidationError("订单金额必须为正数")
            if source_kind == "stored_value":
                balance = self.projection.balances.get((store_id, customer_ref))
                if balance is None or amount > balance["amount"] + 1e-9:
                    raise ValidationError("储值余额不足，无法下单")

        return self._commit(receipt_no, planned, validate)

    def top_up_balance(
        self, store_id: str, customer_ref: str, delta: float, receipt_no: str
    ) -> CommandResult:
        planned = [
            (
                "STORED_BALANCE_POSTED",
                {
                    "store_id": store_id,
                    "customer_ref": customer_ref,
                    "delta": delta,
                    "reason": "top_up",
                    "receipt_no": receipt_no,
                },
            )
        ]

        def validate() -> None:
            self._require_stage(store_id, "operating", "announced")
            if delta <= 0:
                raise ValidationError("储值充值金额必须为正数")

        return self._commit(receipt_no, planned, validate)

    def _order_or_raise(self, store_id: str, order_no: str) -> dict[str, Any]:
        order = self.projection.order(store_id, order_no)
        if order is None:
            raise ValidationError(f"订单不存在：{order_no}")
        return order

    def _require_live_order(self, order: dict[str, Any]) -> None:
        if order["status"] in _TERMINAL_ORDER_STATUS:
            last = order["dispositions"][-1]
            raise EntitlementAlreadyConsumed(
                f"订单 {order['order_no']} 的权益已被「{last['kind']}」消耗，"
                "不能再次退款、换店或提货"
            )

    def _require_unfulfillable(self, store_id: str) -> None:
        store = self.projection.require_store(store_id)
        stage = store["stage"]
        effective_at = datetime.fromisoformat(store["announced"]["effective_at"])
        if stage in _FULFILLABLE_STAGES and self.clock.now() < effective_at:
            raise StageViolation("生效日之前订单仍可履行，不能退款或恢复权益")

    def fulfill_order(self, store_id: str, order_no: str, receipt_no: str) -> CommandResult:
        """生效日前仍可履行的订单：到店提货，正常完成。"""
        planned = [
            (
                "ORDER_FULFILLED",
                {"store_id": store_id, "order_no": order_no, "receipt_no": receipt_no},
            )
        ]

        def validate() -> None:
            self._require_stage(store_id, *_FULFILLABLE_STAGES)
            order = self._order_or_raise(store_id, order_no)
            self._require_live_order(order)
            effective_at = datetime.fromisoformat(
                self.projection.require_store(store_id)["announced"]["effective_at"]
            )
            if self.clock.now() >= effective_at:
                raise StageViolation("已过生效日，订单无法履行，应按来源恢复权益或退款")

        return self._commit(receipt_no, planned, validate)

    def transfer_order(
        self,
        store_id: str,
        order_no: str,
        successor_ref: str,
        order_scope: str,
        receipt_no: str,
    ) -> CommandResult:
        """换店承接：须承接门店独立确认。"""
        planned = [
            (
                "ORDER_TRANSFERRED",
                {
                    "store_id": store_id,
                    "order_no": order_no,
                    "successor_ref": successor_ref,
                    "order_scope": order_scope,
                    "receipt_no": receipt_no,
                },
            )
        ]

        def validate() -> None:
            self._require_stage(store_id, *_FULFILLABLE_STAGES)
            order = self._order_or_raise(store_id, order_no)
            self._require_live_order(order)
            self._require_matching_successor(store_id, successor_ref, order_scope)
            self._require_confirmation(store_id, "successor")

        return self._commit(receipt_no, planned, validate)

    def refund_order(
        self, store_id: str, order_no: str, amount: float, receipt_no: str
    ) -> CommandResult:
        """退款：资金清算独立确认后才能付出。"""
        planned = [
            (
                "REFUND_ISSUED",
                {
                    "store_id": store_id,
                    "order_no": order_no,
                    "amount": amount,
                    "receipt_no": receipt_no,
                },
            )
        ]

        def validate() -> None:
            self._require_stage(store_id, *_DISPOSITION_STAGES)
            order = self._order_or_raise(store_id, order_no)
            self._require_live_order(order)
            if amount <= 0 or amount > order["amount"] + 1e-9:
                raise ValidationError("退款金额必须为正且不超过订单金额")
            self._require_unfulfillable(store_id)
            self._require_confirmation(store_id, "finance")

        return self._commit(receipt_no, planned, validate)

    def restore_order_entitlement(
        self, store_id: str, order_no: str, receipt_no: str
    ) -> CommandResult:
        """无法履行的订单按来源恢复权益：储值退回余额，券恢复券权益。"""
        order = self._order_or_raise(store_id, order_no)
        restore_kind = {
            "stored_value": "stored_balance",
            "group_coupon": "coupon",
            "gift_coupon": "coupon",
            "payment": "refund_only",
        }.get(order["source_kind"])
        if restore_kind is None:
            raise ValidationError(f"未知订单来源：{order['source_kind']}")
        planned: list[tuple[str, dict[str, Any]]] = [
            (
                "ENTITLEMENT_RESTORED",
                {
                    "store_id": store_id,
                    "order_no": order_no,
                    "source_kind": order["source_kind"],
                    "restore_kind": restore_kind,
                    "amount": order["amount"],
                    "receipt_no": receipt_no,
                },
            )
        ]
        if order["source_kind"] == "stored_value":
            planned.append(
                (
                    "STORED_BALANCE_POSTED",
                    {
                        "store_id": store_id,
                        "customer_ref": order["customer_ref"],
                        "delta": order["amount"],
                        "reason": "restore_order",
                        "receipt_no": receipt_no,
                    },
                )
            )

        def validate() -> None:
            self._require_stage(store_id, *_DISPOSITION_STAGES)
            self._require_live_order(order)
            self._require_unfulfillable(store_id)
            self._require_confirmation(store_id, "finance")

        return self._commit(receipt_no, planned, validate)

    def record_coupon_redemption(
        self,
        store_id: str,
        redemption_no: str,
        amount: float,
        scope: str,
        receipt_no: str,
        order_no: Optional[str] = None,
    ) -> CommandResult:
        """券核销入账；同一核销编号在任何门店只计一次（历史核销不重复计算）。"""
        existing = self.projection.coupon_redemptions.get(redemption_no)
        if existing is not None:
            if abs(existing["amount"] - amount) > 1e-9:
                raise ValidationError(
                    f"核销编号 {redemption_no} 金额与历史记录不一致，疑似重复核销"
                )
            original = self.projection.event_ids.get(existing["event_id"])
            return CommandResult(events=[original] if original else [], replayed=True)

        payload = {
            "store_id": store_id,
            "source_ref": redemption_no,
            "amount": amount,
            "scope": scope,
            "receipt_no": receipt_no,
        }
        if order_no:
            payload["order_no"] = order_no
        return self._commit(
            f"redeem:{store_id}:{redemption_no}", [("ENTITLEMENT_SETTLED", payload)]
        )

    def transfer_balance(
        self,
        store_id: str,
        customer_ref: str,
        successor_ref: str,
        amount: float,
        receipt_no: str,
    ) -> CommandResult:
        """储值余额转承接门店：资金清算与承接门店双方独立确认后才能转移。"""
        planned = [
            (
                "BALANCE_TRANSFERRED",
                {
                    "store_id": store_id,
                    "customer_ref": customer_ref,
                    "successor_ref": successor_ref,
                    "amount": amount,
                    "receipt_no": receipt_no,
                },
            )
        ]

        def validate() -> None:
            self._require_stage(store_id, *_DISPOSITION_STAGES)
            balance = self.projection.balances.get((store_id, customer_ref))
            if balance is None or amount <= 0 or amount > balance["amount"] + 1e-9:
                raise ValidationError("转出金额必须为正且不超过剩余储值余额")
            plan = self.projection.require_store(store_id).get("successor")
            if plan is None or plan["successor_ref"] != successor_ref:
                raise ValidationError("承接门店与既定承接方案不一致")
            self._require_confirmation(store_id, "finance")
            self._require_confirmation(store_id, "successor")

        return self._commit(receipt_no, planned, validate)

    # --------------------------------------------------------------- 承接与确认

    def plan_successor(self, store_id: str, successor_ref: str, scope: list[str]) -> str:
        self._require_stage(
            store_id, "announced", "frozen", "production_stopped"
        )
        event = self._emit(
            "SUCCESSOR_PLANNED",
            "exit_plan",
            f"plan:{store_id}",
            {
                "store_id": store_id,
                "successor_ref": successor_ref,
                "scope": list(scope),
                "planned_at": self._at(),
            },
            f"successor-plan:{store_id}:{successor_ref}",
        )
        return event["event_id"]

    def _require_matching_successor(
        self, store_id: str, successor_ref: str, scope: str
    ) -> None:
        plan = self.projection.require_store(store_id).get("successor")
        if plan is None:
            raise ValidationError("尚未制定承接方案")
        if plan["successor_ref"] != successor_ref:
            raise ValidationError("承接门店与既定承接方案不一致")
        if scope not in plan["scope"]:
            raise ValidationError(f"承接范围 {scope} 不在方案范围内")

    def confirm_independent(
        self,
        store_id: str,
        party: str,
        confirmer_id: str,
        confirmed: bool = True,
    ) -> str:
        """资金清算方（finance）或承接门店（successor）独立确认。"""
        if party not in ("finance", "successor"):
            raise ValidationError("确认方只能是 finance 或 successor")
        store = self.projection.require_store(store_id)
        if party == "finance":
            if confirmer_id == self.manager_id(store_id):
                raise ResponsibilityViolation("资金清算确认人不能是本店店长，确认必须独立")
        if party == "successor":
            plan = store.get("successor")
            if plan is not None and not str(confirmer_id).startswith(
                f"{plan['successor_ref']}:"
            ):
                raise ResponsibilityViolation("承接确认必须来自承接门店身份")
        # 确认可撤销后重发，事件标识带聚合版本号，保证每次确认都是可投影的新事件
        seq = self.store.next_version("exit_plan", f"plan:{store_id}")
        event = self._emit(
            "INDEPENDENT_CONFIRMATION",
            "exit_plan",
            f"plan:{store_id}",
            {
                "store_id": store_id,
                "party": party,
                "confirmer_id": confirmer_id,
                "confirmed": confirmed,
            },
            f"confirm:{store_id}:{party}:{confirmer_id}:{confirmed}#{seq}",
        )
        return event["event_id"]

    def resolve_suspension(
        self,
        store_id: str,
        receipt_no: str,
        resolution: str,
        resolver_id: str,
    ) -> CommandResult:
        """人工裁决暂停回执：reissue（作废原编号，要求换新编号重试）或 reject（拒绝）。"""
        if resolution not in ("reissue", "reject"):
            raise ValidationError("裁决只能是 reissue 或 reject")
        store = self.projection.require_store(store_id)
        suspension = store["suspensions"].get(receipt_no)
        if suspension is None or suspension["status"] != "suspended":
            raise ValidationError("该回执不处于暂停状态")
        event = self._emit(
            "CLEARING_RESUMED",
            "exit_plan",
            f"plan:{store_id}",
            {
                "store_id": store_id,
                "receipt_no": receipt_no,
                "resolution": resolution,
                "resolver_id": resolver_id,
            },
            f"resume:{receipt_no}",
        )
        return CommandResult(events=[event])

    # ----------------------------------------------------------- 设备/员工/供应商

    def record_leased_equipment(
        self,
        store_id: str,
        equipment_no: str,
        lessor_ref: str,
        receipt_no: str,
    ) -> CommandResult:
        planned = [
            (
                "LEASE_EQUIPMENT_RECORDED",
                {
                    "store_id": store_id,
                    "equipment_no": equipment_no,
                    "lessor_ref": lessor_ref,
                    "receipt_no": receipt_no,
                },
            )
        ]

        def validate() -> None:
            self._require_stage(store_id, "operating", "announced")

        return self._commit(receipt_no, planned, validate)

    def close_lease(self, store_id: str, equipment_no: str, receipt_no: str) -> CommandResult:
        planned = [
            (
                "LEASE_CLOSED",
                {
                    "store_id": store_id,
                    "equipment_no": equipment_no,
                    "receipt_no": receipt_no,
                },
            )
        ]

        def validate() -> None:
            self._require_stage(
                store_id, "pickup_refund_closed", "lease_returned", "settlement"
            )
            if (
                self.projection.equipment.get((store_id, equipment_no), {}).get("status")
                != "leased"
            ):
                raise ValidationError("租赁设备不存在或已归还")

        return self._commit(receipt_no, planned, validate)

    def record_staff_handover(
        self, store_id: str, staff_id: str, items: list[str], receipt_no: str
    ) -> CommandResult:
        planned = [
            (
                "STAFF_HANDOVER_RECORDED",
                {
                    "store_id": store_id,
                    "staff_id": staff_id,
                    "items": items,
                    "receipt_no": receipt_no,
                },
            )
        ]

        def validate() -> None:
            self._require_stage(
                store_id, "announced", "frozen", "production_stopped"
            )
            if not items:
                raise ValidationError("交接事项不能为空")

        return self._commit(receipt_no, planned, validate)

    def confirm_staff_handover(
        self, store_id: str, staff_id: str, confirmer_id: str
    ) -> str:
        handover = self.projection.staff.get((store_id, staff_id))
        if handover is None:
            raise ValidationError("员工交接单不存在")
        if handover["confirmed"]:
            raise ValidationError("交接已确认")
        if confirmer_id == staff_id:
            raise ResponsibilityViolation("员工不能确认自己的交接")
        event = self._emit(
            "STAFF_HANDOVER_CONFIRMED",
            "handover_task",
            f"handover:{store_id}:staff:{staff_id}",
            {
                "store_id": store_id,
                "staff_id": staff_id,
                "confirmer_id": confirmer_id,
            },
            f"staff-confirm:{store_id}:{staff_id}",
        )
        return event["event_id"]

    def record_consignment(
        self,
        store_id: str,
        supplier_ref: str,
        batch_id: str,
        quantity: int,
        amount: float,
        receipt_no: str,
    ) -> CommandResult:
        """供应商寄售原料台账（冻结前登记）。"""
        planned = [
            (
                "SUPPLIER_CONSIGNMENT_RECORDED",
                {
                    "store_id": store_id,
                    "supplier_ref": supplier_ref,
                    "batch_id": batch_id,
                    "quantity": quantity,
                    "amount": amount,
                    "receipt_no": receipt_no,
                },
            )
        ]

        def validate() -> None:
            self._require_stage(store_id, "operating", "announced")
            if quantity <= 0 or amount < 0:
                raise ValidationError("寄售数量必须为正，金额不能为负")

        return self._commit(receipt_no, planned, validate)

    def settle_supplier(
        self,
        store_id: str,
        supplier_ref: str,
        amount: float,
        receipt_no: str,
    ) -> CommandResult:
        planned = [
            (
                "SUPPLIER_SETTLED",
                {
                    "store_id": store_id,
                    "supplier_ref": supplier_ref,
                    "amount": amount,
                    "receipt_no": receipt_no,
                },
            )
        ]

        def validate() -> None:
            self._require_stage(store_id, "lease_returned", "settlement")
            ledger = self.projection.suppliers.get((store_id, supplier_ref))
            if ledger is None:
                raise ValidationError("供应商台账不存在")
            outstanding = (
                sum(line["amount"] for line in ledger["lines"]) - ledger["settled_amount"]
            )
            if amount <= 0 or amount > outstanding + 1e-9:
                raise ValidationError("结算金额必须为正且不超过未结金额")
            self._require_confirmation(store_id, "finance")

        return self._commit(receipt_no, planned, validate)

    # ----------------------------------------------------------------- 关闭

    def unfinished_obligations(self, store_id: str) -> dict[str, list[str]]:
        p = self.projection
        blockers: dict[str, list[str]] = {}
        open_orders = [
            f"订单 {o['order_no']} 状态 {o['status']}"
            for (sid, _), o in p.orders.items()
            if sid == store_id and o["status"] not in _TERMINAL_ORDER_STATUS
        ]
        if open_orders:
            blockers["orders"] = open_orders
        balances = [
            f"客户 {b['customer_ref']} 余额 {b['amount']}"
            for (sid, _), b in p.balances.items()
            if sid == store_id and b["amount"] > 1e-9
        ]
        if balances:
            blockers["balances"] = balances
        equipment = [
            f"设备 {e['equipment_no']} 未归还"
            for (sid, _), e in p.equipment.items()
            if sid == store_id and e["status"] != "closed"
        ]
        if equipment:
            blockers["equipment"] = equipment
        staff = [
            f"员工 {s['staff_id']} 交接未确认"
            for (sid, _), s in p.staff.items()
            if sid == store_id and not s["confirmed"]
        ]
        if staff:
            blockers["staff"] = staff
        suppliers = [
            f"供应商 {s['supplier_ref']} 未结清"
            for (sid, _), s in p.suppliers.items()
            if sid == store_id
            and abs(s["settled_amount"] - sum(line["amount"] for line in s["lines"])) > 1e-9
        ]
        if suppliers:
            blockers["suppliers"] = suppliers
        losses = [
            f"损耗 {loss['loss_id']} 未{'核实' if loss['verified_quantity'] is None else '审批'}"
            for (sid, _), batch in p.batches.items()
            if sid == store_id
            for loss in batch["losses"].values()
            if loss["decision"] is None
        ]
        if losses:
            blockers["losses"] = losses
        store = p.stores.get(store_id, {})
        missing = [
            name
            for name in ("finance", "successor")
            if name not in store.get("confirmations", {})
        ]
        if missing:
            blockers["confirmations"] = [f"缺少独立确认：{name}" for name in missing]
        active = [
            r
            for r in store.get("suspensions", {}).values()
            if r["status"] == "suspended"
        ]
        if active:
            blockers["suspensions"] = [f"回执 {r['receipt_no']} 暂停未裁决" for r in active]
        return blockers

    def close_handover(self, store_id: str, receipt_no: str) -> CommandResult:
        planned = [
            (
                "HANDOVER_CLOSED",
                {"store_id": store_id, "closed_at": self._at(), "receipt_no": receipt_no},
            )
        ]

        def validate() -> None:
            self._require_stage(store_id, "settlement")
            blockers = self.unfinished_obligations(store_id)
            if blockers:
                raise SettlementBlocked(f"仍有未完成义务，不能关闭：{canonical(blockers)}")

        return self._commit(receipt_no, planned, validate)
