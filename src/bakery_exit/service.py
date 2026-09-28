"""门店退场义务清算服务。

所有状态判断都来自事件存储折叠结果；命令要么原子写入一批事件，要么不产生事件。
关键规则：
- 停业发布即冻结债权债务快照，冻结后不得新增债权债务实体；
- 店长可以核实现场数量、可以发起损耗减免，但不能批准自己的减免；
- 资金清算与承接门店由不同责任方独立确认；
- 生效日前仍可履行的订单正常提货；无法履行的订单按来源恢复权益；
- 同一权益的退款、换店、提货互斥；核销凭据全局唯一，门店合并不重复计算；
- 相同关闭回执重放不转移任何资产，编号相同而内容不同则暂停案件。
"""

from __future__ import annotations

import contextvars
import functools
import hashlib
import json
from datetime import datetime
from typing import Any, Callable, Optional

from . import aggregates as agg
from .errors import (
    AuthorizationError,
    ConflictHoldError,
    DuplicateRedemptionError,
    EntitlementExhaustedError,
    IdempotencyConflictError,
    PhaseError,
)
from .store import EventSpec, EventStore, StoredEvent

ISO = "%Y-%m-%dT%H:%M:%S%z"

ORDER_TERMINAL = {"fulfilled", "unfulfillable", "transferred"}
PAYMENT_ORIGINS = {"stored_value", "voucher", "groupbuy", "prepaid", "gift"}
VOUCHER_ORIGINS = {"voucher", "groupbuy", "gift"}
BATCH_ORIGINS = {"central_factory", "consignment", "on_site"}


def _iso(moment: datetime) -> str:
    return moment.strftime(ISO)


def snapshot_hash(snapshot: dict[str, Any]) -> str:
    body = json.dumps(snapshot, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def idempotent(method: Callable[..., list[StoredEvent]]) -> Callable[..., list[StoredEvent]]:
    """相同 command_id 的重放先于一切状态校验返回首次结果，不再产生事件。

    被包装方法不声明 command_id 形参；命令编号经上下文传给存储，保证跨进程重放。
    """

    @functools.wraps(method)
    def wrapper(self: "ExitClearingService", *args: Any, **kwargs: Any) -> list[StoredEvent]:
        command_id = kwargs.pop("command_id", None)
        if command_id is not None and self.store.command_result(command_id) is not None:
            return self.store.command_result(command_id)  # type: ignore[return-value]
        if command_id is None:
            return method(self, *args, **kwargs)
        token = _current_command.set(command_id)
        try:
            return method(self, *args, **kwargs)
        finally:
            _current_command.reset(token)

    return wrapper


_current_command: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "exit_clearing_command_id", default=None
)


class ExitClearingService:
    def __init__(self, store: EventStore, clock: Any) -> None:
        self.store = store
        self.clock = clock

    # ============================================================ 基础设施

    def _state(self, aggregate_type: str, aggregate_id: str) -> Optional[dict[str, Any]]:
        return agg.fold(self.store.events_for(aggregate_type, aggregate_id))

    def _spec(self, aggregate_type: str, aggregate_id: str, event_type: str, payload: dict[str, Any]) -> EventSpec:
        return EventSpec(
            aggregate_type, aggregate_id, event_type, payload,
            self.store.version_of(aggregate_type, aggregate_id),
        )

    def _commit(self, specs: list[EventSpec], command_id: Optional[str] = None) -> list[StoredEvent]:
        return self.store.commit(
            specs, _iso(self.clock.now), command_id=command_id or _current_command.get()
        )

    def _case(self, case_id: str) -> dict[str, Any]:
        case = self._state("store_period", case_id)
        if case is None:
            raise PhaseError(f"清算案件 {case_id} 尚未建立", field="case_id")
        return case

    def _require_writable(self, case_id: str) -> dict[str, Any]:
        case = self._case(case_id)
        if case.get("status") == "suspended":
            raise ConflictHoldError("案件因编号冲突已暂停，须先恢复后才能继续操作", field="case_id")
        if case.get("status") == "closed":
            raise PhaseError("案件已关闭，不得再变动", field="case_id")
        return case

    def _require_frozen(self, case_id: str) -> dict[str, Any]:
        case = self._require_writable(case_id)
        if case.get("status") not in {"frozen", "resumed"}:
            raise PhaseError("债权债务快照尚未冻结，不能执行清算动作", field="case_id")
        return case

    def _require_intake_open(self, case_id: str) -> dict[str, Any]:
        """经营期可登记台账；停业发布后、冻结前仍可补录；冻结即关闭新增入口。"""
        case = self._case(case_id)
        if case.get("status") == "suspended":
            raise ConflictHoldError("案件已暂停", field="case_id")
        if case.get("status") not in {"announced"}:
            raise PhaseError("快照冻结后不得新增债权债务实体", field="case_id")
        return case

    def _require_in_snapshot(self, case: dict[str, Any], key: str, entity_id: str) -> None:
        ids = set(case.get("snapshot", {}).get(key, []))
        if ids and entity_id not in ids:
            raise PhaseError(f"{entity_id} 不在冻结快照的 {key} 清册内", field=entity_id)

    # ============================================================ 暂停与恢复

    @idempotent
    def suspend_case(self, case_id: str, reason: str, conflict_refs: list[str]) -> list[StoredEvent]:
        self._case(case_id)
        return self._commit([self._spec("store_period", case_id, "CASE_SUSPENDED", {
            "reason": reason,
            "conflict_refs": list(conflict_refs),
            "suspended_at": _iso(self.clock.now),
        })])

    @idempotent
    def resume_case(self, case_id: str, resumed_by: str) -> list[StoredEvent]:
        case = self._case(case_id)
        if case.get("status") != "suspended":
            raise PhaseError("只有处于暂停状态的案件可以恢复", field="case_id")
        return self._commit([self._spec("store_period", case_id, "CASE_RESUMED", {
            "resumed_by": resumed_by,
            "resumed_at": _iso(self.clock.now),
        })])

    # ============================================================ 发布、冻结、停产

    @idempotent
    def announce_exit(
        self, case_id: str, store_code: str, effective_at: datetime, announced_by: str
    ) -> list[StoredEvent]:
        if self._state("store_period", case_id) is not None:
            raise PhaseError("清算案件已存在，停业决定不可重复发布", field="case_id")
        if effective_at.tzinfo is None:
            raise PhaseError("生效时间必须携带时区", field="effective_at")
        return self._commit([self._spec("store_period", case_id, "EXIT_ANNOUNCED", {
            "store_code": store_code,
            "announced_at": _iso(self.clock.now),
            "effective_at": _iso(effective_at),
            "announced_by": announced_by,
        })])

    @idempotent
    def freeze_obligations(
        self, case_id: str, snapshot: dict[str, Any], frozen_by: str
    ) -> list[StoredEvent]:
        case = self._require_writable(case_id)
        if case.get("status") != "announced":
            raise PhaseError("只有已发布停业决定的案件才能冻结快照", field="case_id")
        return self._commit([self._spec("store_period", case_id, "OBLIGATION_FROZEN", {
            "cutoff_at": _iso(self.clock.now),
            "snapshot_hash": snapshot_hash(snapshot),
            "snapshot": snapshot,
            "frozen_by": frozen_by,
        })])

    @idempotent
    def halt_production(self, case_id: str, halted_by: str) -> list[StoredEvent]:
        self._require_frozen(case_id)
        return self._commit([self._spec("store_period", case_id, "PRODUCTION_HALTED", {
            "halted_at": _iso(self.clock.now),
            "halted_by": halted_by,
        })])

    # ============================================================ 订单登记

    @idempotent
    def register_order(
        self,
        order_id: str,
        case_ref: str,
        customer_id: str,
        source: str,
        scheduled_pickup_at: datetime,
        items: list[dict[str, Any]],
        payment: Optional[dict[str, Any]] = None,
    ) -> list[StoredEvent]:
        if source not in PAYMENT_ORIGINS:
            raise PhaseError(f"订单来源 {source} 未登记", field="source")
        if scheduled_pickup_at.tzinfo is None:
            raise PhaseError("约定取货时间必须携带时区", field="scheduled_pickup_at")
        if self._state("store_period", case_ref) is not None:
            self._require_intake_open(case_ref)
        return self._commit([self._spec("customer_order", order_id, "ORDER_REGISTERED", {
            "case_ref": case_ref,
            "order_ref": order_id,
            "customer_id": customer_id,
            "source": source,
            "scheduled_pickup_at": _iso(scheduled_pickup_at),
            "items": list(items),
            "payment": payment or {"method": source},
        })])

    def _open_order(self, order_id: str) -> dict[str, Any]:
        order = self._state("customer_order", order_id)
        if order is None:
            raise PhaseError(f"订单 {order_id} 不存在", field="order_id")
        if order["status"] in ORDER_TERMINAL:
            raise PhaseError(f"订单已处于终态 {order['status']}，不能重复处理", field="order_id")
        return order

    # ============================================================ 提货 / 无法履行 / 换店

    @idempotent
    def pickup_order(self, order_id: str, picked_by: str) -> list[StoredEvent]:
        """生效日前正常完成：提货核销，权益只消耗一次。"""
        order = self._open_order(order_id)
        case = self._require_frozen(order["case_ref"])
        effective_at = datetime.strptime(case["effective_at"], ISO)
        scheduled_at = datetime.strptime(order["scheduled_pickup_at"], ISO)
        if self.clock.now > effective_at or scheduled_at > effective_at:
            raise PhaseError(
                "该订单无法在生效日前履行，须走权益恢复或换店承接，不能按正常订单提货",
                field="order_id",
            )

        payment = order.get("payment", {})
        method = payment.get("method", order["source"])
        specs: list[EventSpec] = []

        if method == "stored_value":
            ledger_id = payment["ledger_id"]
            self._require_in_snapshot(case, "ledgers", ledger_id)
            ledger = self._state("customer_entitlement", ledger_id)
            if ledger is None:
                raise PhaseError("储值账户不存在", field="ledger_id")
            hold_ref = payment.get("hold_ref")
            hold = ledger["holds"].get(hold_ref) if hold_ref else None
            released_back = hold["amount"] if hold and not hold["released"] else 0
            projected_available = agg.balance_available(ledger) + released_back - payment["amount"]
            if projected_available < 0:
                raise EntitlementExhaustedError("储值余额不足以完成本次提货，退款/换店可能已占用", field="ledger_id")
            if hold and not hold["released"]:
                specs.append(self._spec("customer_entitlement", ledger_id, "BALANCE_HOLD_RELEASED", {
                    "hold_ref": hold_ref,
                }))
            specs.append(self._spec("customer_entitlement", ledger_id, "BALANCE_CONSUMED", {
                "case_ref": order["case_ref"],
                "consume_ref": order_id,
                "order_ref": order_id,
                "amount": payment["amount"],
                "consumed_at": _iso(self.clock.now),
            }))
        elif method in VOUCHER_ORIGINS:
            voucher_id = payment["voucher_code"]
            self._require_in_snapshot(case, "vouchers", voucher_id)
            self._guard_redemption(voucher_id, order_id)
            specs.append(self._spec("voucher", voucher_id, "VOUCHER_REDEEMED", {
                "voucher_code": voucher_id,
                "store_code": case["store_code"],
                "redemption_ref": order_id,
                "redeemed_at": _iso(self.clock.now),
            }))

        specs.append(self._spec("customer_order", order_id, "ORDER_FULFILLED", {
            "order_ref": order_id,
            "fulfilled_at": _iso(self.clock.now),
            "picked_by": picked_by,
        }))
        return self._commit(specs)

    @idempotent
    def mark_unfulfillable(
        self, order_id: str, reason: str, decided_by: str
    ) -> list[StoredEvent]:
        """无法履行的订单按来源恢复权益：退储值、恢复券、登记预付退款。"""
        order = self._open_order(order_id)
        case = self._require_frozen(order["case_ref"])
        payment = order.get("payment", {})
        method = payment.get("method", order["source"])

        specs = [self._spec("customer_order", order_id, "ORDER_MARKED_UNFULFILLABLE", {
            "order_ref": order_id,
            "reason": reason,
            "decided_at": _iso(self.clock.now),
            "decided_by": decided_by,
        })]

        if method == "stored_value":
            ledger_id = payment["ledger_id"]
            self._require_in_snapshot(case, "ledgers", ledger_id)
            ledger = self._state("customer_entitlement", ledger_id)
            hold_ref = payment.get("hold_ref")
            released_back = 0.0
            if ledger is not None and hold_ref and not ledger["holds"].get(hold_ref, {}).get("released", True):
                released_back = ledger["holds"][hold_ref]["amount"]
                specs.append(self._spec("customer_entitlement", ledger_id, "BALANCE_HOLD_RELEASED", {
                    "hold_ref": hold_ref,
                }))
            if ledger is not None and agg.balance_available(ledger) + released_back < payment["amount"]:
                raise EntitlementExhaustedError(
                    "储值余额已被其他退款/换店占用，不能重复退还", field="ledger_id"
                )
            specs.append(self._spec("customer_entitlement", ledger_id, "BALANCE_REFUNDED", {
                "case_ref": order["case_ref"],
                "amount": payment["amount"],
                "reason_ref": order_id,
            }))
            specs.append(self._spec("customer_order", order_id, "ENTITLEMENT_RESTORED", {
                "order_ref": order_id,
                "source": "stored_value",
                "restoration": {"type": "cash_refund", "ledger_id": ledger_id, "amount": payment["amount"]},
            }))
        elif method in VOUCHER_ORIGINS:
            voucher_id = payment["voucher_code"]
            self._require_in_snapshot(case, "vouchers", voucher_id)
            specs.append(self._spec("voucher", voucher_id, "VOUCHER_RESTORED", {
                "voucher_code": voucher_id,
                "source": method,
                "reason": order_id,
                "restored_at": _iso(self.clock.now),
            }))
            specs.append(self._spec("customer_order", order_id, "ENTITLEMENT_RESTORED", {
                "order_ref": order_id,
                "source": method,
                "restoration": {"type": "voucher", "voucher_code": voucher_id},
            }))
        elif method == "prepaid":
            specs.append(self._spec("customer_order", order_id, "ENTITLEMENT_RESTORED", {
                "order_ref": order_id,
                "source": "prepaid",
                "restoration": {"type": "cash_refund", "amount": payment["amount"]},
            }))
        return self._commit(specs)

    @idempotent
    def transfer_order(self, order_id: str, plan_id: str) -> list[StoredEvent]:
        """换店承接：与提货、退款互斥，订单只能进入一种终态。"""
        order = self._open_order(order_id)
        case = self._require_frozen(order["case_ref"])
        plan = self._require_confirmed_plan(plan_id)
        if plan["case_ref"] != order["case_ref"]:
            raise PhaseError("承接方案与订单不属于同一案件", field="plan_ref")
        scope = plan.get("order_scope") or []
        if scope and order["order_ref"] not in scope:
            raise PhaseError("订单不在承接方案范围内", field="order_id")
        payment = order.get("payment", {})
        method = payment.get("method", order["source"])
        specs: list[EventSpec] = []
        if method in VOUCHER_ORIGINS:
            voucher_id = payment["voucher_code"]
            self._require_in_snapshot(case, "vouchers", voucher_id)
            voucher = self._state("voucher", voucher_id)
            if voucher is not None and voucher.get("status") != "redeemed":
                specs.append(self._spec("voucher", voucher_id, "VOUCHER_TRANSFERRED", {
                    "voucher_code": voucher_id,
                    "successor_store_code": plan["successor_store_code"],
                    "plan_ref": plan_id,
                }))
        specs.append(self._spec("customer_order", order_id, "ORDER_TRANSFERRED", {
            "successor_ref": plan.get("successor_ref", plan["successor_store_code"]),
            "order_scope": scope,
            "plan_ref": plan_id,
            "transferred_at": _iso(self.clock.now),
        }))
        return self._commit(specs)

    # ============================================================ 储值账户

    @idempotent
    def open_balance_ledger(
        self, ledger_id: str, case_ref: str, customer_id: str, amount: float, currency: str = "CNY"
    ) -> list[StoredEvent]:
        if self._state("store_period", case_ref) is not None:
            self._require_intake_open(case_ref)
        return self._commit([self._spec("customer_entitlement", ledger_id, "BALANCE_LEDGER_OPENED", {
            "case_ref": case_ref,
            "customer_id": customer_id,
            "amount": amount,
            "currency": currency,
        })])

    @idempotent
    def hold_for_order(
        self, ledger_id: str, hold_ref: str, order_ref: str, amount: float
    ) -> list[StoredEvent]:
        ledger = self._state("customer_entitlement", ledger_id)
        if ledger is None:
            raise PhaseError("储值账户不存在", field="ledger_id")
        if agg.balance_available(ledger) < amount:
            raise EntitlementExhaustedError("可用储值余额不足，不能占用", field="ledger_id")
        return self._commit([self._spec("customer_entitlement", ledger_id, "BALANCE_HELD", {
            "hold_ref": hold_ref,
            "order_ref": order_ref,
            "amount": amount,
            "held_at": _iso(self.clock.now),
        })])

    @idempotent
    def refund_balance(
        self, ledger_id: str, amount: float, reason_ref: str, commanded_by: str
    ) -> list[StoredEvent]:
        ledger = self._state("customer_entitlement", ledger_id)
        if ledger is None:
            raise PhaseError("储值账户不存在", field="ledger_id")
        self._require_frozen(ledger.get("case_ref", ""))
        if agg.balance_available(ledger) < amount:
            raise EntitlementExhaustedError("可用余额不足，退款与提货/换店不得重复消耗", field="ledger_id")
        return self._commit([self._spec("customer_entitlement", ledger_id, "BALANCE_REFUNDED", {
            "case_ref": ledger.get("case_ref"),
            "amount": amount,
            "reason_ref": reason_ref,
            "commanded_by": commanded_by,
            "refunded_at": _iso(self.clock.now),
        })])

    @idempotent
    def transfer_balance(
        self, ledger_id: str, successor_store_code: str, amount: float, reason_ref: str, plan_id: str
    ) -> list[StoredEvent]:
        ledger = self._state("customer_entitlement", ledger_id)
        if ledger is None:
            raise PhaseError("储值账户不存在", field="ledger_id")
        plan = self._require_confirmed_plan(plan_id)
        if plan["successor_store_code"] != successor_store_code:
            raise PhaseError("承接门店与已确认方案不一致", field="successor_store_code")
        if agg.balance_available(ledger) < amount:
            raise EntitlementExhaustedError("可用余额不足，换店与退款/提货不得重复消耗", field="ledger_id")
        return self._commit([self._spec("customer_entitlement", ledger_id, "BALANCE_TRANSFERRED", {
            "case_ref": ledger.get("case_ref"),
            "successor_store_code": successor_store_code,
            "amount": amount,
            "reason_ref": reason_ref,
            "plan_ref": plan_id,
            "transferred_at": _iso(self.clock.now),
        })])

    # ============================================================ 券与核销

    @idempotent
    def register_voucher(
        self, voucher_id: str, case_ref: str, voucher_code: str, source: str,
        face_value: float, currency: str = "CNY",
    ) -> list[StoredEvent]:
        if source not in VOUCHER_ORIGINS:
            raise PhaseError(f"券来源 {source} 未登记", field="source")
        if self._state("store_period", case_ref) is not None:
            self._require_intake_open(case_ref)
        return self._commit([self._spec("voucher", voucher_id, "VOUCHER_REGISTERED", {
            "case_ref": case_ref,
            "voucher_code": voucher_code,
            "source": source,
            "face_value": face_value,
            "currency": currency,
        })])

    def _redemption_used(self, redemption_ref: str) -> bool:
        """核销凭据全局唯一：跨聚合扫描全部事件，门店合并也不重复计算。"""
        return any(
            e.event_type == "VOUCHER_REDEEMED" and e.payload.get("redemption_ref") == redemption_ref
            for e in self.store.all_events()
        )

    def _guard_redemption(self, voucher_id: str, redemption_ref: str) -> None:
        voucher = self._state("voucher", voucher_id)
        if voucher is None:
            raise PhaseError("券不存在", field="voucher_code")
        if voucher.get("status") in {"transferred"}:
            raise PhaseError("券已随订单转店，不得在原门店核销", field="voucher_code")
        if redemption_ref in voucher.get("redemptions", {}):
            raise DuplicateRedemptionError("该核销凭据已使用，不得重复核销", field="redemption_ref")
        if self._redemption_used(redemption_ref):
            raise DuplicateRedemptionError(
                "核销凭据已在其他门店使用，历史核销不得因门店合并重复计算", field="redemption_ref"
            )

    @idempotent
    def redeem_voucher(self, voucher_id: str, redemption_ref: str, store_code: str) -> list[StoredEvent]:
        self._guard_redemption(voucher_id, redemption_ref)
        return self._commit([self._spec("voucher", voucher_id, "VOUCHER_REDEEMED", {
            "voucher_code": voucher_id,
            "store_code": store_code,
            "redemption_ref": redemption_ref,
            "redeemed_at": _iso(self.clock.now),
        })])

    @idempotent
    def transfer_voucher(self, voucher_id: str, plan_id: str) -> list[StoredEvent]:
        voucher = self._state("voucher", voucher_id)
        if voucher is None:
            raise PhaseError("券不存在", field="voucher_code")
        if voucher.get("status") == "redeemed":
            raise PhaseError("已核销券不得转店", field="voucher_code")
        plan = self._require_confirmed_plan(plan_id)
        return self._commit([self._spec("voucher", voucher_id, "VOUCHER_TRANSFERRED", {
            "voucher_code": voucher_id,
            "successor_store_code": plan["successor_store_code"],
            "plan_ref": plan_id,
        })])

    # ============================================================ 库存、现制损耗

    @idempotent
    def deliver_batch(
        self, batch_id: str, case_ref: str, sku: str, quantity: float, origin: str
    ) -> list[StoredEvent]:
        if origin not in BATCH_ORIGINS:
            raise PhaseError(f"批次来源 {origin} 未登记", field="origin")
        if self._state("store_period", case_ref) is not None:
            self._require_intake_open(case_ref)
        return self._commit([self._spec("inventory_position", batch_id, "BATCH_DELIVERED", {
            "case_ref": case_ref,
            "batch_ref": batch_id,
            "sku": sku,
            "quantity": quantity,
            "origin": origin,
            "delivered_at": _iso(self.clock.now),
        })])

    def _batch_in_frozen_case(self, batch_id: str) -> dict[str, Any]:
        batch = self._state("inventory_position", batch_id)
        if batch is None:
            raise PhaseError(f"批次 {batch_id} 不存在", field="batch_id")
        case = self._require_frozen(batch["case_ref"])
        self._require_in_snapshot(case, "batches", batch_id)
        return batch

    @idempotent
    def count_on_site(
        self, batch_id: str, count_ref: str, counted_by: str, quantity: float
    ) -> list[StoredEvent]:
        """店长可以核实现场数量（仅记录，不构成减免批准）。"""
        self._batch_in_frozen_case(batch_id)
        return self._commit([self._spec("inventory_position", batch_id, "ON_SITE_COUNTED", {
            "batch_ref": batch_id,
            "count_ref": count_ref,
            "counted_by": counted_by,
            "quantity": quantity,
            "counted_at": _iso(self.clock.now),
        })])

    @idempotent
    def request_loss(
        self, batch_id: str, request_ref: str, requested_by: str, quantity: float, reason: str
    ) -> list[StoredEvent]:
        """店长发起损耗减免申请。"""
        batch = self._batch_in_frozen_case(batch_id)
        if quantity <= 0:
            raise PhaseError("损耗数量必须为正", field="quantity")
        if request_ref in batch.get("loss_requests", {}):
            raise PhaseError("损耗申请编号已存在", field="request_ref")
        return self._commit([self._spec("inventory_position", batch_id, "LOSS_REQUESTED", {
            "batch_ref": batch_id,
            "request_ref": request_ref,
            "requested_by": requested_by,
            "quantity": quantity,
            "reason": reason,
            "requested_at": _iso(self.clock.now),
        })])

    @idempotent
    def approve_loss(self, batch_id: str, request_ref: str, approved_by: str) -> list[StoredEvent]:
        """批准损耗：批准人不得是申请人本人（店长不能批自己的减免）。"""
        batch = self._batch_in_frozen_case(batch_id)
        request = batch.get("loss_requests", {}).get(request_ref)
        if request is None:
            raise PhaseError("损耗申请不存在", field="request_ref")
        if request["status"] != "requested":
            raise PhaseError("损耗申请已处理", field="request_ref")
        if approved_by == request["requested_by"]:
            raise AuthorizationError("店长不能批准自己发起的损耗减免", field="approved_by")
        if agg.batch_remaining(batch) < request["quantity"]:
            raise EntitlementExhaustedError("批次余量不足，损耗核销与转出/退回不得重复", field="batch_id")
        return self._commit([
            self._spec("inventory_position", batch_id, "LOSS_APPROVED", {
                "batch_ref": batch_id,
                "request_ref": request_ref,
                "approved_by": approved_by,
                "approved_at": _iso(self.clock.now),
            }),
            self._spec("inventory_position", batch_id, "BATCH_WRITTEN_OFF", {
                "batch_ref": batch_id,
                "quantity": request["quantity"],
                "reason_ref": request_ref,
            }),
        ])

    @idempotent
    def transfer_batch(self, batch_id: str, quantity: float, plan_id: str) -> list[StoredEvent]:
        batch = self._batch_in_frozen_case(batch_id)
        plan = self._require_confirmed_plan(plan_id)
        if agg.batch_remaining(batch) < quantity:
            raise EntitlementExhaustedError("批次余量不足，转出与损耗/退货不得重复", field="batch_id")
        return self._commit([self._spec("inventory_position", batch_id, "BATCH_TRANSFERRED", {
            "case_ref": batch["case_ref"],
            "batch_ref": batch_id,
            "successor_store_code": plan["successor_store_code"],
            "quantity": quantity,
            "plan_ref": plan_id,
        })])

    @idempotent
    def return_consignment(self, batch_id: str, supplier_id: str, quantity: float) -> list[StoredEvent]:
        """寄售原料退回供应商，冲减可处置余量。"""
        batch = self._batch_in_frozen_case(batch_id)
        if batch.get("origin") != "consignment":
            raise PhaseError("只有寄售批次可以退回供应商", field="batch_id")
        if agg.batch_remaining(batch) < quantity:
            raise EntitlementExhaustedError("批次余量不足，退货与损耗/转出不得重复", field="batch_id")
        return self._commit([self._spec("inventory_position", batch_id, "CONSIGNMENT_RETURNED", {
            "case_ref": batch["case_ref"],
            "supplier_id": supplier_id,
            "batch_ref": batch_id,
            "quantity": quantity,
            "returned_at": _iso(self.clock.now),
        })])

    # ============================================================ 供应商结算

    @idempotent
    def open_supplier_ledger(self, supplier_id: str, case_ref: str, supplier_name: str) -> list[StoredEvent]:
        if self._state("store_period", case_ref) is not None:
            self._require_intake_open(case_ref)
        return self._commit([self._spec("supplier_account", supplier_id, "SUPPLIER_LEDGER_OPENED", {
            "case_ref": case_ref,
            "supplier_id": supplier_id,
            "supplier_name": supplier_name,
        })])

    @idempotent
    def register_consignment(
        self, supplier_id: str, batch_id: str, case_ref: str, sku: str,
        quantity: float, unit_amount: float,
    ) -> list[StoredEvent]:
        """寄售原料入库：批次台账与供应商往来一次原子登记。"""
        if self._state("store_period", case_ref) is not None:
            self._require_intake_open(case_ref)
        return self._commit([
            self._spec("inventory_position", batch_id, "BATCH_DELIVERED", {
                "case_ref": case_ref,
                "batch_ref": batch_id,
                "sku": sku,
                "quantity": quantity,
                "origin": "consignment",
                "delivered_at": _iso(self.clock.now),
            }),
            self._spec("supplier_account", supplier_id, "CONSIGNMENT_RECEIVED", {
                "case_ref": case_ref,
                "supplier_id": supplier_id,
                "batch_ref": batch_id,
                "sku": sku,
                "quantity": quantity,
                "unit_amount": unit_amount,
            }),
        ])

    @idempotent
    def propose_settlement(
        self, supplier_id: str, settlement_ref: str, amount: float, proposed_by: str
    ) -> list[StoredEvent]:
        supplier = self._state("supplier_account", supplier_id)
        if supplier is None:
            raise PhaseError("供应商台账不存在", field="supplier_id")
        self._require_frozen(supplier["case_ref"])
        if settlement_ref in supplier.get("settlements", {}):
            raise PhaseError("结算单编号已存在", field="settlement_ref")
        return self._commit([self._spec("supplier_account", supplier_id, "SETTLEMENT_PROPOSED", {
            "case_ref": supplier["case_ref"],
            "supplier_id": supplier_id,
            "settlement_ref": settlement_ref,
            "amount": amount,
            "proposed_by": proposed_by,
            "proposed_at": _iso(self.clock.now),
        })])

    @idempotent
    def confirm_settlement(self, supplier_id: str, settlement_ref: str, confirmed_by: str) -> list[StoredEvent]:
        """供应商（或独立财务）独立确认：确认人不得是结算发起人。"""
        supplier = self._state("supplier_account", supplier_id)
        if supplier is None:
            raise PhaseError("供应商台账不存在", field="supplier_id")
        settlement = supplier.get("settlements", {}).get(settlement_ref)
        if settlement is None:
            raise PhaseError("结算单不存在", field="settlement_ref")
        if settlement["status"] != "proposed":
            raise PhaseError("结算单已确认或已支付", field="settlement_ref")
        if confirmed_by == settlement["proposed_by"]:
            raise AuthorizationError("资金清算必须由独立于发起方的责任人确认", field="confirmed_by")
        return self._commit([self._spec("supplier_account", supplier_id, "SETTLEMENT_CONFIRMED", {
            "case_ref": supplier["case_ref"],
            "supplier_id": supplier_id,
            "settlement_ref": settlement_ref,
            "confirmed_by": confirmed_by,
            "confirmed_at": _iso(self.clock.now),
        })])

    @idempotent
    def pay_settlement(self, supplier_id: str, settlement_ref: str) -> list[StoredEvent]:
        supplier = self._state("supplier_account", supplier_id)
        if supplier is None:
            raise PhaseError("供应商台账不存在", field="supplier_id")
        settlement = supplier.get("settlements", {}).get(settlement_ref)
        if settlement is None or settlement["status"] != "confirmed":
            raise PhaseError("结算单未经独立确认，不得支付", field="settlement_ref")
        return self._commit([self._spec("supplier_account", supplier_id, "SETTLEMENT_PAID", {
            "case_ref": supplier["case_ref"],
            "supplier_id": supplier_id,
            "settlement_ref": settlement_ref,
            "paid_at": _iso(self.clock.now),
        })])

    # ============================================================ 租赁设备

    @idempotent
    def register_lease(
        self, equipment_id: str, case_ref: str, lessor: str, lease_end_at: datetime
    ) -> list[StoredEvent]:
        if lease_end_at.tzinfo is None:
            raise PhaseError("租约到期时间必须携带时区", field="lease_end_at")
        if self._state("store_period", case_ref) is not None:
            self._require_intake_open(case_ref)
        return self._commit([self._spec("equipment_lease", equipment_id, "LEASE_REGISTERED", {
            "case_ref": case_ref,
            "equipment_ref": equipment_id,
            "lessor": lessor,
            "lease_end_at": _iso(lease_end_at),
        })])

    def _lease(self, equipment_id: str) -> dict[str, Any]:
        lease = self._state("equipment_lease", equipment_id)
        if lease is None:
            raise PhaseError("设备租约不存在", field="equipment_ref")
        self._require_frozen(lease["case_ref"])
        return lease

    @idempotent
    def terminate_lease(self, equipment_id: str, terminated_by: str) -> list[StoredEvent]:
        lease = self._lease(equipment_id)
        if lease["status"] != "registered":
            raise PhaseError("租约已终止或设备已撤场", field="equipment_ref")
        return self._commit([self._spec("equipment_lease", equipment_id, "LEASE_TERMINATED", {
            "case_ref": lease["case_ref"],
            "equipment_ref": equipment_id,
            "terminated_at": _iso(self.clock.now),
            "terminated_by": terminated_by,
        })])

    @idempotent
    def remove_equipment(self, equipment_id: str, witness_by: str) -> list[StoredEvent]:
        lease = self._lease(equipment_id)
        if lease["status"] != "terminated":
            raise PhaseError("租约未终止，不能撤设备", field="equipment_ref")
        return self._commit([self._spec("equipment_lease", equipment_id, "EQUIPMENT_REMOVED", {
            "case_ref": lease["case_ref"],
            "equipment_ref": equipment_id,
            "removed_at": _iso(self.clock.now),
            "witness_by": witness_by,
        })])

    @idempotent
    def settle_deposit(self, equipment_id: str, amount: float) -> list[StoredEvent]:
        lease = self._lease(equipment_id)
        if lease["status"] != "removed":
            raise PhaseError("设备未撤场，押金不能结算", field="equipment_ref")
        return self._commit([self._spec("equipment_lease", equipment_id, "DEPOSIT_SETTLED", {
            "case_ref": lease["case_ref"],
            "equipment_ref": equipment_id,
            "amount": amount,
            "settled_at": _iso(self.clock.now),
        })])

    # ============================================================ 员工交接

    @idempotent
    def assign_handover(
        self, task_id: str, case_ref: str, category: str, title: str,
        owner_role: str, visibility: list[str], due_at: datetime,
    ) -> list[StoredEvent]:
        if due_at.tzinfo is None:
            raise PhaseError("交接期限必须携带时区", field="due_at")
        self._require_frozen(case_ref)
        return self._commit([self._spec("handover_task", task_id, "HANDOVER_ASSIGNED", {
            "case_ref": case_ref,
            "task_ref": task_id,
            "category": category,
            "title": title,
            "owner_role": owner_role,
            "visibility": list(visibility),
            "due_at": _iso(due_at),
            "assigned_at": _iso(self.clock.now),
        })])

    def _handover(self, task_id: str) -> dict[str, Any]:
        task = self._state("handover_task", task_id)
        if task is None:
            raise PhaseError("交接事项不存在", field="task_ref")
        self._require_writable(task["case_ref"])
        return task

    @idempotent
    def acknowledge_handover(self, task_id: str, acknowledged_by: str) -> list[StoredEvent]:
        task = self._handover(task_id)
        if task["status"] != "assigned":
            raise PhaseError("交接事项已认领或已关闭", field="task_ref")
        return self._commit([self._spec("handover_task", task_id, "HANDOVER_ACKNOWLEDGED", {
            "task_ref": task_id,
            "acknowledged_by": acknowledged_by,
            "acknowledged_at": _iso(self.clock.now),
        })])

    @idempotent
    def close_handover(self, task_id: str, closed_by: str) -> list[StoredEvent]:
        self._handover(task_id)
        return self._commit([self._spec("handover_task", task_id, "HANDOVER_CLOSED", {
            "task_ref": task_id,
            "closed_at": _iso(self.clock.now),
            "closed_by": closed_by,
        })])

    # ============================================================ 承接方案（双重独立确认）

    @idempotent
    def propose_plan(
        self, plan_id: str, case_ref: str, successor_store_code: str,
        order_scope: list[str], proposed_by: str,
    ) -> list[StoredEvent]:
        self._require_frozen(case_ref)
        if self._state("exit_plan", plan_id) is not None:
            raise PhaseError("承接方案已存在", field="plan_ref")
        return self._commit([self._spec("exit_plan", plan_id, "PLAN_PROPOSED", {
            "case_ref": case_ref,
            "plan_ref": plan_id,
            "successor_store_code": successor_store_code,
            "order_scope": list(order_scope),
            "proposed_by": proposed_by,
            "proposed_at": _iso(self.clock.now),
        })])

    def _plan(self, plan_id: str) -> dict[str, Any]:
        plan = self._state("exit_plan", plan_id)
        if plan is None:
            raise PhaseError("承接方案不存在", field="plan_ref")
        self._require_writable(plan["case_ref"])
        return plan

    def _require_confirmed_plan(self, plan_id: str) -> dict[str, Any]:
        plan = self._plan(plan_id)
        if not (plan["funds_confirmed"] and plan["successor_confirmed"]):
            raise PhaseError("承接方案尚未完成资金与承接门店双重确认", field="plan_ref")
        return plan

    @idempotent
    def confirm_funds(self, plan_id: str, confirmed_by: str) -> list[StoredEvent]:
        plan = self._plan(plan_id)
        if plan["funds_confirmed"]:
            raise PhaseError("资金清算已确认", field="plan_ref")
        return self._commit([self._spec("exit_plan", plan_id, "FUNDS_CONFIRMED", {
            "case_ref": plan["case_ref"],
            "plan_ref": plan_id,
            "confirmed_by": confirmed_by,
            "confirmed_at": _iso(self.clock.now),
        })])

    @idempotent
    def confirm_successor(
        self, plan_id: str, successor_ref: str, confirmed_by: str
    ) -> list[StoredEvent]:
        """承接门店独立确认，且确认人不能与资金确认人为同一人。"""
        plan = self._plan(plan_id)
        if plan["successor_confirmed"]:
            raise PhaseError("承接门店已确认", field="plan_ref")
        if plan["funds_confirmed"] and confirmed_by == plan.get("funds_confirmed_by"):
            raise AuthorizationError("资金清算与承接门店必须由不同责任方确认", field="confirmed_by")
        return self._commit([self._spec("exit_plan", plan_id, "SUCCESSOR_CONFIRMED", {
            "case_ref": plan["case_ref"],
            "plan_ref": plan_id,
            "successor_ref": successor_ref,
            "confirmed_by": confirmed_by,
            "confirmed_at": _iso(self.clock.now),
        })])

    @idempotent
    def complete_plan(self, plan_id: str) -> list[StoredEvent]:
        plan = self._require_confirmed_plan(plan_id)
        if plan.get("status") == "completed":
            raise PhaseError("承接方案已完成", field="plan_ref")
        return self._commit([self._spec("exit_plan", plan_id, "PLAN_COMPLETED", {
            "case_ref": plan["case_ref"],
            "plan_ref": plan_id,
            "completed_at": _iso(self.clock.now),
        })])

    # ============================================================ 关闭回执

    def close_case(
        self,
        case_id: str,
        closure_ref: str,
        scope: list[str],
        successor_ref: str,
        closed_by: str,
        command_id: Optional[str] = None,
    ) -> list[StoredEvent]:
        """关闭案件。

        相同 closure_ref/command_id 重放只返回首次结果，不再转移资产或余额；
        编号相同但日期、范围或承接方不同 -> 暂停案件。
        """
        dedupe_key = command_id or closure_ref
        payload = {
            "closed_at": _iso(self.clock.now),
            "closure_ref": closure_ref,
            "scope": list(scope),
            "successor_ref": successor_ref,
            "closed_by": closed_by,
        }

        if self.store.command_result(dedupe_key) is not None:
            # 编号已存在：一致回执原样重放（无任何资产/余额变动）；
            # 日期、范围或承接方不同 -> 指纹冲突，暂停案件。
            spec = EventSpec(
                "store_period", case_id, "CASE_CLOSED", payload,
                self.store.version_of("store_period", case_id),
            )
            try:
                return self.store.commit([spec], _iso(self.clock.now), command_id=dedupe_key)
            except IdempotencyConflictError:
                self._suspend_for_closure_conflict(case_id, dedupe_key)
                raise

        case = self._require_writable(case_id)
        pending = self.unfinished_obligations(case_id)
        if pending:
            raise PhaseError("仍有未完成义务，不能关闭：" + "、".join(pending), field="case_id")
        spec = EventSpec(
            "store_period", case_id, "CASE_CLOSED", payload,
            self.store.version_of("store_period", case_id),
        )
        try:
            return self.store.commit([spec], _iso(self.clock.now), command_id=dedupe_key)
        except IdempotencyConflictError:
            self._suspend_for_closure_conflict(case_id, dedupe_key)
            raise

    def _suspend_for_closure_conflict(self, case_id: str, dedupe_key: str) -> None:
        self.store.commit([
            EventSpec(
                "store_period", case_id, "CASE_SUSPENDED",
                {
                    "reason": "关闭回执与首次回执的日期/范围/承接方不一致",
                    "conflict_refs": [dedupe_key],
                    "suspended_at": _iso(self.clock.now),
                },
                self.store.version_of("store_period", case_id),
            )
        ], _iso(self.clock.now))

    # ============================================================ 未完成义务（总部视角）

    def unfinished_obligations(self, case_id: str) -> list[str]:
        pending: list[str] = []
        seen: set[tuple[str, str]] = set()
        for event in self.store.all_events():
            key = (event.aggregate_type, event.aggregate_id)
            if key in seen:
                continue
            seen.add(key)
            if event.aggregate_type == "store_period":
                continue
            state = self._state(*key)
            if state is None or state.get("case_ref") != case_id:
                continue
            status = state.get("status")
            if event.aggregate_type == "customer_order" and status not in ORDER_TERMINAL:
                pending.append(f"订单 {event.aggregate_id} 状态 {status}")
            elif event.aggregate_type == "supplier_account":
                for ref, settlement in state.get("settlements", {}).items():
                    if settlement["status"] != "paid":
                        pending.append(f"供应商 {event.aggregate_id} 结算 {ref} 状态 {settlement['status']}")
            elif event.aggregate_type == "equipment_lease" and status != "removed":
                pending.append(f"设备 {event.aggregate_id} 状态 {status}")
            elif event.aggregate_type == "handover_task" and status != "closed":
                pending.append(f"交接 {event.aggregate_id} 状态 {status}")
            elif event.aggregate_type == "exit_plan" and status != "completed":
                pending.append(f"承接方案 {event.aggregate_id} 状态 {status}")
        return sorted(set(pending))
