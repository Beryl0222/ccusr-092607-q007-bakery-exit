"""清算调度器：用可控时钟推进停产、取货、退款、租约、结算五个阶段。

阶段顺序固定；每个阶段登记截止时刻，显式推进。进程恢复后从事件日志重建，
当前阶段（第一个尚未推进的阶段）自动延续，并记录 RECOVERY_RESUMED。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from . import aggregates as agg
from .store import EventSpec, EventStore, StoredEvent

ISO = "%Y-%m-%dT%H:%M:%S%z"

# 固定阶段顺序：停产 -> 取货 -> 退款 -> 租约撤场 -> 供应商结算
STAGE_ORDER = ("production_halt", "pickup", "refund", "lease", "settlement")


def _iso(moment: datetime) -> str:
    return moment.strftime(ISO)


class Scheduler:
    def __init__(self, store: EventStore, clock: Any, case_id: str) -> None:
        self.store = store
        self.clock = clock
        self.case_id = case_id
        self.scheduler_id = f"scheduler-{case_id}"

    # ============================================================ 重建

    @classmethod
    def restore(cls, store: EventStore, clock: Any, case_id: str) -> "Scheduler":
        """进程恢复入口：折叠历史阶段，返回延续原阶段的调度器。"""
        scheduler = cls(store, clock, case_id)
        state = scheduler._state()
        if state is not None and state.get("stages"):
            # 恢复事实记录为事件；若此前从未推进过任何阶段，也照样可追溯重启。
            store.commit([
                EventSpec(
                    "scheduler", scheduler.scheduler_id, "RECOVERY_RESUMED",
                    {
                        "case_ref": case_id,
                        "resumed_at": _iso(clock.now),
                        "stage": scheduler.current_stage,
                    },
                    store.version_of("scheduler", scheduler.scheduler_id),
                )
            ], _iso(clock.now))
        return scheduler

    def _state(self) -> Optional[dict[str, Any]]:
        return agg.fold(self.store.events_for("scheduler", self.scheduler_id))

    def _spec(self, event_type: str, payload: dict[str, Any]) -> EventSpec:
        return EventSpec(
            "scheduler", self.scheduler_id, event_type, payload,
            self.store.version_of("scheduler", self.scheduler_id),
        )

    # ============================================================ 排期

    def schedule_stage(self, stage: str, deadline_at: datetime, command_id: Optional[str] = None) -> list[StoredEvent]:
        if stage not in STAGE_ORDER:
            raise ValueError(f"未知阶段 {stage}")
        if deadline_at.tzinfo is None:
            raise ValueError("阶段截止时间必须携带时区")
        cached = self.store.command_result(command_id) if command_id else None
        if cached is not None:
            return cached
        return self.store.commit([self._spec("STAGE_SCHEDULED", {
            "case_ref": self.case_id,
            "stage": stage,
            "deadline_at": _iso(deadline_at),
        })], _iso(self.clock.now), command_id=command_id)

    def schedule_all(self, deadlines: dict[str, datetime]) -> list[StoredEvent]:
        """一次性登记五个阶段截止时刻，顺序必须与 STAGE_ORDER 一致或递增。"""
        missing = [s for s in STAGE_ORDER if s not in deadlines]
        if missing:
            raise ValueError(f"缺少阶段排期：{missing}")
        ordered = [deadlines[s] for s in STAGE_ORDER]
        for earlier, later in zip(ordered, ordered[1:]):
            if later < earlier:
                raise ValueError("阶段截止时刻必须不早于前一阶段")
        produced: list[StoredEvent] = []
        for stage, deadline in zip(STAGE_ORDER, ordered):
            produced.extend(self.schedule_stage(stage, deadline))
        return produced

    # ============================================================ 推进

    @property
    def stages(self) -> dict[str, dict[str, Any]]:
        state = self._state()
        return dict(state.get("stages", {})) if state else {}

    @property
    def current_stage(self) -> Optional[str]:
        """第一个已排期但尚未推进的阶段；全部推进完返回 None。"""
        state = self._state()
        if state is None:
            return None
        for stage in state.get("order", []):
            if state["stages"][stage]["advanced_at"] is None:
                return stage
        return None

    def advance_stage(self, stage: str, command_id: Optional[str] = None) -> list[StoredEvent]:
        """只能按顺序推进：前一阶段未推进则拒绝。"""
        if command_id and self.store.command_result(command_id) is not None:
            return self.store.command_result(command_id)  # type: ignore[return-value]
        state = self._state()
        if state is None or stage not in state.get("stages", {}):
            raise ValueError(f"阶段 {stage} 尚未排期")
        if state["stages"][stage]["advanced_at"] is not None:
            raise ValueError(f"阶段 {stage} 已推进，重放不产生新事件")
        expected_index = STAGE_ORDER.index(stage)
        for prior in STAGE_ORDER[:expected_index]:
            record = state["stages"].get(prior)
            if record is not None and record["advanced_at"] is None:
                raise ValueError(f"前序阶段 {prior} 尚未推进，不能越级进入 {stage}")
        return self.store.commit([self._spec("STAGE_ADVANCED", {
            "case_ref": self.case_id,
            "stage": stage,
            "advanced_at": _iso(self.clock.now),
        })], _iso(self.clock.now), command_id=command_id)

    # ============================================================ 时钟查询

    def _parse(self, value: str) -> datetime:
        return datetime.strptime(value, ISO)

    def due_stages(self) -> list[str]:
        """当前时钟已到截止时刻、但尚未推进的阶段（按顺序）。"""
        state = self._state()
        if state is None:
            return []
        return [
            stage for stage in state.get("order", [])
            if state["stages"][stage]["advanced_at"] is None
            and self.clock.now >= self._parse(state["stages"][stage]["deadline_at"])
        ]

    def overdue_stages(self) -> list[str]:
        """已超过截止时刻仍未推进的阶段。"""
        state = self._state()
        if state is None:
            return []
        return [
            stage for stage in state.get("order", [])
            if state["stages"][stage]["advanced_at"] is None
            and self.clock.now > self._parse(state["stages"][stage]["deadline_at"])
        ]

    def recovery_count(self) -> int:
        state = self._state()
        return int(state.get("recovery_count", 0)) if state else 0
