"""阶段调度器：用可控时钟推进停产、取货退款、租约归还、结算期限。

阶段状态只来自事件流（STAGE_ADVANCED / OBLIGATION_FROZEN），因此进程重启后
重建服务即可延续原阶段；时钟对齐到事件日志最后时间，不会倒流。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from .clock import ControllableClock
from .service import STAGE_ORDER, ExitClearingService

# 冻结后由调度器驱动的阶段及其期限字段（顺序即推进顺序）
_DEADLINE_STAGES = [
    ("production_stop_at", "production_stopped"),
    ("pickup_refund_deadline", "pickup_refund_closed"),
    ("lease_return_deadline", "lease_returned"),
    ("settlement_deadline", "settlement"),
]


class StageScheduler:
    def __init__(self, service: ExitClearingService) -> None:
        self.service = service

    @property
    def clock(self) -> ControllableClock:
        return self.service.clock

    def _deadlines(self, store_id: str) -> dict[str, str]:
        store = self.service.projection.require_store(store_id)
        frozen = store.get("frozen")
        if frozen is None:
            return {}
        return frozen.get("deadlines", {})

    def advance_to(self, moment: datetime, store_ids: Optional[list[str]] = None) -> list[dict[str, Any]]:
        """推进时钟到指定时刻，并自动跨越所有已到期的阶段。"""
        self.clock.advance_to(moment)
        return self.tick(store_ids)

    def advance(self, delta, store_ids: Optional[list[str]] = None) -> list[dict[str, Any]]:
        self.clock.advance(delta)
        return self.tick(store_ids)

    def tick(self, store_ids: Optional[list[str]] = None) -> list[dict[str, Any]]:
        """在当前时钟时刻检查所有已到期阶段并推进，返回新产生的阶段事件。"""
        progressed: list[dict[str, Any]] = []
        targets = store_ids or list(self.service.projection.stores)
        for store_id in targets:
            store = self.service.projection.stores.get(store_id)
            if store is None or store.get("frozen") is None:
                continue
            if store["stage"] == "closed":
                continue
            deadlines = self._deadlines(store_id)
            current_rank = STAGE_ORDER.index(store["stage"])
            for deadline_field, target_stage in _DEADLINE_STAGES:
                target_rank = STAGE_ORDER.index(target_stage)
                if target_rank <= current_rank:
                    continue
                deadline = datetime.fromisoformat(deadlines[deadline_field])
                if self.clock.now() >= deadline:
                    event = self._advance_stage(store_id, store["stage"], target_stage)
                    progressed.append(event)
                    store = self.service.projection.require_store(store_id)
        return progressed

    def _advance_stage(self, store_id: str, from_stage: str, to_stage: str) -> dict[str, Any]:
        payload = {
            "store_id": store_id,
            "from_stage": from_stage,
            "to_stage": to_stage,
            "advanced_at": self.clock.now().isoformat(),
        }
        return self.service._emit(
            "STAGE_ADVANCED", "exit_plan", f"plan:{store_id}", payload,
            f"stage:{store_id}:{to_stage}",
        )

    def schedule_status(self, store_id: str) -> dict[str, Any]:
        """返回各阶段的到期时间与是否已跨越。"""
        store = self.service.projection.require_store(store_id)
        deadlines = self._deadlines(store_id)
        current_rank = STAGE_ORDER.index(store["stage"])
        rows = []
        for field, stage in _DEADLINE_STAGES:
            rows.append({
                "stage": stage,
                "deadline": deadlines.get(field),
                "reached": STAGE_ORDER.index(stage) <= current_rank,
                "due": deadlines.get(field) is not None
                and self.clock.now() >= datetime.fromisoformat(deadlines[field]),
            })
        return {"now": self.clock.now().isoformat(), "stage": store["stage"], "stages": rows}
