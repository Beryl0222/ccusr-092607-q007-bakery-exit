"""可控时钟：调度器用它推进停产、取货、退款、租约和结算期限。"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional


class ClockMovedBackwards(RuntimeError):
    """时钟只能前进；进程恢复时以日志最后时间为起点。"""


class ControllableClock:
    def __init__(self, start: datetime) -> None:
        if start.tzinfo is None or start.utcoffset() is None:
            raise ValueError("时钟起点必须携带时区")
        self._now = start

    def now(self) -> datetime:
        return self._now

    def advance_to(self, moment: datetime) -> datetime:
        if moment.tzinfo is None or moment.utcoffset() is None:
            raise ValueError("推进目标时间必须携带时区")
        if moment < self._now:
            raise ClockMovedBackwards(f"时钟不能倒流：{moment.isoformat()} < {self._now.isoformat()}")
        self._now = moment
        return self._now

    def advance(self, delta: timedelta) -> datetime:
        return self.advance_to(self._now + delta)

    def restore_to(self, moment: datetime) -> None:
        """仅用于进程恢复：直接对齐到事件日志中的最后时间，不做倒流校验。"""
        if moment.tzinfo is None or moment.utcoffset() is None:
            raise ValueError("恢复时间必须携带时区")
        self._now = moment

    def is_at_or_after(self, moment: Optional[datetime]) -> bool:
        return moment is not None and self._now >= moment
