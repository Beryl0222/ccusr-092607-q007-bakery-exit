"""可控时钟：测试与调度器用同一时钟推进截止期限。"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional


class ControllableClock:
    """可显式推进的时钟，所有时间均带时区，缺省使用东八区。"""

    def __init__(self, start: Optional[datetime] = None) -> None:
        if start is None:
            start = datetime(2026, 9, 1, 9, 0, tzinfo=timezone(timedelta(hours=8)))
        if start.tzinfo is None:
            raise ValueError("时钟初始时间必须携带时区")
        self._now = start

    @property
    def now(self) -> datetime:
        return self._now

    def advance(self, **delta) -> datetime:
        self._now = self._now + timedelta(**delta)
        return self._now

    def set_to(self, moment: datetime) -> datetime:
        if moment.tzinfo is None:
            raise ValueError("目标时间必须携带时区")
        if moment < self._now:
            raise ValueError("可控时钟不允许回拨")
        self._now = moment
        return self._now
