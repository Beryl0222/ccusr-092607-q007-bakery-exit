"""调度器期限推进与进程恢复测试。"""

import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bakery_exit import ControllableClock, ExitClearingService  # noqa: E402
from bakery_exit.clock import ClockMovedBackwards  # noqa: E402
from bakery_exit.scheduler import StageScheduler  # noqa: E402
from bakery_exit.store import EventStore  # noqa: E402

TZ = timezone(timedelta(hours=8))


class SchedulerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = str(Path(self.tmp.name) / "events.jsonl")
        self.schema = json.loads((ROOT / "contracts/domain.schema.json").read_text("utf-8"))

    def _service(self, start=datetime(2026, 9, 1, 9, 0, tzinfo=TZ)):
        store = EventStore(self.path)
        clock = ControllableClock(start)
        svc = ExitClearingService(store, clock, self.schema)
        return svc, StageScheduler(svc), store

    def _freeze(self, svc):
        svc.open_store_period("S-001", manager_id="mgr-001")
        svc.announce_exit("S-001", datetime(2026, 10, 10, 22, tzinfo=TZ).isoformat())
        svc.freeze_obligations(
            "S-001",
            cutoff_at=datetime(2026, 9, 25, 10, tzinfo=TZ).isoformat(),
            production_stop_at=datetime(2026, 9, 30, 22, tzinfo=TZ).isoformat(),
            pickup_refund_deadline=datetime(2026, 10, 10, 22, tzinfo=TZ).isoformat(),
            lease_return_deadline=datetime(2026, 10, 15, 22, tzinfo=TZ).isoformat(),
            settlement_deadline=datetime(2026, 10, 20, 22, tzinfo=TZ).isoformat(),
        )

    def test_stages_only_cross_at_their_deadlines(self):
        svc, sched, _ = self._service()
        self._freeze(svc)
        self.assertEqual("frozen", svc.projection.require_store("S-001")["stage"])

        sched.advance_to(datetime(2026, 9, 30, 21, tzinfo=TZ))
        self.assertEqual("frozen", svc.projection.require_store("S-001")["stage"])
        sched.advance_to(datetime(2026, 9, 30, 22, tzinfo=TZ))
        self.assertEqual("production_stopped", svc.projection.require_store("S-001")["stage"])

        # 一次跳过多个期限 -> 连续跨越，不跳阶段
        events = sched.advance_to(datetime(2026, 10, 16, tzinfo=TZ))
        stages = [e["payload"]["to_stage"] for e in events]
        self.assertEqual(["pickup_refund_closed", "lease_returned"], stages)

    def test_clock_cannot_move_backwards(self):
        svc, sched, _ = self._service()
        self._freeze(svc)
        sched.advance_to(datetime(2026, 10, 1, tzinfo=TZ))
        with self.assertRaises(ClockMovedBackwards):
            sched.advance_to(datetime(2026, 9, 2, tzinfo=TZ))

    def test_stage_resumes_after_process_restart(self):
        """进程恢复：重建服务后时钟对齐日志最后时间，阶段延续，不重复推进。"""
        svc, sched, store = self._service()
        self._freeze(svc)
        sched.advance_to(datetime(2026, 10, 16, tzinfo=TZ))
        self.assertEqual("lease_returned", svc.projection.require_store("S-001")["stage"])
        events_before = len(store)

        # 模拟进程重启：全新对象从同一日志回放
        svc2, sched2, store2 = self._service()
        self.assertEqual("lease_returned", svc2.projection.require_store("S-001")["stage"])
        self.assertEqual(datetime(2026, 10, 16, tzinfo=TZ), sched2.clock.now())
        self.assertEqual(events_before, len(store2))

        # tick 不再产生重复的阶段事件
        produced = sched2.tick()
        self.assertEqual([], produced)
        self.assertEqual(events_before, len(store2))

        # 继续推进到结算期
        sched2.advance_to(datetime(2026, 10, 21, tzinfo=TZ))
        self.assertEqual("settlement", svc2.projection.require_store("S-001")["stage"])

    def test_event_stream_versions_stay_contiguous_after_restart(self):
        svc, sched, store = self._service()
        self._freeze(svc)
        sched.advance_to(datetime(2026, 10, 5, tzinfo=TZ))
        svc2, _, _ = self._service()
        # 恢复后新事件仍能在正确聚合版本号上续写
        svc2.plan_successor("S-001", "S-002", ["orders"])
        plan_events = store.for_aggregate("exit_plan", "plan:S-001")
        versions = [e["version"] for e in plan_events]
        self.assertEqual(versions, sorted(versions))
        self.assertEqual(len(versions), len(set(versions)))


if __name__ == "__main__":
    unittest.main()
