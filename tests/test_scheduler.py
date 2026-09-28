"""调度器：可控时钟推进、阶段顺序、逾期查询与进程恢复延续。"""

import datetime as dt
import tempfile
import unittest
from pathlib import Path

from _fixtures import build_case
from bakery_exit import ControllableClock, EventStore, ExitClearingService, Scheduler
from bakery_exit.scheduler import STAGE_ORDER


class SchedulerTests(unittest.TestCase):
    def _scheduler(self):
        svc, store, clock, case_id, _ = build_case()
        scheduler = Scheduler(store, clock, case_id)
        deadlines = {
            stage: clock.now + dt.timedelta(days=i + 1)
            for i, stage in enumerate(STAGE_ORDER)
        }
        scheduler.schedule_all(deadlines)
        return svc, scheduler, store, clock, case_id

    def test_stages_must_advance_in_order(self) -> None:
        _, scheduler, *_ = self._scheduler()
        with self.assertRaises(ValueError):
            scheduler.advance_stage("refund")
        self.assertEqual("production_halt", scheduler.current_stage)
        scheduler.advance_stage("production_halt")
        self.assertEqual("pickup", scheduler.current_stage)

    def test_clock_drives_due_stages(self) -> None:
        _, scheduler, _, clock, _ = self._scheduler()
        self.assertEqual([], scheduler.due_stages())
        clock.advance(days=2, hours=1)
        self.assertEqual(["production_halt", "pickup"], scheduler.due_stages())
        scheduler.advance_stage("production_halt")
        # 已推进的阶段不再出现在逾期清单
        self.assertEqual(["pickup"], scheduler.overdue_stages())

    def test_advance_is_idempotent_replay(self) -> None:
        _, scheduler, store, clock, _ = self._scheduler()
        scheduler.advance_stage("production_halt", command_id="adv-1")
        before = len(store.all_events())
        # 同一命令重放不产生事件
        scheduler.advance_stage("production_halt", command_id="adv-1")
        self.assertEqual(before, len(store.all_events()))

    def test_deadlines_must_be_ordered(self) -> None:
        _, store, clock, case_id = self._make_bare()
        scheduler = Scheduler(store, clock, case_id)
        bad = {stage: clock.now + dt.timedelta(days=1) for stage in STAGE_ORDER}
        bad["refund"] = clock.now  # 早于 pickup
        with self.assertRaises(ValueError):
            scheduler.schedule_all(bad)

    def _make_bare(self):
        svc, store, clock, case_id, _ = build_case()
        return svc, store, clock, case_id


class SchedulerRecoveryTests(unittest.TestCase):
    def test_process_recovery_resumes_current_stage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "events.jsonl"
            tz = dt.timezone(dt.timedelta(hours=8))
            clock = ControllableClock(dt.datetime(2026, 9, 1, 9, 0, tzinfo=tz))
            store = EventStore(path)
            svc = ExitClearingService(store, clock)
            case_id = "case-r1"
            eff = clock.now + dt.timedelta(days=14)
            svc.announce_exit(case_id, "BJ-001", eff, "hq")
            svc.freeze_obligations(case_id, {}, "audit")

            scheduler = Scheduler(store, clock, case_id)
            deadlines = {s: clock.now + dt.timedelta(days=i + 1) for i, s in enumerate(STAGE_ORDER)}
            scheduler.schedule_all(deadlines)
            scheduler.advance_stage("production_halt")
            scheduler.advance_stage("pickup")
            self.assertEqual("refund", scheduler.current_stage)

            # 模拟进程重启：新存储从日志重放
            clock2 = ControllableClock(dt.datetime(2026, 9, 5, 9, 0, tzinfo=tz))
            store2 = EventStore(path)
            restored = Scheduler.restore(store2, clock2, case_id)
            self.assertEqual("refund", restored.current_stage)
            self.assertEqual(1, restored.recovery_count())

            # 恢复后继续推进原阶段
            restored.advance_stage("refund")
            self.assertEqual("lease", restored.current_stage)

            # 再次重启，阶段与恢复次数都延续
            clock3 = ControllableClock(dt.datetime(2026, 9, 6, 9, 0, tzinfo=tz))
            store3 = EventStore(path)
            restored2 = Scheduler.restore(store3, clock3, case_id)
            self.assertEqual("lease", restored2.current_stage)
            self.assertEqual(2, restored2.recovery_count())


if __name__ == "__main__":
    unittest.main()
