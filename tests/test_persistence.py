"""事件存储持久化：JSONL 重放、跨进程命令幂等与指纹冲突。"""

import tempfile
import unittest
from pathlib import Path

from bakery_exit import ControllableClock, EventStore, ExitClearingService
from bakery_exit.errors import IdempotencyConflictError
from bakery_exit.store import EventSpec
import datetime as dt


class PersistenceTests(unittest.TestCase):
    def test_replay_rebuilds_versions_and_commands(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "events.jsonl"
            tz = dt.timezone(dt.timedelta(hours=8))
            clock = ControllableClock(dt.datetime(2026, 9, 1, 9, 0, tzinfo=tz))
            store = EventStore(path)
            svc = ExitClearingService(store, clock)
            eff = clock.now + dt.timedelta(days=10)
            svc.announce_exit("case-p", "BJ-001", eff, "hq", command_id="announce-p")

            # 新进程重放：版本延续
            store2 = EventStore(path)
            self.assertEqual(1, store2.version_of("store_period", "case-p"))
            replay = store2.command_result("announce-p")
            self.assertIsNotNone(replay)
            self.assertEqual(1, len(replay))

            # 相同命令编号、相同指纹：返回首次事件，不追加
            clock2 = ControllableClock(dt.datetime(2026, 9, 2, 9, 0, tzinfo=tz))
            svc2 = ExitClearingService(store2, clock2)
            produced = svc2.announce_exit("case-p", "BJ-001", eff, "hq", command_id="announce-p")
            self.assertEqual(1, len(store2.all_events()))
            self.assertEqual(produced[0].event_id, replay[0].event_id)

    def test_same_command_different_payload_conflicts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "events.jsonl"
            store = EventStore(path)
            spec = EventSpec("store_period", "c", "CASE_SUSPENDED",
                             {"reason": "a", "conflict_refs": []}, 0)
            store.commit([spec], "2026-09-01T09:00:00+0800", command_id="cmd-x")
            different = EventSpec("store_period", "c", "CASE_SUSPENDED",
                                  {"reason": "b", "conflict_refs": []}, 1)
            with self.assertRaises(IdempotencyConflictError):
                store.commit([different], "2026-09-02T09:00:00+0800", command_id="cmd-x")


if __name__ == "__main__":
    unittest.main()
