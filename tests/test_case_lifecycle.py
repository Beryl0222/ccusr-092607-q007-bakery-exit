"""案件生命周期：发布、冻结快照、停产、暂停/恢复、关闭与未完成义务门禁。"""

import datetime as dt
import unittest

from _fixtures import build_case
from bakery_exit import (
    ConcurrencyError,
    ConflictHoldError,
    ControllableClock,
    ExitClearingService,
    EventStore,
    PhaseError,
)
from bakery_exit.errors import IdempotencyConflictError
from bakery_exit.store import EventSpec


def settle_case(svc, clock, case_id="case-1") -> None:
    """把案件下全部义务清结到可关闭状态。"""
    svc.pickup_order("order-1", "cust-A")
    svc.pickup_order("order-2", "cust-A")
    svc.mark_unfulfillable("order-3", "生效日后无法制做", "ops")
    svc.request_loss("batch-C1", "loss-1", "manager-li", 3, "报损")
    svc.approve_loss("batch-C1", "loss-1", "district-wang")
    svc.return_consignment("batch-F1", "supplier-F", 20)
    svc.propose_settlement("supplier-F", "set-1", 360.0, "accountant")
    svc.confirm_settlement("supplier-F", "set-1", "supplier-contact")
    svc.pay_settlement("supplier-F", "set-1")
    svc.terminate_lease("equip-O1", "ops")
    svc.remove_equipment("equip-O1", "manager-li")
    svc.settle_deposit("equip-O1", 1000.0)
    svc.propose_plan("plan-1", case_id, "BJ-009", [], "planner")
    svc.confirm_funds("plan-1", "finance")
    svc.confirm_successor("plan-1", "BJ-009", "succ-mgr")
    svc.complete_plan("plan-1")
    svc.assign_handover(
        "task-1", case_id, "staff", "交接", "leader", ["staff"],
        clock.now + dt.timedelta(days=5),
    )
    svc.acknowledge_handover("task-1", "employee-chen")
    svc.close_handover("task-1", "employee-chen")


class CaseLifecycleTests(unittest.TestCase):
    def test_freeze_then_intake_is_closed(self) -> None:
        svc, store, clock, case_id, _ = build_case()
        with self.assertRaises(PhaseError):
            svc.deliver_batch("batch-NEW", case_id, "可颂", 10, "central_factory")
        with self.assertRaises(PhaseError):
            svc.register_order(
                "order-NEW", case_id, "cust-C", "prepaid",
                clock.now + dt.timedelta(days=1), [{"sku": "x", "qty": 1}],
            )
        case = svc._state("store_period", case_id)
        self.assertEqual(case["status"], "frozen")
        self.assertEqual(64, len(case["snapshot_hash"]))

    def test_entities_outside_snapshot_are_rejected(self) -> None:
        svc, *_ = build_case()
        with self.assertRaises(PhaseError):
            svc.count_on_site("batch-UNKNOWN", "c", "manager-li", 1)

    def test_suspended_case_blocks_mutations_and_resumes(self) -> None:
        svc, *_ = build_case()
        case_id = "case-1"
        svc.suspend_case(case_id, "编号冲突", ["receipt-x"])
        with self.assertRaises(ConflictHoldError):
            svc.count_on_site("batch-C1", "c2", "manager-li", 40)
        svc.resume_case(case_id, "hq-arbitrator")
        svc.count_on_site("batch-C1", "c2", "manager-li", 40)
        self.assertEqual(svc._state("store_period", case_id)["status"], "resumed")

    def test_resume_requires_suspended(self) -> None:
        svc, *_ = build_case()
        with self.assertRaises(PhaseError):
            svc.resume_case("case-1", "hq-arbitrator")

    def test_duplicate_announce_is_rejected(self) -> None:
        clock = ControllableClock(dt.datetime(2026, 9, 1, tzinfo=dt.timezone(dt.timedelta(hours=8))))
        svc = ExitClearingService(EventStore(), clock)
        eff = clock.now + dt.timedelta(days=7)
        svc.announce_exit("case-x", "S1", eff, "hq")
        with self.assertRaises(PhaseError):
            svc.announce_exit("case-x", "S1", eff, "hq")

    def test_closure_requires_no_pending_obligations(self) -> None:
        svc, *_ = build_case()
        with self.assertRaises(PhaseError) as ctx:
            svc.close_case("case-1", "r1", ["all"], "BJ-009", "hq")
        self.assertIn("未完成义务", str(ctx.exception))

    def test_full_close_and_replay(self) -> None:
        svc, store, clock, case_id, _ = build_case()
        settle_case(svc, clock, case_id)

        self.assertEqual([], svc.unfinished_obligations(case_id))
        events_before = len(store.all_events())
        svc.close_case(case_id, "receipt-1", ["all"], "BJ-009", "hq", command_id="cmd-close")
        self.assertEqual("closed", svc._state("store_period", case_id)["status"])

        # 相同回执重放：不产生事件、不转移资产/余额
        svc.close_case(case_id, "receipt-1", ["all"], "BJ-009", "hq", command_id="cmd-close")
        self.assertEqual(events_before + 1, len(store.all_events()))

        with self.assertRaises(PhaseError):
            svc.refund_balance("ledger-A", 1.0, "x", "finance")

    def test_closure_conflict_suspends_case(self) -> None:
        svc, store, clock, case_id, _ = build_case()
        settle_case(svc, clock, case_id)

        svc.close_case(case_id, "receipt-9", ["orders", "batches"], "BJ-009", "hq")
        events_before = len(store.all_events())

        # 编号相同、范围不同 -> 暂停；第二次调用只追加 CASE_SUSPENDED
        with self.assertRaises(IdempotencyConflictError):
            svc.close_case(case_id, "receipt-9", ["orders"], "BJ-009", "hq")
        self.assertEqual("suspended", svc._state("store_period", case_id)["status"])
        tail = store.all_events()[events_before:]
        self.assertEqual(["CASE_SUSPENDED"], [e.event_type for e in tail])

    def test_stale_version_is_rejected(self) -> None:
        """两个并发提交基于同一旧版本：存储层乐观锁拒绝后者。"""
        svc, store, clock, case_id, _ = build_case()
        stale = store.version_of("store_period", case_id)
        svc.suspend_case(case_id, "first", ["a"])
        with self.assertRaises(ConcurrencyError):
            store.commit([
                EventSpec(
                    "store_period", case_id, "CASE_RESUMED",
                    {"resumed_by": "x", "resumed_at": "2026-09-02T09:00:00+0800"},
                    stale,
                )
            ], "2026-09-02T09:00:00+0800")


if __name__ == "__main__":
    unittest.main()
