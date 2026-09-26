"""清算服务规则测试：冻结、三权分立、幂等、冲突暂停、权益互斥。"""

import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bakery_exit import (  # noqa: E402
    ConfirmationRequired,
    ControllableClock,
    EntitlementAlreadyConsumed,
    ExitClearingService,
    ResponsibilityViolation,
    SettlementBlocked,
    StageViolation,
    SuspendedError,
    ValidationError,
)
from bakery_exit.store import EventStore  # noqa: E402

TZ = timezone(timedelta(hours=8))


def iso(y, mo, d, h=9, mi=0):
    return datetime(y, mo, d, h, mi, tzinfo=TZ).isoformat()


class ClearingTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.schema = json.loads((ROOT / "contracts/domain.schema.json").read_text("utf-8"))
        self.svc, self.sched = self._build(self.tmp.name)

    def _build(self, directory: str):
        from bakery_exit.scheduler import StageScheduler

        store = EventStore(str(Path(directory) / "events.jsonl"))
        clock = ControllableClock(datetime(2026, 9, 1, 9, 0, tzinfo=TZ))
        svc = ExitClearingService(store, clock, self.schema)
        return svc, StageScheduler(svc)

    def bootstrap_frozen_store(self, sid="S-001", manager="mgr-001"):
        """经营期数据齐备并完成停业宣布+冻结，返回服务实例。"""
        svc = self.svc
        svc.open_store_period(sid, manager_id=manager)
        svc.deliver_batch(sid, "B-1", "吐司", 100, receipt_no=f"{sid}:d1")
        svc.record_consignment(sid, "sup-1", "B-2", 50, 800.0, f"{sid}:c1")
        svc.top_up_balance(sid, "cust-1", 200.0, f"{sid}:t1")
        svc.register_order(sid, "O-1", "stored_value", "sv-1", 80.0, "cust-1", f"{sid}:o1")
        svc.register_order(sid, "O-2", "group_coupon", "gc-1", 60.0, "cust-1", f"{sid}:o2")
        svc.record_leased_equipment(sid, "EQ-1", "lessor-1", f"{sid}:e1")
        svc.announce_exit(sid, iso(2026, 10, 10, 22))
        svc.record_staff_handover(sid, "emp-1", ["钥匙", "制服"], f"{sid}:h1")
        snapshot = svc.freeze_obligations(
            sid,
            cutoff_at=iso(2026, 9, 25, 10),
            production_stop_at=iso(2026, 9, 30, 22),
            pickup_refund_deadline=iso(2026, 10, 10, 22),
            lease_return_deadline=iso(2026, 10, 15, 22),
            settlement_deadline=iso(2026, 10, 20, 22),
        )
        return snapshot

    # -------------------------------------------------------------- 冻结快照

    def test_snapshot_freezes_obligations_and_hash_is_stable(self):
        snap1 = self.bootstrap_frozen_store()
        snap2 = self.svc._build_snapshot("S-001", snap1["cutoff_at"])
        self.assertEqual(snap1["snapshot_hash"], snap2["snapshot_hash"])
        self.assertIn("O-1", [o["order_no"] for o in snap1["orders"]])
        self.assertEqual(800.0, snap1["suppliers"][0]["lines"][0]["amount"])

    def test_no_new_debts_after_freeze(self):
        self.bootstrap_frozen_store()
        with self.assertRaises(StageViolation):
            self.svc.register_order(
                "S-001", "O-9", "payment", "p-9", 10.0, "cust-1", "S-001:o9"
            )
        with self.assertRaises(StageViolation):
            self.svc.deliver_batch("S-001", "B-9", "蛋糕", 5, receipt_no="S-001:d9")

    # -------------------------------------------------------------- 损耗三权分立

    def test_store_manager_can_verify_but_not_approve_own_loss(self):
        self.bootstrap_frozen_store()
        self.svc.report_fresh_loss("S-001", "B-1", "L-1", 10, "mgr-001")
        self.svc.verify_fresh_loss("S-001", "B-1", "L-1", 8, "mgr-001")
        with self.assertRaises(ResponsibilityViolation):
            self.svc.decide_fresh_loss("S-001", "B-1", "L-1", "waived", "mgr-001")
        result = self.svc.decide_fresh_loss("S-001", "B-1", "L-1", "waived", "audit-001")
        self.assertTrue(result.changed)

    def test_loss_cannot_be_decided_before_verification(self):
        self.bootstrap_frozen_store()
        self.svc.report_fresh_loss("S-001", "B-1", "L-1", 10, "mgr-001")
        with self.assertRaises(ValidationError):
            self.svc.decide_fresh_loss("S-001", "B-1", "L-1", "charged", "audit-001")

    # -------------------------------------------------------------- 幂等与暂停

    def test_same_receipt_replay_moves_nothing(self):
        self.bootstrap_frozen_store()
        self.svc.plan_successor("S-001", "S-002", ["orders"])
        self.svc.confirm_independent("S-001", "successor", "S-002:mgr")
        before = len(self.svc.store)
        first = self.svc.transfer_order("S-001", "O-2", "S-002", "orders", "tr-1")
        second = self.svc.transfer_order("S-001", "O-2", "S-002", "orders", "tr-1")
        self.assertTrue(first.changed)
        self.assertTrue(second.replayed)
        self.assertEqual(len(self.svc.store), before + 1)

    def test_replay_stays_safe_after_entitlement_consumed(self):
        """权益已被消耗、阶段已推进后，原回执重放仍返回既有结果而非报错。"""
        self.bootstrap_frozen_store()
        self.svc.plan_successor("S-001", "S-002", ["orders"])
        self.svc.confirm_independent("S-001", "successor", "S-002:mgr")
        self.svc.transfer_order("S-001", "O-2", "S-002", "orders", "tr-1")
        self.sched.advance_to(datetime(2026, 10, 21, tzinfo=TZ))
        replayed = self.svc.transfer_order("S-001", "O-2", "S-002", "orders", "tr-1")
        self.assertTrue(replayed.replayed)

    def test_same_receipt_different_successor_suspends_without_moving_assets(self):
        self.bootstrap_frozen_store()
        self.svc.plan_successor("S-001", "S-002", ["orders"])
        self.svc.confirm_independent("S-001", "successor", "S-002:mgr")
        self.svc.transfer_order("S-001", "O-2", "S-002", "orders", "tr-1")
        order = self.svc.projection.order("S-001", "O-2")
        self.assertEqual("transferred_out", order["status"])

        # 新订单、相同回执编号但承接方不同 -> 暂停，不转移
        self.svc.confirm_independent("S-001", "finance", "fin-001")
        with self.assertRaises(SuspendedError):
            self.svc.transfer_balance("S-001", "cust-1", "S-009", 10.0, "tr-1")
        # 余额分毫未动
        self.assertEqual(120.0, self.svc.projection.balance("S-001", "cust-1")["amount"])
        suspension = self.svc.projection.require_store("S-001")["suspensions"]["tr-1"]
        self.assertEqual("suspended", suspension["status"])

        # 暂停期间同回执的任何重试一律拒绝
        with self.assertRaises(SuspendedError):
            self.svc.transfer_balance("S-001", "cust-1", "S-009", 10.0, "tr-1")

        # 裁决 reissue 后，用正确承接方与新回执可成功
        self.svc.resolve_suspension("S-001", "tr-1", "reissue", "hq-001")
        result = self.svc.transfer_balance("S-001", "cust-1", "S-002", 10.0, "tr-1b")
        self.assertTrue(result.changed)

    def test_same_receipt_different_scope_suspends(self):
        self.bootstrap_frozen_store()
        self.svc.plan_successor("S-001", "S-002", ["orders", "cakes"])
        self.svc.confirm_independent("S-001", "successor", "S-002:mgr")
        self.svc.transfer_order("S-001", "O-2", "S-002", "orders", "tr-2")
        # 相同编号、不同范围 -> 暂停，不产生第二次转移
        with self.assertRaises(SuspendedError):
            self.svc.transfer_order("S-001", "O-2", "S-002", "cakes", "tr-2")
        order = self.svc.projection.order("S-001", "O-2")
        self.assertEqual(1, len([d for d in order["dispositions"] if d["kind"] == "transferred"]))

    # -------------------------------------------------------------- 权益并发互斥

    def test_refund_transfer_pickup_are_mutually_exclusive(self):
        self.bootstrap_frozen_store()
        self.svc.fulfill_order("S-001", "O-1", "f-1")
        for call in (
            lambda: self.svc.refund_order("S-001", "O-1", 80.0, "rf-x"),
            lambda: self.svc.transfer_order("S-001", "O-1", "S-002", "orders", "to-x"),
        ):
            with self.assertRaises(EntitlementAlreadyConsumed):
                call()

    def test_transfer_wins_then_pickup_and_refund_lose_same_entitlement(self):
        """并发竞争同一份权益：先到的换店成功，后到的提货、退款都被拒。"""
        self.bootstrap_frozen_store()
        self.svc.plan_successor("S-001", "S-002", ["orders"])
        self.svc.confirm_independent("S-001", "successor", "S-002:mgr")
        self.svc.confirm_independent("S-001", "finance", "fin-001")
        transferred = self.svc.transfer_order("S-001", "O-2", "S-002", "orders", "tr-1")
        self.assertTrue(transferred.changed)
        # 冻结窗口内提货被拒
        with self.assertRaises(EntitlementAlreadyConsumed):
            self.svc.fulfill_order("S-001", "O-2", "f-late")
        # 生效日后退款同样被拒（阶段已放行，但权益已消耗）
        self.sched.advance_to(datetime(2026, 10, 11, tzinfo=TZ))
        with self.assertRaises(EntitlementAlreadyConsumed):
            self.svc.refund_order("S-001", "O-2", 60.0, "rf-late")

    # -------------------------------------------------------------- 独立确认

    def test_finance_confirmation_must_be_independent_of_manager(self):
        self.bootstrap_frozen_store()
        with self.assertRaises(ResponsibilityViolation):
            self.svc.confirm_independent("S-001", "finance", "mgr-001")
        self.svc.confirm_independent("S-001", "finance", "fin-001")

    def test_successor_confirmation_requires_successor_identity(self):
        self.bootstrap_frozen_store()
        self.svc.plan_successor("S-001", "S-002", ["orders"])
        with self.assertRaises(ResponsibilityViolation):
            self.svc.confirm_independent("S-001", "successor", "S-003:mgr")
        self.svc.confirm_independent("S-001", "successor", "S-002:mgr")

    def test_assets_do_not_move_without_confirmation(self):
        self.bootstrap_frozen_store()
        self.svc.plan_successor("S-001", "S-002", ["orders"])
        with self.assertRaises(ConfirmationRequired):
            self.svc.transfer_order("S-001", "O-2", "S-002", "orders", "tr-1")
        self.sched.advance_to(datetime(2026, 10, 11, tzinfo=TZ))
        with self.assertRaises(ConfirmationRequired):
            self.svc.refund_order("S-001", "O-2", 60.0, "rf-1")

    # -------------------------------------------------------------- 履约期限

    def test_fulfill_before_effective_date_then_restore_after(self):
        self.bootstrap_frozen_store()
        # 生效日前：正常提货
        self.svc.fulfill_order("S-001", "O-1", "f-1")
        self.assertEqual("fulfilled", self.svc.projection.order("S-001", "O-1")["status"])
        # 生效日后：不能提货
        self.sched.advance_to(datetime(2026, 10, 11, tzinfo=TZ))
        with self.assertRaises(StageViolation):
            self.svc.fulfill_order("S-001", "O-2", "f-2")
        # 无法履行的团购券订单按来源恢复券权益
        self.svc.confirm_independent("S-001", "finance", "fin-001")
        result = self.svc.restore_order_entitlement("S-001", "O-2", "rs-1")
        self.assertTrue(result.changed)
        order = self.svc.projection.order("S-001", "O-2")
        self.assertEqual("restored", order["status"])
        self.assertEqual("coupon", order["dispositions"][-1]["restore_kind"])

    def test_stored_value_order_restored_to_balance_once(self):
        self.bootstrap_frozen_store()
        # O-1 已在下单时划出 80；生效日后无法履行 -> 退回 80
        self.sched.advance_to(datetime(2026, 10, 11, tzinfo=TZ))
        self.svc.confirm_independent("S-001", "finance", "fin-001")
        self.svc.restore_order_entitlement("S-001", "O-1", "rs-1")
        self.assertEqual(200.0, self.svc.projection.balance("S-001", "cust-1")["amount"])
        with self.assertRaises(EntitlementAlreadyConsumed):
            self.svc.restore_order_entitlement("S-001", "O-1", "rs-2")

    def test_refund_before_effective_date_rejected(self):
        self.bootstrap_frozen_store()
        self.svc.confirm_independent("S-001", "finance", "fin-001")
        with self.assertRaises(StageViolation):
            self.svc.refund_order("S-001", "O-2", 60.0, "rf-1")

    # -------------------------------------------------------------- 核销去重

    def test_coupon_redemption_not_double_counted_on_store_merge(self):
        self.bootstrap_frozen_store()
        self.svc.open_store_period("S-002", manager_id="mgr-002")
        first = self.svc.record_coupon_redemption("S-001", "rd-1", 60.0, "orders", "rd-r1")
        second = self.svc.record_coupon_redemption("S-002", "rd-1", 60.0, "orders", "rd-r2")
        self.assertTrue(first.changed)
        self.assertTrue(second.replayed)
        self.assertEqual(1, len(self.svc.projection.coupon_redemptions))
        with self.assertRaises(ValidationError):
            self.svc.record_coupon_redemption("S-002", "rd-1", 99.0, "orders", "rd-r3")

    # -------------------------------------------------------------- 关闭拦截

    def test_close_blocked_until_all_obligations_done(self):
        self.bootstrap_frozen_store()
        self.svc.plan_successor("S-001", "S-002", ["orders", "balances"])
        self.svc.confirm_independent("S-001", "successor", "S-002:mgr")
        self.svc.confirm_independent("S-001", "finance", "fin-001")
        self.svc.fulfill_order("S-001", "O-1", "f-1")
        self.svc.transfer_order("S-001", "O-2", "S-002", "orders", "tr-1")
        self.svc.report_fresh_loss("S-001", "B-1", "L-1", 3, "mgr-001")
        self.sched.advance_to(datetime(2026, 10, 21, tzinfo=TZ))
        blockers = self.svc.unfinished_obligations("S-001")
        self.assertIn("losses", blockers)
        self.assertIn("balances", blockers)
        self.assertIn("equipment", blockers)
        self.assertIn("staff", blockers)
        self.assertIn("suppliers", blockers)
        with self.assertRaises(SettlementBlocked):
            self.svc.close_handover("S-001", "close-1")

    def test_close_receipt_replay_does_not_change_anything(self):
        """完成全部义务后关闭；相同关闭回执重放不再产生事件或转移。"""
        self.bootstrap_frozen_store()
        svc = self.svc
        svc.plan_successor("S-001", "S-002", ["orders", "balances"])
        svc.confirm_independent("S-001", "successor", "S-002:mgr")
        svc.confirm_independent("S-001", "finance", "fin-001")
        svc.fulfill_order("S-001", "O-1", "f-1")
        svc.transfer_order("S-001", "O-2", "S-002", "orders", "tr-1")
        svc.report_fresh_loss("S-001", "B-1", "L-1", 3, "mgr-001")
        svc.verify_fresh_loss("S-001", "B-1", "L-1", 3, "mgr-001")
        svc.decide_fresh_loss("S-001", "B-1", "L-1", "charged", "audit-001")
        self.sched.advance_to(datetime(2026, 10, 16, tzinfo=TZ))
        svc.close_lease("S-001", "EQ-1", "cl-1")
        svc.confirm_staff_handover("S-001", "emp-1", "hr-001")
        svc.transfer_balance("S-001", "cust-1", "S-002", 120.0, "tb-1")
        self.sched.advance_to(datetime(2026, 10, 21, tzinfo=TZ))
        svc.settle_supplier("S-001", "sup-1", 800.0, "ss-1")
        before = len(svc.store)
        first = svc.close_handover("S-001", "close-1")
        second = svc.close_handover("S-001", "close-1")
        self.assertTrue(first.changed)
        self.assertTrue(second.replayed)
        self.assertEqual(before + 1, len(svc.store))
        self.assertEqual("closed", svc.projection.require_store("S-001")["stage"])


if __name__ == "__main__":
    unittest.main()
