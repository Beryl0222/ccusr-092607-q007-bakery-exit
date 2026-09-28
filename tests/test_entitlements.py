"""订单履约、权益恢复、储值互斥消耗、券核销全局防重。"""

import datetime as dt
import unittest

from _fixtures import build_case
from bakery_exit import (
    DuplicateRedemptionError,
    EntitlementExhaustedError,
    PhaseError,
    aggregates as agg,
)


class EntitlementTests(unittest.TestCase):
    # ---------------------------------------------------------- 提货

    def test_pickup_before_effective_consumes_balance_once(self) -> None:
        svc, store, clock, case_id, _ = build_case()
        svc.pickup_order("order-1", "cust-A")
        ledger = svc._state("customer_entitlement", "ledger-A")
        self.assertEqual(68.0, ledger["consumed"])
        self.assertEqual(132.0, agg.balance_available(ledger))
        order = svc._state("customer_order", "order-1")
        self.assertEqual("fulfilled", order["status"])

        # 终态订单不能再提货或换店
        with self.assertRaises(PhaseError):
            svc.pickup_order("order-1", "cust-A")
        with self.assertRaises(PhaseError):
            svc.mark_unfulfillable("order-1", "x", "ops")

    def test_pickup_after_effective_is_rejected(self) -> None:
        svc, store, clock, case_id, _ = build_case()
        clock.advance(days=20)
        with self.assertRaises(PhaseError):
            svc.pickup_order("order-1", "cust-A")

    def test_order_scheduled_after_effective_cannot_pickup(self) -> None:
        # order-3 约定取货日在生效日之后
        svc, *_ = build_case()
        with self.assertRaises(PhaseError):
            svc.pickup_order("order-3", "cust-B")

    def test_voucher_pickup_records_redemption(self) -> None:
        svc, *_ = build_case()
        svc.pickup_order("order-2", "cust-A")
        voucher = svc._state("voucher", "vouch-G")
        self.assertEqual("redeemed", voucher["status"])
        self.assertIn("order-2", voucher["redemptions"])

    # ---------------------------------------------------------- 权益恢复

    def test_unfulfillable_restores_by_source(self) -> None:
        svc, *_ = build_case()
        # 团购券订单无法履行 -> 券恢复
        svc.mark_unfulfillable("order-2", "门店提前闭店", "ops")
        voucher = svc._state("voucher", "vouch-G")
        self.assertEqual("restored", voucher["status"])
        order = svc._state("customer_order", "order-2")
        self.assertEqual("unfulfillable", order["status"])
        self.assertEqual("voucher", order["restorations"][0]["type"])

        # 预付订单无法履行 -> 登记现金退还权益
        svc.mark_unfulfillable("order-3", "定制蛋糕无法制做", "ops")
        order3 = svc._state("customer_order", "order-3")
        self.assertEqual(158.0, order3["restorations"][0]["amount"])

    def test_unfulfillable_stored_value_refunds_and_releases_hold(self) -> None:
        svc, *_ = build_case()
        svc.mark_unfulfillable("order-1", "缺货", "ops")
        ledger = svc._state("customer_entitlement", "ledger-A")
        self.assertEqual(68.0, ledger["refunded"])
        self.assertTrue(ledger["holds"]["hold-1"]["released"])
        # 占用释放后，其余 132 与退款口径互不占用：可用仍为 132
        self.assertEqual(132.0, agg.balance_available(ledger))

    # ---------------------------------------------------------- 并发互斥

    def test_refund_then_second_refund_is_rejected(self) -> None:
        svc, *_ = build_case()
        svc.refund_balance("ledger-A", 132.0, "refund-1", "finance")
        with self.assertRaises(EntitlementExhaustedError):
            svc.refund_balance("ledger-A", 1.0, "refund-2", "finance")

    def test_refund_blocks_transfer_of_held_funds(self) -> None:
        svc, *_ = build_case()
        # 未占用的 132 先退款；被订单占用的 68 不能再被换店拿走
        svc.refund_balance("ledger-A", 132.0, "refund-1", "finance")
        self._confirm_plan(svc)
        with self.assertRaises(EntitlementExhaustedError):
            svc.transfer_balance("ledger-A", "BJ-009", 68.0, "order-1", "plan-1")

    def test_transfer_then_refund_is_rejected(self) -> None:
        svc, *_ = build_case()
        self._confirm_plan(svc)
        svc.transfer_balance("ledger-A", "BJ-009", 100.0, "move-1", "plan-1")
        with self.assertRaises(EntitlementExhaustedError):
            svc.refund_balance("ledger-A", 40.0, "refund-2", "finance")

    def test_pickup_then_refund_cannot_touch_consumed_amount(self) -> None:
        svc, *_ = build_case()
        svc.pickup_order("order-1", "cust-A")
        with self.assertRaises(EntitlementExhaustedError):
            svc.refund_balance("ledger-A", 140.0, "refund-x", "finance")
        # 未消耗的 132 仍可退
        svc.refund_balance("ledger-A", 132.0, "refund-ok", "finance")

    def test_hold_overdraft_is_rejected(self) -> None:
        svc, *_ = build_case()
        with self.assertRaises(EntitlementExhaustedError):
            svc.hold_for_order("ledger-A", "hold-2", "order-x", 133.0)

    def test_free_balance_refunded_order_restore_uses_released_hold(self) -> None:
        svc, *_ = build_case()
        # 未占用的 132 先退走；订单占用的 68 仍可经“无法履行”释放后退
        svc.refund_balance("ledger-A", 132.0, "refund-free", "finance")
        svc.mark_unfulfillable("order-1", "缺货", "ops")
        ledger = svc._state("customer_entitlement", "ledger-A")
        self.assertEqual(200.0, ledger["refunded"])
        self.assertEqual(0.0, agg.balance_available(ledger))

    # ---------------------------------------------------------- 券核销防重

    def test_redemption_ref_is_globally_unique(self) -> None:
        svc, store, clock, case_id, _ = build_case()
        svc.redeem_voucher("vouch-G", "redeem-001", "BJ-001")
        # 同一凭据再次核销（哪怕模拟门店合并后的另一本台账）一律拒绝
        with self.assertRaises(DuplicateRedemptionError):
            svc.redeem_voucher("vouch-G", "redeem-001", "BJ-009")

    def test_transferred_voucher_cannot_be_redeemed_at_origin(self) -> None:
        svc, *_ = build_case()
        self._confirm_plan(svc, scope=["order-2"])
        svc.transfer_order("order-2", "plan-1")
        with self.assertRaises(PhaseError):
            svc.redeem_voucher("vouch-G", "redeem-002", "BJ-001")

    def test_redeemed_voucher_cannot_transfer(self) -> None:
        svc, *_ = build_case()
        svc.pickup_order("order-2", "cust-A")
        self._confirm_plan(svc)
        with self.assertRaises(PhaseError):
            svc.transfer_voucher("vouch-G", "plan-1")

    # ---------------------------------------------------------- 换店承接

    def test_transfer_order_requires_scope(self) -> None:
        svc, *_ = build_case()
        self._confirm_plan(svc, scope=["order-3"])
        with self.assertRaises(PhaseError):
            svc.transfer_order("order-2", "plan-1")
        svc.transfer_order("order-3", "plan-1")
        order = svc._state("customer_order", "order-3")
        self.assertEqual("transferred", order["status"])
        self.assertEqual("BJ-009", order["successor_ref"])

    def test_transfer_and_restore_are_mutually_exclusive(self) -> None:
        svc, *_ = build_case()
        self._confirm_plan(svc, scope=["order-3"])
        svc.transfer_order("order-3", "plan-1")
        with self.assertRaises(PhaseError):
            svc.mark_unfulfillable("order-3", "x", "ops")

    # ---------------------------------------------------------- 辅助

    def _confirm_plan(self, svc, scope=None) -> None:
        svc.propose_plan("plan-1", "case-1", "BJ-009", scope or [], "planner")
        svc.confirm_funds("plan-1", "finance")
        svc.confirm_successor("plan-1", "BJ-009", "succ-mgr")


if __name__ == "__main__":
    unittest.main()
