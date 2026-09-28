"""现制损耗职责分离、寄售退回、供应商独立结算、租赁设备与员工交接。"""

import datetime as dt
import unittest

from _fixtures import build_case
from bakery_exit import AuthorizationError, EntitlementExhaustedError, PhaseError


class InventoryLossTests(unittest.TestCase):
    def test_manager_counts_but_cannot_approve_own_loss(self) -> None:
        svc, *_ = build_case()
        svc.count_on_site("batch-C1", "count-1", "manager-li", 47)
        svc.request_loss("batch-C1", "loss-1", "manager-li", 3, "当日现制报损")
        with self.assertRaises(AuthorizationError):
            svc.approve_loss("batch-C1", "loss-1", "manager-li")
        svc.approve_loss("batch-C1", "loss-1", "district-wang")
        batch = svc._state("inventory_position", "batch-C1")
        self.assertEqual(3, batch["written_off"])
        self.assertEqual("approved", batch["loss_requests"]["loss-1"]["status"])

    def test_counting_alone_does_not_write_off(self) -> None:
        svc, *_ = build_case()
        svc.count_on_site("batch-C1", "count-1", "manager-li", 47)
        batch = svc._state("inventory_position", "batch-C1")
        self.assertEqual(0, batch["written_off"])
        self.assertEqual(47, batch["last_count_quantity"])

    def test_approved_loss_cannot_be_processed_twice(self) -> None:
        svc, *_ = build_case()
        svc.request_loss("batch-C1", "loss-1", "manager-li", 3, "报损")
        svc.approve_loss("batch-C1", "loss-1", "district-wang")
        with self.assertRaises(PhaseError):
            svc.approve_loss("batch-C1", "loss-1", "district-wang")

    def test_loss_transfer_return_share_same_remaining(self) -> None:
        svc, *_ = build_case()
        self._confirm_plan(svc)
        svc.request_loss("batch-C1", "loss-1", "manager-li", 48, "报损")
        svc.approve_loss("batch-C1", "loss-1", "district-wang")
        # 仅剩 2，再转出 3 必须被拒
        with self.assertRaises(EntitlementExhaustedError):
            svc.transfer_batch("batch-C1", 3, "plan-1")
        svc.transfer_batch("batch-C1", 2, "plan-1")

    def test_only_consignment_can_be_returned(self) -> None:
        svc, *_ = build_case()
        with self.assertRaises(PhaseError):
            svc.return_consignment("batch-C1", "supplier-F", 5)
        svc.return_consignment("batch-F1", "supplier-F", 20)
        batch = svc._state("inventory_position", "batch-F1")
        self.assertEqual(20, batch["returned"])

    def _confirm_plan(self, svc) -> None:
        svc.propose_plan("plan-1", "case-1", "BJ-009", [], "planner")
        svc.confirm_funds("plan-1", "finance")
        svc.confirm_successor("plan-1", "BJ-009", "succ-mgr")


class SupplierSettlementTests(unittest.TestCase):
    def test_settlement_needs_independent_confirmation(self) -> None:
        svc, *_ = build_case()
        svc.propose_settlement("supplier-F", "set-1", 360.0, "accountant")
        # 发起人不能自己确认
        with self.assertRaises(AuthorizationError):
            svc.confirm_settlement("supplier-F", "set-1", "accountant")
        # 未确认不能支付
        with self.assertRaises(PhaseError):
            svc.pay_settlement("supplier-F", "set-1")
        svc.confirm_settlement("supplier-F", "set-1", "supplier-contact")
        svc.pay_settlement("supplier-F", "set-1")
        supplier = svc._state("supplier_account", "supplier-F")
        self.assertEqual("paid", supplier["settlements"]["set-1"]["status"])

    def test_settlement_ref_is_idempotent(self) -> None:
        svc, *_ = build_case()
        svc.propose_settlement("supplier-F", "set-1", 360.0, "accountant")
        with self.assertRaises(PhaseError):
            svc.propose_settlement("supplier-F", "set-1", 360.0, "accountant")


class LeaseAndHandoverTests(unittest.TestCase):
    def test_lease_must_terminate_before_removal(self) -> None:
        svc, *_ = build_case()
        with self.assertRaises(PhaseError):
            svc.remove_equipment("equip-O1", "manager-li")
        svc.terminate_lease("equip-O1", "ops")
        with self.assertRaises(PhaseError):
            svc.settle_deposit("equip-O1", 1000.0)
        svc.remove_equipment("equip-O1", "manager-li")
        svc.settle_deposit("equip-O1", 1000.0)
        lease = svc._state("equipment_lease", "equip-O1")
        self.assertEqual(1000.0, lease["deposit_amount"])

    def test_handover_lifecycle(self) -> None:
        svc, store, clock, case_id, _ = build_case()
        svc.assign_handover(
            "task-1", case_id, "staff", "排班交接", "leader", ["staff"],
            clock.now + dt.timedelta(days=5),
        )
        svc.acknowledge_handover("task-1", "employee-chen")
        with self.assertRaises(PhaseError):
            svc.acknowledge_handover("task-1", "employee-chen")
        svc.close_handover("task-1", "employee-chen")
        self.assertEqual("closed", svc._state("handover_task", "task-1")["status"])


if __name__ == "__main__":
    unittest.main()
