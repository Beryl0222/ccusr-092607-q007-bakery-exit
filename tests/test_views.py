"""消费者/供应商/员工视图的权限隔离，以及总部追溯链。"""

import datetime as dt
import unittest

from _fixtures import build_case
from bakery_exit import (
    AuthorizationError,
    CustomerView,
    EmployeeView,
    HeadquartersView,
    ReadModel,
    SupplierView,
)
from test_case_lifecycle import settle_case


class ViewTests(unittest.TestCase):
    def setUp(self) -> None:
        self.svc, self.store, self.clock, self.case_id, _ = build_case()
        settle_case(self.svc, self.clock, self.case_id)
        self.model = ReadModel(self.store)

    def test_customer_sees_only_own_orders_and_balances(self) -> None:
        view_a = CustomerView(self.model, "cust-A")
        refs = {o["order_ref"] for o in view_a.orders()}
        self.assertEqual({"order-1", "order-2"}, refs)
        self.assertEqual(["ledger-A"], [b["ledger_ref"] for b in view_a.balances()])

        view_b = CustomerView(self.model, "cust-B")
        self.assertEqual({"order-3"}, {o["order_ref"] for o in view_b.orders()})
        self.assertEqual([], view_b.balances())

    def test_customer_sees_order_destinations(self) -> None:
        view_a = CustomerView(self.model, "cust-A")
        outcomes = {o["order_ref"]: o["destination"]["outcome"] for o in view_a.orders()}
        self.assertEqual("picked_up", outcomes["order-1"])
        self.assertEqual("picked_up", outcomes["order-2"])
        view_b = CustomerView(self.model, "cust-B")
        destination = view_b.orders()[0]["destination"]
        self.assertEqual("restored", destination["outcome"])
        self.assertEqual(158.0, destination["restoration"]["amount"])

    def test_customer_balance_shows_destination(self) -> None:
        # 另起一个换店案件观察余额去向
        svc, store, clock, case_id, _ = build_case("case-2")
        svc.propose_plan("plan-x", case_id, "BJ-009", [], "planner")
        svc.confirm_funds("plan-x", "finance-x")
        svc.confirm_successor("plan-x", "BJ-009", "succ-y")
        svc.transfer_balance("ledger-A", "BJ-009", 50.0, "mv", "plan-x")
        model = ReadModel(store)
        balance = CustomerView(model, "cust-A").balances()[0]
        self.assertEqual("BJ-009", balance["transferred_to"])
        self.assertEqual(50.0, balance["transferred"])
        self.assertEqual(82.0, balance["available"])  # 200 - 68 占用 - 50 转出

    def test_customer_voucher_listing_respects_ownership(self) -> None:
        vouchers = CustomerView(self.model, "cust-A").vouchers()
        self.assertEqual(["vouch-G"], [v["voucher_ref"] for v in vouchers])
        self.assertEqual([], CustomerView(self.model, "cust-Z").vouchers())

    def test_supplier_sees_only_own_ledger(self) -> None:
        view = SupplierView(self.model, "supplier-F")
        self.assertEqual(["batch-F1"], [c["batch_ref"] for c in view.consignments()])
        self.assertEqual({"set-1"}, {s["settlement_ref"] for s in view.settlements()})
        self.assertEqual([], SupplierView(self.model, "supplier-UNKNOWN").settlements())

    def test_employee_visibility_is_role_scoped(self) -> None:
        staff = EmployeeView(self.model, "e-1", "staff")
        self.assertEqual(["task-1"], [t["task_ref"] for t in staff.handovers(self.case_id)])
        outsider = EmployeeView(self.model, "e-2", "marketing")
        self.assertEqual([], outsider.handovers(self.case_id))
        with self.assertRaises(AuthorizationError):
            outsider.require_visible("task-1")

    def test_headquarters_case_overview(self) -> None:
        overview = HeadquartersView(self.model).case_overview(self.case_id)
        self.assertEqual("BJ-001", overview["origin_store"] if "origin_store" in overview else overview["store_code"])
        self.assertEqual(3, overview["counts"]["orders"])
        self.assertEqual(2, overview["counts"]["batches"])
        self.assertEqual(1, overview["counts"]["suppliers"])

    def test_headquarters_trace_chain(self) -> None:
        # 从结算单追到原门店、批次和责任人
        trace = HeadquartersView(self.model).trace("supplier_account", "supplier-F")
        self.assertEqual("BJ-001", trace["origin_store"])
        self.assertIn("batch-F1", [b["batch_ref"] for b in trace["batches"]])
        self.assertIn("supplier-contact", trace["responsible_persons"])
        self.assertIn("accountant", trace["responsible_persons"])

        # 从损耗批次追到申请店长与批准区经
        trace_batch = HeadquartersView(self.model).trace("inventory_position", "batch-C1")
        self.assertEqual("BJ-001", trace_batch["origin_store"])
        self.assertIn("manager-li", trace_batch["responsible_persons"])
        self.assertIn("district-wang", trace_batch["responsible_persons"])

    def test_trace_unknown_aggregate_is_rejected(self) -> None:
        with self.assertRaises(AuthorizationError):
            HeadquartersView(self.model).trace("customer_order", "nope")


if __name__ == "__main__":
    unittest.main()
