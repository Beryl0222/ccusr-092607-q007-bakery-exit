"""角色视图与总部溯源测试。"""

import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bakery_exit import ControllableClock, ExitClearingService  # noqa: E402
from bakery_exit.scheduler import StageScheduler  # noqa: E402
from bakery_exit.store import EventStore  # noqa: E402
from bakery_exit.views import Views  # noqa: E402

TZ = timezone(timedelta(hours=8))


class ViewTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        schema = json.loads((ROOT / "contracts/domain.schema.json").read_text("utf-8"))
        store = EventStore(str(Path(self.tmp.name) / "events.jsonl"))
        clock = ControllableClock(datetime(2026, 9, 1, 9, 0, tzinfo=TZ))
        self.svc = ExitClearingService(store, clock, schema)
        self.sched = StageScheduler(self.svc)
        self.views = Views(self.svc.projection)
        self._populate()

    def _populate(self) -> None:
        svc = self.svc
        svc.open_store_period("S-001", manager_id="mgr-001")
        svc.deliver_batch("S-001", "B-1", "吐司", 100, receipt_no="d1")
        svc.record_consignment("S-001", "sup-A", "B-9", 20, 300.0, "c1")
        svc.top_up_balance("S-001", "cust-1", 200.0, "t1")
        svc.register_order("S-001", "O-1", "stored_value", "sv-1", 80.0, "cust-1", "o1")
        svc.register_order("S-001", "O-2", "group_coupon", "gc-1", 60.0, "cust-1", "o2")
        svc.record_leased_equipment("S-001", "EQ-1", "lessor-A", "e1")
        svc.announce_exit("S-001", datetime(2026, 10, 10, 22, tzinfo=TZ).isoformat())
        svc.record_staff_handover("S-001", "emp-1", ["钥匙"], "h1")
        svc.freeze_obligations(
            "S-001",
            cutoff_at=datetime(2026, 9, 25, 10, tzinfo=TZ).isoformat(),
            production_stop_at=datetime(2026, 9, 30, 22, tzinfo=TZ).isoformat(),
            pickup_refund_deadline=datetime(2026, 10, 10, 22, tzinfo=TZ).isoformat(),
            lease_return_deadline=datetime(2026, 10, 15, 22, tzinfo=TZ).isoformat(),
            settlement_deadline=datetime(2026, 10, 20, 22, tzinfo=TZ).isoformat(),
        )
        svc.report_fresh_loss("S-001", "B-1", "L-1", 5, "mgr-001")
        svc.verify_fresh_loss("S-001", "B-1", "L-1", 5, "mgr-001")
        svc.decide_fresh_loss("S-001", "B-1", "L-1", "waived", "audit-001")
        svc.fulfill_order("S-001", "O-1", "f1")
        svc.plan_successor("S-001", "S-002", ["orders", "balances"])
        svc.confirm_independent("S-001", "successor", "S-002:mgr")
        svc.confirm_independent("S-001", "finance", "fin-001")
        svc.transfer_order("S-001", "O-2", "S-002", "orders", "tr-1")
        svc.transfer_balance("S-001", "cust-1", "S-002", 120.0, "tb-1")
        self.sched.advance_to(datetime(2026, 10, 21, tzinfo=TZ))
        svc.close_lease("S-001", "EQ-1", "cl-1")
        svc.confirm_staff_handover("S-001", "emp-1", "hr-001")
        svc.settle_supplier("S-001", "sup-A", 300.0, "ss-1")
        svc.close_handover("S-001", "close-1")

    def test_customer_sees_only_own_orders_and_balance_movements(self):
        view = self.views.customer_view("cust-1")
        order_nos = {o["order_no"] for o in view["orders"]}
        self.assertEqual({"O-1", "O-2"}, order_nos)
        statuses = {o["order_no"]: o["status"] for o in view["orders"]}
        self.assertEqual("fulfilled", statuses["O-1"])
        self.assertEqual("transferred_out", statuses["O-2"])
        balance = view["balances"][0]
        self.assertEqual(0.0, balance["remaining"])
        kinds = [m["kind"] for m in balance["movements"]]
        self.assertIn("top_up", kinds)
        self.assertIn("order_payment", kinds)
        self.assertIn("transfer_out", kinds)

        # 别的消费者什么都看不到
        self.assertEqual([], self.views.customer_view("cust-other")["orders"])

    def test_order_whereabouts_tells_customer_where_it_went(self):
        view = self.views.customer_view("cust-1")
        o2 = next(o for o in view["orders"] if o["order_no"] == "O-2")
        self.assertEqual("moved_store", o2["whereabouts"][0]["kind"])
        self.assertEqual("S-002", o2["whereabouts"][0]["successor_ref"])

    def test_supplier_sees_only_own_ledger(self):
        view = self.views.supplier_view("sup-A")
        self.assertEqual(1, len(view["ledgers"]))
        ledger = view["ledgers"][0]
        self.assertEqual(0.0, ledger["outstanding"])
        self.assertTrue(ledger["settled"])
        self.assertEqual([], self.views.supplier_view("sup-B")["ledgers"])

    def test_staff_sees_only_own_handover(self):
        view = self.views.staff_view("emp-1")
        self.assertEqual(1, len(view["handovers"]))
        self.assertTrue(view["handovers"][0]["confirmed"])
        self.assertEqual([], self.views.staff_view("emp-2")["handovers"])

    def test_headquarters_can_trace_receipt_to_store_batch_and_people(self):
        hq = self.views.headquarters_store_view("S-001")
        self.assertEqual("closed", hq["stage"])
        self.assertEqual("S-002", hq["successor"]["successor_ref"])
        batch = next(b for b in hq["batches"] if b["batch_id"] == "B-1")
        loss = batch["losses"][0]
        self.assertEqual("mgr-001", loss["reporter_id"])
        self.assertEqual("mgr-001", loss["verifier_id"])
        self.assertEqual("audit-001", loss["approver_id"])

        trace = self.views.trace_receipt("tr-1")
        self.assertEqual(["S-001"], trace["store_ids"])
        self.assertEqual("ORDER_TRANSFERRED", trace["chain"][0]["event_type"])
        self.assertEqual("S-002", trace["chain"][0]["successor_ref"])

    def test_trace_unknown_receipt_raises(self):
        with self.assertRaises(KeyError):
            self.views.trace_receipt("nope")


if __name__ == "__main__":
    unittest.main()
