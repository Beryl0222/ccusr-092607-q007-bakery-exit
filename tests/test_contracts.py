import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bakery_exit.contracts import validate_event


class ContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.schema = json.loads((ROOT / "contracts/domain.schema.json").read_text(encoding="utf-8"))
        cls.sample = json.loads((ROOT / "data/sample.json").read_text(encoding="utf-8"))

    def test_sample_is_valid(self) -> None:
        self.assertEqual([], validate_event(self.sample, self.schema))

    def test_missing_fields_are_stable(self) -> None:
        issues = validate_event({}, self.schema)
        self.assertEqual(sorted(x.field for x in issues), [x.field for x in issues])

    def test_time_and_version_boundaries(self) -> None:
        event = dict(self.sample, occurred_at="2026-09-25T10:00:00", version=0)
        codes = {(x.field, x.code) for x in validate_event(event, self.schema)}
        self.assertIn(("occurred_at", "timezone_required"), codes)
        self.assertIn(("version", "positive_integer"), codes)

    def test_event_payload_is_required(self) -> None:
        event = dict(self.sample, event_type="OBLIGATION_FROZEN", payload={})
        self.assertIn(("payload.cutoff_at", "required"), [(x.field, x.code) for x in validate_event(event, self.schema)])

    def test_unknown_event_is_rejected(self) -> None:
        issues = validate_event(dict(self.sample, event_type="UNKNOWN"), self.schema)
        self.assertIn(("event_type", "unsupported_value"), [(x.field, x.code) for x in issues])

    def test_event_aggregate_pairing_is_enforced(self) -> None:
        event = dict(self.sample, event_type="BATCH_DELIVERED", aggregate_type="store_period",
                     payload={"store_id": "S-001", "batch_id": "B-1", "sku": "吐司",
                              "quantity": 1, "source_kind": "central_factory"})
        issues = validate_event(event, self.schema)
        self.assertIn(("aggregate_type", "aggregate_mismatch"),
                      [(x.field, x.code) for x in issues])

    def test_new_service_events_keep_contract(self) -> None:
        base = dict(self.sample)
        for event_type, aggregate_type, payload in (
            ("FRESH_LOSS_REPORTED", "inventory_position",
             {"store_id": "S-001", "batch_id": "B-1", "loss_id": "L-1",
              "quantity": 3, "reporter_id": "m-1"}),
            ("CLEARING_SUSPENDED", "exit_plan",
             {"store_id": "S-001", "receipt_no": "r-1", "reason": "冲突"}),
            ("SUPPLIER_SETTLED", "supplier_consignment",
             {"store_id": "S-001", "supplier_ref": "sup-1", "amount": 100,
              "receipt_no": "r-2"}),
        ):
            event = dict(base, event_type=event_type, aggregate_type=aggregate_type, payload=payload)
            self.assertEqual([], validate_event(event, self.schema), event_type)


if __name__ == "__main__":
    unittest.main()
