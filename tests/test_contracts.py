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
        sample_doc = json.loads((ROOT / "data/sample.json").read_text(encoding="utf-8"))
        cls.samples = sample_doc if isinstance(sample_doc, list) else [sample_doc]
        cls.sample = cls.samples[0]

    def test_sample_is_valid(self) -> None:
        for index, event in enumerate(self.samples):
            issues = validate_event(event, self.schema)
            self.assertEqual([], issues, f"样例事件 {index} 存在契约问题: {issues}")

    def test_sample_covers_full_lifecycle(self) -> None:
        types = {e["event_type"] for e in self.samples}
        for expected in [
            "EXIT_ANNOUNCED", "OBLIGATION_FROZEN", "PRODUCTION_HALTED",
            "ORDER_FULFILLED", "ENTITLEMENT_RESTORED",
            "LOSS_APPROVED", "CONSIGNMENT_RETURNED",
            "SETTLEMENT_CONFIRMED", "SUCCESSOR_CONFIRMED",
            "EQUIPMENT_REMOVED", "HANDOVER_CLOSED", "CASE_CLOSED",
        ]:
            self.assertIn(expected, types)

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
        self.assertIn(
            ("payload.cutoff_at", "required"),
            [(x.field, x.code) for x in validate_event(event, self.schema)],
        )

    def test_unknown_event_is_rejected(self) -> None:
        issues = validate_event(dict(self.sample, event_type="UNKNOWN"), self.schema)
        self.assertIn(("event_type", "unsupported_value"), [(x.field, x.code) for x in issues])


if __name__ == "__main__":
    unittest.main()
