"""测试夹具：构造一个已发布停业并冻结快照的案件。"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bakery_exit import ControllableClock, ExitClearingService, EventStore  # noqa: E402

TZ_OFFSET = timedelta(hours=8)


def build_case(case_id: str = "case-1", store=None, clock=None):
    """返回 (service, store, clock, case_id, effective_at)，案件已冻结、已停产。"""
    import datetime as _dt

    clock = clock or ControllableClock(_dt.datetime(2026, 9, 1, 9, 0, tzinfo=_dt.timezone(TZ_OFFSET)))
    store = store or EventStore()
    svc = ExitClearingService(store, clock)
    effective_at = clock.now + timedelta(days=14)

    svc.open_balance_ledger("ledger-A", case_id, "cust-A", 200.0)
    svc.hold_for_order("ledger-A", "hold-1", "order-1", 68.0)
    svc.register_voucher("vouch-G", case_id, "GB-001", "groupbuy", 88.0)
    svc.open_supplier_ledger("supplier-F", case_id, "面粉厂")
    svc.register_consignment("supplier-F", "batch-F1", case_id, "高筋粉", 100, 4.5)
    svc.deliver_batch("batch-C1", case_id, "吐司", 50, "central_factory")
    svc.register_lease("equip-O1", case_id, "烤箱租赁公司", clock.now + timedelta(days=30))
    svc.register_order(
        "order-1", case_id, "cust-A", "stored_value", clock.now + timedelta(days=3),
        [{"sku": "蛋糕", "qty": 1}],
        payment={"method": "stored_value", "ledger_id": "ledger-A", "amount": 68.0, "hold_ref": "hold-1"},
    )
    svc.register_order(
        "order-2", case_id, "cust-A", "groupbuy", clock.now + timedelta(days=4),
        [{"sku": "面包券", "qty": 1}],
        payment={"method": "groupbuy", "voucher_code": "vouch-G"},
    )
    svc.register_order(
        "order-3", case_id, "cust-B", "prepaid", clock.now + timedelta(days=20),
        [{"sku": "定制蛋糕", "qty": 1}],
        payment={"method": "prepaid", "amount": 158.0},
    )

    svc.announce_exit(case_id, "BJ-001", effective_at, "hq-manager")
    snapshot = {
        "ledgers": ["ledger-A"],
        "vouchers": ["vouch-G"],
        "batches": ["batch-F1", "batch-C1"],
        "suppliers": ["supplier-F"],
        "leases": ["equip-O1"],
        "orders": ["order-1", "order-2", "order-3"],
    }
    svc.freeze_obligations(case_id, snapshot, "hq-audit")
    svc.halt_production(case_id, "ops")
    return svc, store, clock, case_id, effective_at
