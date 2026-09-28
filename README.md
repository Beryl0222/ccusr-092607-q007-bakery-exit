# 连锁烘焙退场义务清算台

连锁面包店收缩门店时，把门店经营期、商品与批次、中央工厂配送、现制损耗、订单履约、储值资金、团购券核销、租赁设备、员工交接、供应商结算与承接方案按时间保存为事件，并在停业决定发布时冻结债权债务快照、逐项清算的参考实现。

## 目录

- `contracts/domain.schema.json`：事件信封、事件类型、聚合类型与各事件载荷必填约定。
- `data/sample.json`：由服务实际跑出的完整退场事件序列（50 个事件），可直接校验。
- `src/bakery_exit/`
  - `contracts.py`：基础契约校验（不改写输入）。
  - `clock.py`：可控时钟，测试与调度器用同一时钟推进截止期限。
  - `store.py`：仅追加事件存储，聚合版本乐观并发、`command_id` 幂等指纹、JSONL 持久化与重放恢复。
  - `aggregates.py`：各聚合的事件折叠状态机。
  - `service.py`：清算服务（冻结、职责分离、权益互斥、核销防重、回执幂等/冲突暂停）。
  - `scheduler.py`：停产 → 取货 → 退款 → 租约 → 结算五阶段调度与进程恢复。
  - `views.py`：消费者/供应商/员工权限视图与总部追溯。
  - `cli.py`：命令行校验单个事件、事件数组或 JSONL。
- `tests/`：契约、案件生命周期、权益互斥、损耗与结算职责分离、调度恢复、视图权限共 54 个测试。
- `docs/domain.md`：领域对象、不变量与事件语义。

## 测试

```bash
python3 -m unittest discover -s tests
```

## 编译检查

```bash
python3 -m compileall -q src tests
```

## 样例校验

```bash
PYTHONPATH=src python3 -m bakery_exit.cli contracts/domain.schema.json data/sample.json
```

样例有效时输出 `valid`；发现问题时逐行给出字段、代码和中文说明，并返回非零状态。

## 最小用法

```python
from datetime import datetime, timedelta, timezone
from bakery_exit import ControllableClock, EventStore, ExitClearingService, Scheduler, ReadModel, CustomerView

clock = ControllableClock(datetime(2026, 9, 1, 9, 0, tzinfo=timezone(timedelta(hours=8))))
svc = ExitClearingService(EventStore("/tmp/exit.jsonl"), clock)

svc.announce_exit("case-1", "BJ-001", clock.now + timedelta(days=14), "hq-manager")
svc.freeze_obligations("case-1", snapshot, "hq-audit")
# ……提货、退款、损耗批准、供应商确认、承接方案双确认、调度推进……
svc.close_case("case-1", "receipt-1", scope, successor_ref="BJ-009", closed_by="hq-manager")
```

持久化的 JSONL 在新进程中用 `EventStore(path)` 重放、用 `Scheduler.restore(...)` 延续当前阶段；相同 `command_id`/回执编号重放不会再次转移资产或余额。
