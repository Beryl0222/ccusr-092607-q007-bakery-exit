# 领域约定

连锁烘焙门店关闭时，订单、储值、未取蛋糕、团购券、中央工厂配送、现制损耗、租赁设备、员工交接、供应商结算与承接方案统一按时间以事件保存。本仓库定义可稳定交换的事件契约（`contracts/domain.schema.json`），并在 `src/bakery_exit/` 提供事件溯源的参考实现。

## 聚合

| aggregate_type | 含义 | 关键状态 |
| --- | --- | --- |
| `store_period` | 一次门店退场清算案件 | announced → frozen →（suspended → resumed）→ closed |
| `customer_order` | 单笔订单（含支付来源） | registered → fulfilled / unfulfillable / transferred |
| `customer_entitlement` | 消费者储值账户 | 总额、占用、消耗、退款、转出 |
| `voucher` | 团购券/代金券/赠品券 | registered → redeemed / restored / transferred |
| `inventory_position` | 商品批次（中央工厂/现制/寄售） | 入库、现场核实、损耗批准、转出、退回 |
| `supplier_account` | 供应商往来与结算 | proposed → confirmed → paid |
| `equipment_lease` | 租赁设备 | registered → terminated → removed（押金结算） |
| `handover_task` | 员工/供应商交接事项 | assigned → acknowledged → closed |
| `exit_plan` | 承接方案 | proposed，资金与承接门店分别独立确认后 completed |
| `scheduler` | 阶段调度（停产/取货/退款/租约/结算） | 每阶段排期与推进时刻 |

所有发生时间必须携带时区，`version` 从 1 开始递增；事件仅追加，基础校验不改写调用方输入。

## 核心不变量

1. **冻结快照**：停业发布（`EXIT_ANNOUNCED`）后冻结债权债务快照（`OBLIGATION_FROZEN`，含 `cutoff_at` 与 `snapshot_hash`）。冻结后不得新增订单、账户、批次、设备、供应商清册；清算动作只能引用快照清册内的实体。
2. **职责分离**：店长可核实现场数量（`ON_SITE_COUNTED`）并发起损耗（`LOSS_REQUESTED`），但批准人不得是申请人本人；供应商结算的确认人不得是发起人；承接方案的资金确认与承接门店确认必须是两名不同责任人。
3. **生效日规则**：生效日前仍可履行的订单正常提货（`ORDER_FULFILLED`）；约定取货日晚于生效日或已过生效日的订单不得提货。无法履行的订单按支付来源恢复权益（`ENTITLEMENT_RESTORED`）：储值退回、券恢复、预付登记现金退还。
4. **权益互斥**：同一储值的提货消耗（`BALANCE_CONSUMED`）、退款（`BALANCE_REFUNDED`）、换店转出（`BALANCE_TRANSFERRED`）共享同一份可用余额，下单占用（`BALANCE_HELD`）在履约时核销、在无法履行时释放。并发的退款、换店、提货不能把余额消耗成负数。
5. **核销全局唯一**：券核销以 `redemption_ref` 为全局凭据，跨门店扫描事件日志；已转店的券不得在原店核销，已核销券不得转店——历史核销不因门店合并被重复计算。
6. **批次余量唯一**：损耗核销、转出承接店、寄售退回共享同一批次余量，三种处置之和不得超过入库量；只有 `origin=consignment` 的批次可退回供应商。
7. **关闭回执**：案件关闭（`CASE_CLOSED`）前必须没有未完成义务。相同回执编号重放只返回首次事件，不再转移任何资产或余额；编号相同而日期、范围或承接方不同，指纹冲突，案件暂停（`CASE_SUSPENDED`），不产生任何资产业务事件，需人工恢复（`CASE_RESUMED`）。
8. **命令幂等**：所有写命令可携带 `command_id`；相同编号的重复提交先于状态校验返回首次结果，保证至少一次投递下的幂等。聚合版本采用乐观并发控制，过期版本提交被拒绝。
9. **可控时钟与恢复**：调度阶段固定为 `production_halt → pickup → refund → lease → settlement`，每阶段有截止时刻，只能顺序推进；进程恢复时从事件日志重建，`Scheduler.restore` 记录 `RECOVERY_RESUMED` 并延续原阶段。

## 视图与追溯

- `CustomerView`：消费者只见自己的订单去向（已取/已转店/已恢复）、储值余额去向与本人的券。
- `SupplierView`：供应商只见自己的寄售入库、退回数量与结算单。
- `EmployeeView`：员工按角色经交接事项 `visibility` 授权查看，无权事项不可见。
- `HeadquartersView`：案件总览，以及从任一清算结果（订单、结算单、批次……）沿 `case_ref / batch_ref / plan_ref` 追溯到原门店、商品批次、各环节责任人与未完成义务。

## 事件载荷必填

各事件必填字段见 `contracts/domain.schema.json` 的 `payload_required_by_event`；例如 `OBLIGATION_FROZEN` 需 `cutoff_at`、`snapshot_hash`，`ORDER_TRANSFERRED` 需 `successor_ref`、`order_scope`，`ENTITLEMENT_SETTLED` 需 `source_ref`、`amount`。
