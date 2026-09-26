# 领域约定

描述烘焙门店关闭时订单、储值、库存、设备和多方义务的清算事件。

## 聚合与事件

| 聚合 | 含义 |
| --- | --- |
| `store_period` | 门店经营期：开业、停业决定、债权债务冻结快照、最终关闭 |
| `inventory_position` | 商品与批次：中央工厂配送、现制损耗的报告/现场核实/审批三段 |
| `customer_entitlement` | 消费者权益：订单登记与履约、换店承接、退款、储值转移、按来源恢复 |
| `exit_plan` | 承接与清算方案：承接门店、独立确认、阶段推进、暂停 |
| `supplier_consignment` | 供应商寄售原料台账与结算 |
| `handover_task` | 租赁设备与员工交接事项 |

事件类型：

- 经营期：`STORE_PERIOD_OPENED`、`EXIT_ANNOUNCED`、`OBLIGATION_FROZEN`、`HANDOVER_CLOSED`
- 批次与损耗：`BATCH_DELIVERED`、`FRESH_LOSS_REPORTED`、`FRESH_LOSS_VERIFIED`、`FRESH_LOSS_DECIDED`
- 订单与权益：`ORDER_REGISTERED`、`ORDER_FULFILLED`、`ORDER_TRANSFERRED`、`ENTITLEMENT_SETTLED`、`ENTITLEMENT_RESTORED`、`REFUND_ISSUED`、`BALANCE_TRANSFERRED`、`STORED_BALANCE_POSTED`
- 承接方案：`SUCCESSOR_PLANNED`、`INDEPENDENT_CONFIRMATION`、`CLEARING_SUSPENDED`、`CLEARING_RESUMED`、`STAGE_ADVANCED`
- 交接结算：`LEASE_EQUIPMENT_RECORDED`、`LEASE_CLOSED`、`STAFF_HANDOVER_RECORDED`、`STAFF_HANDOVER_CONFIRMED`、`SUPPLIER_CONSIGNMENT_RECORDED`、`SUPPLIER_SETTLED`

`aggregate_by_event` 固定了每个事件归属的聚合类型；事件与聚合不配对时基础校验直接拒绝。

## 时间与版本

所有发生时间都必须携带时区，版本号从 1 开始在同一聚合内严格递增，基础校验不会改写调用方输入。

## 关键载荷

- `OBLIGATION_FROZEN`：`cutoff_at`, `snapshot_hash`。停业决定发布时对债权债务冻结快照，后续处置全部基于该快照可追溯。
- `ORDER_TRANSFERRED`：`successor_ref`, `order_scope`。
- `ENTITLEMENT_SETTLED`：`source_ref`, `amount`。
- `FRESH_LOSS_*`：报告人（店长）、现场核实数量与核实人、审批决定与审批人分属三个事件，店长不能批准自己的损耗减免。
- `INDEPENDENT_CONFIRMATION`：`party` 标识确认方（`finance` 资金清算 / `successor` 承接门店），与店长处现场职责相互独立。
- `CLEARING_SUSPENDED`：回执编号相同但日期、范围或承接方与既有事实冲突时挂起，不转移任何资产或余额。
- `ENTITLEMENT_RESTORED`：无法履行的订单按 `source_kind`（储值/团购券/活动赠券/普通支付）恢复对应权益。

## 上层服务语义（基础契约不负责）

- 相同 `event_id`/业务回执编号重放必须幂等：返回已有结果，不再转移资产或余额。
- 编号相同而日期、范围或承接方不同 → 暂停（`CLEARING_SUSPENDED`）等待人工裁决。
- 退款、换店、提货互斥并发：同一份权益只能被其中一种处置消耗一次。
- 历史核销不因子订单被合并到承接门店而重复计算（按核销来源幂等去重）。
- 调度器以可控时钟推进停产、取货、退款、租约、结算期限；阶段状态由事件流重建，进程恢复后延续原阶段。
- 视图按角色过滤：消费者只见本人订单与余额去向，供应商/员工只见各自有权查看的交接事项，总部可从任一清算结果追到原门店、商品批次、责任人和未完成义务。
