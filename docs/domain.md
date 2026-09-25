# 领域约定

描述烘焙门店关闭时订单、储值、库存、设备和多方义务的清算事件。

聚合对象包括`store_period`、`customer_entitlement`、`inventory_position`、`exit_plan`。事件类型包括`EXIT_ANNOUNCED`、`OBLIGATION_FROZEN`、`ORDER_TRANSFERRED`、`ENTITLEMENT_SETTLED`、`HANDOVER_CLOSED`。所有发生时间都必须携带时区，版本号从 1 开始递增，基础校验不会改写调用方输入。

## 事件载荷

- `OBLIGATION_FROZEN`：载荷还需包含 `cutoff_at`, `snapshot_hash`。
- `ORDER_TRANSFERRED`：载荷还需包含 `successor_ref`, `order_scope`。
- `ENTITLEMENT_SETTLED`：载荷还需包含 `source_ref`, `amount`。

相同事件标识的业务幂等、冲突隔离和状态推进由上层服务负责；本仓库只定义可稳定交换的基础事实。
