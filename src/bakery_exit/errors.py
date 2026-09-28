"""领域错误：命令被拒绝时抛出，不产生事件。"""

from __future__ import annotations


class DomainError(Exception):
    """所有领域规则冲突的基类。"""

    code = "domain_error"

    def __init__(self, message: str, *, field: str = "$") -> None:
        super().__init__(message)
        self.field = field


class PhaseError(DomainError):
    """当前阶段不允许该操作。"""

    code = "phase_violation"


class AuthorizationError(DomainError):
    """操作人无权执行该动作（如店长批准自己的损耗）。"""

    code = "authorization_violation"


class ConcurrencyError(DomainError):
    """聚合版本已被并发提交推进。"""

    code = "concurrency_conflict"


class IdempotencyConflictError(DomainError):
    """相同回执/命令编号但关键参数不一致，必须暂停人工处理。"""

    code = "idempotency_conflict"


class EntitlementExhaustedError(DomainError):
    """权益余额不足或已被占用，并发退款/换店/提货不得重复消耗。"""

    code = "entitlement_exhausted"


class DuplicateRedemptionError(DomainError):
    """核销凭据已使用过；历史核销不得因门店合并重复计算。"""

    code = "duplicate_redemption"


class ConflictHoldError(DomainError):
    """案件已因编号冲突暂停，必须先解除暂停。"""

    code = "case_held"
