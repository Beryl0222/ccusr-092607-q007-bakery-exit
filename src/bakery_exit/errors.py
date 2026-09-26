"""领域错误类型。"""

from __future__ import annotations


class ClearingError(RuntimeError):
    """所有清算规则错误的基类。"""


class ValidationError(ClearingError):
    """命令本身不合法（缺字段、金额非法等）。"""


class StageViolation(ClearingError):
    """当前阶段不允许该操作（时钟尚未推进到对应期限）。"""


class ResponsibilityViolation(ClearingError):
    """职责分离被破坏（店长自批、越权确认等）。"""


class EntitlementAlreadyConsumed(ClearingError):
    """退款、换店、提货并发竞争：同一份权益已被另一种处置消耗。"""


class ConfirmationRequired(ClearingError):
    """资金清算或承接门店尚未独立确认，资产不得转移。"""


class SettlementBlocked(ClearingError):
    """仍有未完成义务，不能结算或关闭。"""


class SuspendedError(ClearingError):
    """该回执处于暂停状态，裁决恢复前不得继续。"""
