"""连锁烘焙退场义务清算台领域契约与清算服务。"""

from .clock import ControllableClock
from .contracts import ContractIssue, validate_event
from .errors import (
    AuthorizationError,
    ConcurrencyError,
    ConflictHoldError,
    DomainError,
    DuplicateRedemptionError,
    EntitlementExhaustedError,
    IdempotencyConflictError,
    PhaseError,
)
from .scheduler import STAGE_ORDER, Scheduler
from .service import ExitClearingService
from .store import EventSpec, EventStore, StoredEvent
from .views import CustomerView, EmployeeView, HeadquartersView, ReadModel, SupplierView

__all__ = [
    "AuthorizationError",
    "ConcurrencyError",
    "ConflictHoldError",
    "ContractIssue",
    "ControllableClock",
    "CustomerView",
    "DomainError",
    "DuplicateRedemptionError",
    "EmployeeView",
    "EntitlementExhaustedError",
    "EventSpec",
    "EventStore",
    "ExitClearingService",
    "HeadquartersView",
    "IdempotencyConflictError",
    "PhaseError",
    "ReadModel",
    "STAGE_ORDER",
    "Scheduler",
    "StoredEvent",
    "SupplierView",
    "validate_event",
]
