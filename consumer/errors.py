"""Domain-level exceptions used by the consumer."""
from __future__ import annotations


class ValidationError(Exception):
    """Raised when an event fails business or schema validation.

    Events that raise ValidationError are routed to the DLQ.
    """

    def __init__(self, message: str, code: str = "VALIDATION_ERROR"):
        super().__init__(message)
        self.code = code


class StaleEventError(Exception):
    """Raised when an event is older than the last applied event for the
    affected entity. The event is acknowledged (offset committed, event_id
    recorded as processed) but not applied to the state tables.
    """

    def __init__(self, message: str):
        super().__init__(message)
