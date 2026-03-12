from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol

# ---------------------------------------------------------------------------
# Metric type enum
# ---------------------------------------------------------------------------


class MetricType(StrEnum):
    """Prometheus metric types supported by this exporter."""

    GAUGE = "gauge"
    COUNTER = "counter"
    HISTOGRAM = "histogram"


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class MetricValidationError(ValueError):
    """Raised when a Metric is constructed with an invalid name or label."""


# Module-level compiled patterns (required for slots=True dataclass compat)
_NAME_RE = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")
_LABEL_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


# ---------------------------------------------------------------------------
# Metric
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class Metric:
    """A single time-series data point ready for export."""

    name: str
    value: float
    labels: dict[str, str] = field(default_factory=dict)
    description: str | None = None
    metric_type: MetricType = MetricType.GAUGE

    def __post_init__(self) -> None:
        # Coerce plain strings to the enum so callers can pass "gauge" etc.
        if not isinstance(self.metric_type, MetricType):
            object.__setattr__(self, "metric_type", MetricType(self.metric_type))
        self._validate()

    def _validate(self) -> None:
        if not _NAME_RE.match(self.name):
            raise MetricValidationError(
                f"Invalid metric name {self.name!r}. "
                "Must match [a-zA-Z_:][a-zA-Z0-9_:]*"
            )
        for label in self.labels:
            if not _LABEL_RE.match(label):
                raise MetricValidationError(
                    f"Invalid label name {label!r}. Must match [a-zA-Z_][a-zA-Z0-9_]*"
                )
            if label.startswith("__"):
                raise MetricValidationError(
                    f"Label {label!r} cannot start with '__' (reserved by Prometheus)"
                )


# ---------------------------------------------------------------------------
# Collector protocol
# ---------------------------------------------------------------------------


class CollectorStatus(Protocol):
    """Structural type for the dict returned by get_status()."""

    name: str
    connected: bool
    has_data: bool


class Collector(Protocol):
    """Structural interface every collector must satisfy."""

    name: str

    @property
    def is_ready(self) -> bool:
        """True when the collector has received at least one data point."""
        ...

    async def collect(self) -> list[Metric]:
        """Return the latest snapshot as Prometheus metrics."""
        ...

    def get_status(self) -> dict:
        """Return a status dict for health checks."""
        ...
