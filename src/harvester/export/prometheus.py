from __future__ import annotations

import logging
from dataclasses import dataclass, field

from prometheus_client import (
    GC_COLLECTOR,
    PLATFORM_COLLECTOR,
    PROCESS_COLLECTOR,
    CollectorRegistry,
    generate_latest,
)
from prometheus_client.utils import floatToGoString

from harvester.models import Metric, MetricType

logger = logging.getLogger(__name__)

_DEFAULT_COLLECTORS = (PROCESS_COLLECTOR, PLATFORM_COLLECTOR, GC_COLLECTOR)


@dataclass(slots=True)
class _MetricFamily:
    description: str
    metric_type: MetricType
    samples: dict[tuple[tuple[str, str], ...], float] = field(default_factory=dict)


class PrometheusExporter:
    """Maintain metric families and render them in Prometheus text format.

    Counter values are stored as absolute snapshots from the upstream device and
    rendered with their declared Prometheus type. This keeps the original
    metric names stable while exposing accurate ``# TYPE`` metadata.
    """

    def __init__(
        self,
        prefix: str = "",
        disable_default_metrics: bool = True,
    ) -> None:
        self._prefix = prefix
        self._families: dict[str, _MetricFamily] = {}
        self._default_registry: CollectorRegistry | None = None

        if not disable_default_metrics:
            self._default_registry = CollectorRegistry(auto_describe=True)
            for collector in _DEFAULT_COLLECTORS:
                self._default_registry.register(collector)

    # ------------------------------------------------------------------
    # Context-manager support (simplifies test isolation)
    # ------------------------------------------------------------------

    def __enter__(self) -> PrometheusExporter:
        return self

    def __exit__(self, *_: object) -> None:
        """Drop all cached metric families on teardown."""
        self._families.clear()

    # ------------------------------------------------------------------
    # Setup helpers
    # ------------------------------------------------------------------

    def _full_name(self, name: str) -> str:
        return f"{self._prefix}_{name}" if self._prefix else name

    @staticmethod
    def _sample_key(labels: dict[str, str]) -> tuple[tuple[str, str], ...]:
        return tuple(sorted(labels.items()))

    @staticmethod
    def _escape_help(text: str) -> str:
        return text.replace("\\", r"\\").replace("\n", r"\n")

    @staticmethod
    def _escape_label_value(value: str) -> str:
        return (
            value.replace("\\", r"\\")
            .replace("\n", r"\n")
            .replace('"', r'\"')
        )

    def _get_or_create_family(self, metric: Metric) -> _MetricFamily:
        full = self._full_name(metric.name)
        family = self._families.get(full)
        if family is None:
            family = _MetricFamily(
                description=metric.description or full,
                metric_type=metric.metric_type,
            )
            self._families[full] = family
            logger.debug("Registered metric family: %s type=%s", full, family.metric_type)
            return family

        if family.metric_type is not metric.metric_type:
            msg = (
                f"Metric {full!r} was already registered as {family.metric_type}, "
                f"cannot update it as {metric.metric_type}"
            )
            raise ValueError(msg)

        if metric.description and family.description != metric.description:
            logger.warning(
                "Metric %s description mismatch; keeping existing description %r",
                full,
                family.description,
            )

        return family

    def _render_metrics(self) -> bytes:
        lines: list[str] = []
        for name in sorted(self._families):
            family = self._families[name]
            lines.append(f"# HELP {name} {self._escape_help(family.description)}\n")
            lines.append(f"# TYPE {name} {family.metric_type.value}\n")
            for sample_key, value in sorted(family.samples.items()):
                if sample_key:
                    rendered_labels = ",".join(
                        f'{key}="{self._escape_label_value(label_value)}"'
                        for key, label_value in sample_key
                    )
                    lines.append(
                        f"{name}{{{rendered_labels}}} {floatToGoString(value)}\n"
                    )
                else:
                    lines.append(f"{name} {floatToGoString(value)}\n")

        return "".join(lines).encode("utf-8")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def update_metric(self, metric: Metric) -> bool:
        """Set the value of a single metric. Returns ``True`` on success."""
        if metric.metric_type is MetricType.HISTOGRAM:
            logger.warning("Histogram metrics are not supported: %s", metric.name)
            return False
        try:
            family = self._get_or_create_family(metric)
            family.samples[self._sample_key(metric.labels)] = metric.value
        except ValueError as exc:
            logger.warning("Metric update rejected for %s: %s", metric.name, exc)
            return False
        except Exception:
            logger.exception("Failed to update metric %s", metric.name)
            return False
        else:
            return True

    def update_metrics(self, metrics: list[Metric]) -> tuple[int, int]:
        """Set values for a batch of metrics.

        Returns:
            ``(success_count, failure_count)``
        """
        success = sum(1 for m in metrics if self.update_metric(m))
        failed = len(metrics) - success
        if failed:
            logger.warning("%d/%d metric(s) failed to update", failed, len(metrics))
        return success, failed

    def get_metrics(self) -> bytes:
        """Render the current registry in Prometheus text format."""
        rendered = self._render_metrics()
        if self._default_registry is None:
            return rendered

        default_metrics = generate_latest(self._default_registry)
        if rendered and default_metrics:
            return rendered + default_metrics
        return rendered or default_metrics

    @property
    def registered_count(self) -> int:
        """Number of metric families currently registered."""
        return len(self._families)
