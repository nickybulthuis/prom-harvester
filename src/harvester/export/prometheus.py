from __future__ import annotations

import contextlib
import logging

from prometheus_client import (
    GC_COLLECTOR,
    PLATFORM_COLLECTOR,
    PROCESS_COLLECTOR,
    REGISTRY,
    Gauge,
    generate_latest,
)

from harvester.models import Metric, MetricType

logger = logging.getLogger(__name__)

_DEFAULT_COLLECTORS = (PROCESS_COLLECTOR, PLATFORM_COLLECTOR, GC_COLLECTOR)


class PrometheusExporter:
    """Maintain a registry of Prometheus gauges and render them as text.

    Design note: we use :class:`~prometheus_client.Gauge` for **all** metric
    types, including counters.  The P1 meter exposes cumulative energy totals
    as absolute values (e.g. total kWh since installation), not as increments,
    so a Prometheus ``Counter`` (which only ever increases via ``inc()``) would
    not model them correctly.  Using a ``Gauge`` with ``set()`` preserves the
    absolute values while still letting Grafana calculate rates via
    ``increase()`` / ``irate()``.
    """

    def __init__(
        self,
        prefix: str = "",
        disable_default_metrics: bool = True,
    ) -> None:
        self._prefix = prefix
        self._gauges: dict[str, Gauge] = {}

        if disable_default_metrics:
            self._unregister_defaults()

    # ------------------------------------------------------------------
    # Context-manager support (simplifies test isolation)
    # ------------------------------------------------------------------

    def __enter__(self) -> PrometheusExporter:
        return self

    def __exit__(self, *_: object) -> None:
        """Unregister all gauges created by this exporter on teardown."""
        for gauge in self._gauges.values():
            with contextlib.suppress(KeyError):
                REGISTRY.unregister(gauge)
        self._gauges.clear()

    # ------------------------------------------------------------------
    # Setup helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _unregister_defaults() -> None:
        for collector in _DEFAULT_COLLECTORS:
            with contextlib.suppress(KeyError):
                REGISTRY.unregister(collector)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _full_name(self, name: str) -> str:
        return f"{self._prefix}_{name}" if self._prefix else name

    def _get_or_create_gauge(self, metric: Metric) -> Gauge:
        full = self._full_name(metric.name)
        if full not in self._gauges:
            label_names = list(metric.labels)
            self._gauges[full] = Gauge(full, metric.description or full, label_names)
            logger.debug("Registered gauge: %s labels=%s", full, label_names)
        return self._gauges[full]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def update_metric(self, metric: Metric) -> bool:
        """Set the value of a single metric. Returns ``True`` on success."""
        if metric.metric_type is MetricType.HISTOGRAM:
            logger.warning("Histogram metrics are not supported: %s", metric.name)
            return False
        try:
            gauge = self._get_or_create_gauge(metric)
            target = gauge.labels(**metric.labels) if metric.labels else gauge
            target.set(metric.value)
        except ValueError:
            logger.exception("Invalid value for metric %s", metric.name)
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
        return generate_latest(REGISTRY)

    @property
    def registered_count(self) -> int:
        """Number of gauge series currently registered."""
        return len(self._gauges)
