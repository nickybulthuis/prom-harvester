from __future__ import annotations

from harvester.export.prometheus import PrometheusExporter
from harvester.models import Metric, MetricType


def test_counter_metrics_render_as_counter_type() -> None:
    exporter = PrometheusExporter()

    ok = exporter.update_metric(
        Metric(
            name="homewizard_energy_import_kwh",
            value=123.45,
            labels={"source": "p1"},
            description="Total energy imported",
            metric_type=MetricType.COUNTER,
        )
    )

    assert ok is True
    rendered = exporter.get_metrics().decode("utf-8")
    assert "# TYPE homewizard_energy_import_kwh counter" in rendered
    assert 'homewizard_energy_import_kwh{source="p1"} 123.45' in rendered


def test_gauge_metrics_render_as_gauge_type() -> None:
    exporter = PrometheusExporter(prefix="test")

    ok = exporter.update_metric(
        Metric(
            name="active_power_watts",
            value=456.0,
            labels={"phase": "L1"},
            description="Current power",
            metric_type=MetricType.GAUGE,
        )
    )

    assert ok is True
    rendered = exporter.get_metrics().decode("utf-8")
    assert "# TYPE test_active_power_watts gauge" in rendered
    assert 'test_active_power_watts{phase="L1"} 456.0' in rendered


def test_metric_families_group_multiple_samples_under_one_type_header() -> None:
    exporter = PrometheusExporter()

    for phase, value in (("L1", 1.0), ("L2", 2.0), ("L3", 3.0)):
        assert exporter.update_metric(
            Metric(
                name="voltage_sag_total",
                value=value,
                labels={"phase": phase},
                description="Voltage sag count",
                metric_type=MetricType.COUNTER,
            )
        )

    rendered = exporter.get_metrics().decode("utf-8")
    assert rendered.count("# TYPE voltage_sag_total counter") == 1
    assert 'voltage_sag_total{phase="L1"} 1.0' in rendered
    assert 'voltage_sag_total{phase="L2"} 2.0' in rendered
    assert 'voltage_sag_total{phase="L3"} 3.0' in rendered


def test_conflicting_metric_types_are_rejected() -> None:
    exporter = PrometheusExporter()

    assert exporter.update_metric(
        Metric(
            name="shared_metric",
            value=1.0,
            metric_type=MetricType.COUNTER,
        )
    )

    assert (
        exporter.update_metric(
            Metric(
                name="shared_metric",
                value=2.0,
                metric_type=MetricType.GAUGE,
            )
        )
        is False
    )

