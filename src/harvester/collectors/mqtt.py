"""MQTT collector.

Subscribes to a configurable set of MQTT topics and exposes each one as a
Prometheus metric.  An asyncio background task keeps the subscription alive
and updates an in-memory cache on every incoming message.  :meth:`collect`
reads from that cache instantly — the Prometheus scrape never blocks on I/O.

Each topic maps to exactly one metric via the ``metrics`` list in the
configuration:

.. code-block:: yaml

    meters:
      - name: home_mqtt
        source: mqtt
        host: 192.168.1.10
        # port: 1883          # default
        # username: user      # optional
        # password: secret    # optional
        # client_id: harvester  # optional, auto-generated if omitted
        # keepalive: 60       # default
        metrics:
          - topic: home/solar/power
            metric_name: solar_power_watts
            description: "Current solar panel output in watts"
            metric_type: gauge

          - topic: home/grid/energy_import
            metric_name: grid_energy_import_kwh
            description: "Total grid energy imported in kWh"
            metric_type: counter

          - topic: home/battery/soc
            metric_name: battery_soc_percent
            description: "Battery state of charge in percent"
            # metric_type defaults to gauge

          # JSON payload example — extracts DS18B20-1.Temperature from:
          # {"DS18B20-1": {"Temperature": 19.4}, "DS18B20-2": {"Temperature": 21.3}}
          - topic: home/sensors/temperatures
            metric_name: temperature_celsius
            description: "Temperature sensor reading"
            json_path: DS18B20-1.Temperature

Topics that have not yet received a message are omitted from the output
(they do not appear in /metrics at all) until the first message arrives.

Message payloads can be plain numeric strings (``"1234.5"``), boolean
strings (``"true"`` / ``"false"`` → ``1.0`` / ``0.0``), or JSON objects
addressed via ``json_path``.  Unrecognised payloads are logged and ignored.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from dataclasses import dataclass, field
from typing import Literal

import aiomqtt
from pydantic import Field

from harvester.collectors.base import BaseCollector
from harvester.collectors.registry import register
from harvester.models import BaseMeterConfig, Metric, MetricType
from harvester.models.config import StrictModel

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Payload parsing
# ---------------------------------------------------------------------------


def _to_float(
    value: object, topic: str, json_path: str | None, collector_name: str
) -> float | None:
    """Convert a payload value (str, int, float, or bool) to float.

    - ``True``  → ``1.0``
    - ``False`` → ``0.0``
    - ``"true"`` / ``"false"`` (case-insensitive) → ``1.0`` / ``0.0``
    - Any numeric string or number → ``float(value)``
    - Anything else → ``None`` (logged as a warning)
    """
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip().lower()
        if stripped == "true":
            return 1.0
        if stripped == "false":
            return 0.0
        try:
            return float(stripped)
        except ValueError:
            pass

    location = f"{topic}[{json_path}]" if json_path else topic
    logger.warning(
        "[%s] Cannot convert %r to float on %s — ignoring",
        collector_name, value, location,
    )
    return None


# ---------------------------------------------------------------------------
# Per-metric config (nested inside MqttConfig)
# ---------------------------------------------------------------------------


class MqttMetricConfig(StrictModel):
    """Mapping from a single MQTT topic to a Prometheus metric."""

    topic: str = Field(..., min_length=1, description="MQTT topic to subscribe to")
    metric_name: str = Field(
        ...,
        min_length=1,
        description="Prometheus metric name (e.g. 'solar_power_watts')",
    )
    description: str = Field(
        default="",
        description="Human-readable description shown in /metrics",
    )
    metric_type: MetricType = Field(
        default=MetricType.GAUGE,
        description="Prometheus metric type: gauge or counter",
    )
    json_path: str | None = Field(
        default=None,
        description=(
            "Dot-separated path into a JSON payload, e.g. 'DS18B20-1.Temperature'. "
            "Leave empty for plain numeric payloads."
        ),
    )
    extra_labels: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Additional static labels to attach to this metric, e.g. "
            "'{phase: L1}' to distinguish per-phase topics that share a metric name."
        ),
    )


# ---------------------------------------------------------------------------
# Top-level collector config
# ---------------------------------------------------------------------------


@register
class MqttConfig(BaseMeterConfig):
    """Configuration for an MQTT collector."""

    source: Literal["mqtt"] = "mqtt"

    host: str = Field(..., min_length=1, description="MQTT broker hostname or IP")
    port: int = Field(default=1883, ge=1, le=65535, description="MQTT broker port")
    username: str | None = Field(default=None, description="MQTT username (optional)")
    password: str | None = Field(default=None, description="MQTT password (optional)")
    client_id: str | None = Field(
        default=None,
        description="MQTT client ID (auto-generated if omitted)",
    )
    keepalive: int = Field(
        default=60,
        gt=0,
        description="MQTT keepalive interval in seconds",
    )
    metrics: list[MqttMetricConfig] = Field(
        ...,
        min_length=1,
        description="List of topic→metric mappings",
    )

    def create_collector(self) -> MqttCollector:
        return MqttCollector(self)


# ---------------------------------------------------------------------------
# Internal cache entry
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _CachedValue:
    value: float
    metric_name: str
    description: str
    metric_type: MetricType
    topic: str = ""  # original topic (for the label)
    extra_labels: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Collector
# ---------------------------------------------------------------------------


class MqttCollector(BaseCollector):
    """Subscribe to MQTT topics and cache the latest value for each.

    A background asyncio task maintains the subscription and reconnects
    automatically on disconnect.  :meth:`collect` reads from the cache
    and returns immediately without blocking on I/O.
    """

    def __init__(self, config: MqttConfig) -> None:
        super().__init__(config.name, config)
        self._config = config

        # topic → cached value
        self._cache: dict[str, _CachedValue] = {}
        # topic → list of metric configs (multiple metrics can share a topic
        # when using json_path to extract different keys from the same payload)
        self._topic_map: dict[str, list[MqttMetricConfig]] = {}
        for m in config.metrics:
            self._topic_map.setdefault(m.topic, []).append(m)

        self._listen_task: asyncio.Task | None = None
        self._ready = asyncio.Event()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def is_ready(self) -> bool:
        """True once at least one message has been received on any topic."""
        return self._ready.is_set()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Start the background MQTT listener task."""
        logger.info(
            "[%s] Connecting to MQTT broker at %s:%d",
            self.name, self._config.host, self._config.port,
        )
        self._listen_task = asyncio.create_task(
            self._listen_loop(), name=f"mqtt-listen-{self.name}"
        )

        # Wait for the first message (or a short timeout) so startup errors
        # surface promptly rather than silently at the first scrape.
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=10.0)
        except TimeoutError:
            logger.warning(
                "[%s] No MQTT message received within 10 s — "
                "broker reachable but topics may be empty",
                self.name,
            )

    async def disconnect(self) -> None:
        """Cancel the background listener task."""
        if self._listen_task and not self._listen_task.done():
            self._listen_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._listen_task
            self._listen_task = None
        logger.info("[%s] Disconnected", self.name)

    # ------------------------------------------------------------------
    # Background listener
    # ------------------------------------------------------------------

    async def _listen_loop(self) -> None:
        """Connect to the broker, subscribe, and process messages forever.

        Reconnects automatically after any error with a short backoff.
        """
        client_id = self._config.client_id or f"prom-harvester-{uuid.uuid4().hex[:8]}"
        reconnect_delay = 5.0

        while True:
            try:
                async with aiomqtt.Client(
                    hostname=self._config.host,
                    port=self._config.port,
                    username=self._config.username,
                    password=self._config.password,
                    identifier=client_id,
                    keepalive=self._config.keepalive,
                ) as client:
                    # Subscribe to all configured topics
                    for topic in self._topic_map:
                        await client.subscribe(topic)
                        logger.debug("[%s] Subscribed to %s", self.name, topic)

                    logger.info(
                        "[%s] Connected to %s:%d — listening on %d topic(s)",
                        self.name, self._config.host, self._config.port,
                        len(self._topic_map),
                    )

                    async for message in client.messages:
                        self._handle_message(str(message.topic), message.payload)

            except asyncio.CancelledError:
                raise
            except aiomqtt.MqttError as exc:
                logger.warning(
                    "[%s] MQTT error: %s — reconnecting in %.0f s",
                    self.name, exc, reconnect_delay,
                )
                await asyncio.sleep(reconnect_delay)
            except Exception:  # noqa: BLE001
                logger.exception(
                    "[%s] Unexpected error — reconnecting in %.0f s",
                    self.name, reconnect_delay,
                )
                await asyncio.sleep(reconnect_delay)

    def _handle_message(self, topic: str, payload: bytes | str) -> None:
        """Parse a raw MQTT payload and update the cache.

        Supports two payload styles:
        - Plain numeric string: ``"19.4"``
        - JSON object with a dot-separated ``json_path``: ``{"DS18B20-1": {"Temperature": 19.4}}``
          resolved via ``json_path="DS18B20-1.Temperature"``

        For JSON topics, multiple metrics can map to the same topic with
        different ``json_path`` values — all are updated in one pass.
        """
        # Collect all metric configs for this topic (there may be several
        # with different json_paths pointing into the same JSON payload).
        configs = self._topic_map.get(topic)
        if configs is None:
            logger.debug("[%s] Ignoring unexpected topic: %s", self.name, topic)
            return

        raw = payload.decode() if isinstance(payload, bytes) else payload

        updated = 0
        for metric_config in configs:
            value = self._parse_payload(topic, raw, metric_config.json_path)
            if value is None:
                continue

            # Use topic + json_path as cache key so multiple metrics on the
            # same topic don't overwrite each other.
            cache_key = f"{topic}#{metric_config.json_path or ''}"
            self._cache[cache_key] = _CachedValue(
                value=value,
                metric_name=metric_config.metric_name,
                description=metric_config.description,
                metric_type=metric_config.metric_type,
                topic=topic,
                extra_labels=metric_config.extra_labels,
            )
            logger.debug(
                "[%s] %s%s = %s",
                self.name, topic,
                f" [{metric_config.json_path}]" if metric_config.json_path else "",
                value,
            )
            updated += 1

        if updated and not self._ready.is_set():
            self._ready.set()

    def _parse_payload(
        self, topic: str, raw: str, json_path: str | None
    ) -> float | None:
        """Extract a float value from a raw payload string.

        Supported payload formats:
        - Plain numeric string: ``"19.4"``
        - Boolean string: ``"true"`` / ``"false"`` → ``1.0`` / ``0.0``
        - JSON object with a dot-separated ``json_path`` to a numeric or
          boolean leaf value.
        """
        import json  # noqa: PLC0415 — lazy import, json is stdlib

        if json_path:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning(
                    "[%s] Expected JSON on %s but got: %r", self.name, topic, raw
                )
                return None

            node: object = data
            for key in json_path.split("."):
                if not isinstance(node, dict) or key not in node:
                    logger.warning(
                        "[%s] Path %r not found in payload on %s",
                        self.name, json_path, topic,
                    )
                    return None
                node = node[key]

            return _to_float(node, topic, json_path, self.name)

        return _to_float(raw.strip(), topic, None, self.name)

    # ------------------------------------------------------------------
    # Collect
    # ------------------------------------------------------------------

    async def collect(self) -> list[Metric]:
        """Return the latest cached value for each topic as Prometheus metrics.

        Topics that have not yet received a message are omitted entirely.
        Never blocks on I/O.
        """
        if not self._cache:
            logger.debug("[%s] No data yet", self.name)
            return []

        base_labels = {
            "source": self.name,
            "host": self._config.host,
            "device_type": "mqtt",
        }
        metrics: list[Metric] = []

        for cached in self._cache.values():
            metrics.append(
                Metric(
                    name=cached.metric_name,
                    value=cached.value,
                    labels={**base_labels, "topic": cached.topic, **cached.extra_labels},
                    description=cached.description or cached.metric_name,
                    metric_type=cached.metric_type,
                )
            )

        logger.debug("[%s] Collected %d metrics", self.name, len(metrics))
        return metrics

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def get_status(self) -> dict:
        """Return a status dict for the /health endpoint."""
        return {
            "name": self.name,
            "host": self._config.host,
            "port": self._config.port,
            "connected": self._listen_task is not None and not self._listen_task.done(),
            "has_data": self.is_ready,
            "topics": list(self._topic_map),
            "received": [v.topic for v in self._cache.values()],
        }
