from __future__ import annotations

import asyncio
import contextlib
import functools
import json
import logging
import ssl
from dataclasses import dataclass
from typing import ClassVar, Literal

import websockets
from pydantic import Field

from harvester.collectors.base import BaseCollector
from harvester.collectors.registry import register
from harvester.models import BaseMeterConfig, Metric, MetricType

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SSL — built once per process, shared across all reconnects
# ---------------------------------------------------------------------------


@functools.lru_cache(maxsize=1)
def _build_ssl_context() -> ssl.SSLContext:
    """Return a cached SSL context that skips certificate verification.

    HomeWizard P1 meters use self-signed certificates, so verification is
    intentionally disabled.  The cache means we pay the construction cost
    once across the whole process lifetime.
    """
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


# ---------------------------------------------------------------------------
# Metric mapping descriptor
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class MetricMapping:
    """Maps a raw P1 JSON key to a Prometheus metric definition."""

    key: str
    name: str
    description: str
    metric_type: MetricType = MetricType.GAUGE


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


@register
class HomewizardP1Config(BaseMeterConfig):
    """Configuration for a HomeWizard P1 WiFi meter."""

    source: Literal["homewizard_p1"] = "homewizard_p1"
    host: str = Field(..., min_length=1, description="Hostname or IP of the meter")
    token: str | None = Field(default=None, description="API token for authorization")

    def create_collector(self) -> HomewizardP1Collector:
        return HomewizardP1Collector(config=self)


# ---------------------------------------------------------------------------
# Collector
# ---------------------------------------------------------------------------


class HomewizardP1Collector(BaseCollector):
    """Collect metrics from a HomeWizard P1 WiFi meter via WebSocket."""

    RECONNECT_DELAY: ClassVar[int] = 5  # seconds between reconnect attempts

    METRIC_MAPPINGS: ClassVar[tuple[MetricMapping, ...]] = (
        # --- Power (Watts) ---
        MetricMapping(
            "power_w", "homewizard_active_power_watts", "Active power (total) in watts"
        ),
        MetricMapping(
            "power_l1_w", "homewizard_active_power_l1_watts", "Active power L1 in watts"
        ),
        MetricMapping(
            "power_l2_w", "homewizard_active_power_l2_watts", "Active power L2 in watts"
        ),
        MetricMapping(
            "power_l3_w", "homewizard_active_power_l3_watts", "Active power L3 in watts"
        ),
        # --- Energy import (kWh) ---
        MetricMapping(
            "energy_import_kwh",
            "homewizard_energy_import_kwh",
            "Total energy imported",
            MetricType.COUNTER,
        ),
        MetricMapping(
            "energy_import_t1_kwh",
            "homewizard_energy_import_t1_kwh",
            "Energy imported tariff 1",
            MetricType.COUNTER,
        ),
        MetricMapping(
            "energy_import_t2_kwh",
            "homewizard_energy_import_t2_kwh",
            "Energy imported tariff 2",
            MetricType.COUNTER,
        ),
        # Legacy import keys
        MetricMapping(
            "total_power_import_kwh",
            "homewizard_total_power_import_kwh",
            "Total power imported (legacy)",
            MetricType.COUNTER,
        ),
        MetricMapping(
            "total_power_import_t1_kwh",
            "homewizard_total_power_import_t1_kwh",
            "Total power imported T1 (legacy)",
            MetricType.COUNTER,
        ),
        MetricMapping(
            "total_power_import_t2_kwh",
            "homewizard_total_power_import_t2_kwh",
            "Total power imported T2 (legacy)",
            MetricType.COUNTER,
        ),
        # --- Energy export (kWh) ---
        MetricMapping(
            "energy_export_kwh",
            "homewizard_energy_export_kwh",
            "Total energy exported",
            MetricType.COUNTER,
        ),
        MetricMapping(
            "energy_export_t1_kwh",
            "homewizard_energy_export_t1_kwh",
            "Energy exported tariff 1",
            MetricType.COUNTER,
        ),
        MetricMapping(
            "energy_export_t2_kwh",
            "homewizard_energy_export_t2_kwh",
            "Energy exported tariff 2",
            MetricType.COUNTER,
        ),
        # Legacy export keys
        MetricMapping(
            "total_power_export_kwh",
            "homewizard_total_power_export_kwh",
            "Total power exported (legacy)",
            MetricType.COUNTER,
        ),
        MetricMapping(
            "total_power_export_t1_kwh",
            "homewizard_total_power_export_t1_kwh",
            "Total power exported T1 (legacy)",
            MetricType.COUNTER,
        ),
        MetricMapping(
            "total_power_export_t2_kwh",
            "homewizard_total_power_export_t2_kwh",
            "Total power exported T2 (legacy)",
            MetricType.COUNTER,
        ),
        # --- Voltage (Volts) ---
        MetricMapping(
            "voltage_l1_v", "homewizard_voltage_l1_volts", "Voltage L1 in volts"
        ),
        MetricMapping(
            "voltage_l2_v", "homewizard_voltage_l2_volts", "Voltage L2 in volts"
        ),
        MetricMapping(
            "voltage_l3_v", "homewizard_voltage_l3_volts", "Voltage L3 in volts"
        ),
        # --- Current (Amps) ---
        MetricMapping(
            "current_a", "homewizard_current_total_amps", "Total current in amps"
        ),
        MetricMapping(
            "current_l1_a", "homewizard_current_l1_amps", "Current L1 in amps"
        ),
        MetricMapping(
            "current_l2_a", "homewizard_current_l2_amps", "Current L2 in amps"
        ),
        MetricMapping(
            "current_l3_a", "homewizard_current_l3_amps", "Current L3 in amps"
        ),
        # --- Tariff ---
        MetricMapping("tariff", "homewizard_active_tariff", "Active tariff (1 or 2)"),
        # --- Voltage sags ---
        MetricMapping(
            "voltage_sag_l1_count",
            "homewizard_voltage_sag_l1_total",
            "Voltage sag count L1",
            MetricType.COUNTER,
        ),
        MetricMapping(
            "voltage_sag_l2_count",
            "homewizard_voltage_sag_l2_total",
            "Voltage sag count L2",
            MetricType.COUNTER,
        ),
        MetricMapping(
            "voltage_sag_l3_count",
            "homewizard_voltage_sag_l3_total",
            "Voltage sag count L3",
            MetricType.COUNTER,
        ),
        # --- Voltage swells ---
        MetricMapping(
            "voltage_swell_l1_count",
            "homewizard_voltage_swell_l1_total",
            "Voltage swell count L1",
            MetricType.COUNTER,
        ),
        MetricMapping(
            "voltage_swell_l2_count",
            "homewizard_voltage_swell_l2_total",
            "Voltage swell count L2",
            MetricType.COUNTER,
        ),
        MetricMapping(
            "voltage_swell_l3_count",
            "homewizard_voltage_swell_l3_total",
            "Voltage swell count L3",
            MetricType.COUNTER,
        ),
        # --- Power failures ---
        MetricMapping(
            "any_power_fail_count",
            "homewizard_power_fail_any_total",
            "Power failures (any duration)",
            MetricType.COUNTER,
        ),
        MetricMapping(
            "long_power_fail_count",
            "homewizard_power_fail_long_total",
            "Long power failures",
            MetricType.COUNTER,
        ),
        # --- Gas & water (legacy direct keys) ---
        MetricMapping(
            "total_gas_m3",
            "homewizard_gas_total_m3",
            "Total gas usage in m³",
            MetricType.COUNTER,
        ),
        MetricMapping(
            "total_water_m3",
            "homewizard_water_total_m3",
            "Total water usage in m³",
            MetricType.COUNTER,
        ),
    )

    def __init__(self, config: HomewizardP1Config) -> None:
        super().__init__(config.name, config)
        self.host: str = config.host
        self.token: str | None = config.token

        self._ws: websockets.WebSocketClientProtocol | None = None
        self._latest_data: dict = {}
        self._listen_task: asyncio.Task | None = None
        self._connected: bool = False

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def ws_url(self) -> str:
        return f"wss://{self.host}/api/ws"

    @property
    def is_connected(self) -> bool:
        return self._connected and self._ws is not None

    @property
    def is_ready(self) -> bool:
        """True once the first measurement has been received."""
        return bool(self._latest_data)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Connect to the WebSocket and start the background listen task."""
        logger.info("[%s] Connecting to %s", self.name, self.ws_url)
        self._ws = await websockets.connect(self.ws_url, ssl=_build_ssl_context())
        self._connected = True
        self._listen_task = asyncio.create_task(
            self._listen(), name=f"{self.name}-listen"
        )
        logger.info("[%s] Connected", self.name)

    async def disconnect(self) -> None:
        """Cancel the listen task and close the WebSocket."""
        logger.info("[%s] Disconnecting", self.name)
        self._connected = False

        if self._listen_task and not self._listen_task.done():
            self._listen_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._listen_task
            self._listen_task = None

        if self._ws:
            await self._ws.close()
            self._ws = None

        logger.info("[%s] Disconnected", self.name)

    # ------------------------------------------------------------------
    # WebSocket internals
    # ------------------------------------------------------------------

    async def _reconnect(self) -> bool:
        """Try once to re-establish the WebSocket connection."""
        logger.info("[%s] Reconnecting…", self.name)
        try:
            self._ws = await websockets.connect(self.ws_url, ssl=_build_ssl_context())
        except Exception:
            logger.exception("[%s] Reconnection failed", self.name)
            return False
        logger.info("[%s] Reconnected", self.name)
        return True

    async def _send(self, msg_type: str, data: str) -> None:
        """Send a typed JSON message to the server."""
        if not self._ws:
            logger.warning("[%s] Cannot send — not connected", self.name)
            return
        await self._ws.send(json.dumps({"type": msg_type, "data": data}))

    async def _handle_message(self, raw: str) -> None:
        """Parse and dispatch a single WebSocket message."""
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("[%s] Received non-JSON message", self.name)
            return

        match data.get("type"):
            case "authorization_requested":
                logger.info("[%s] Authorising", self.name)
                await self._send("authorization", self.token or "")
            case "authorized":
                logger.info("[%s] Authorised — subscribing to measurements", self.name)
                await self._send("subscribe", "measurement")
            case "subscription_accepted":
                logger.info("[%s] Subscription accepted", self.name)
            case "measurement":
                self._latest_data = data.get("data", {})
                logger.debug("[%s] Measurement received", self.name)
            case "error":
                logger.error(
                    "[%s] Server error: %s", self.name, data.get("data", "unknown")
                )
            case unknown:
                logger.warning("[%s] Unknown message type: %s", self.name, unknown)

    async def _listen(self) -> None:
        """Background task: read messages and reconnect on connection drops."""
        while self._connected:
            try:
                async for raw in self._ws:
                    await self._handle_message(raw)

            except websockets.ConnectionClosed as exc:
                if not self._connected:
                    break
                logger.warning(
                    "[%s] Connection closed (code=%s reason=%s) — retrying in %ss",
                    self.name,
                    exc.code,
                    exc.reason,
                    self.RECONNECT_DELAY,
                )
                await asyncio.sleep(self.RECONNECT_DELAY)
                if self._connected:
                    await self._reconnect()

            except asyncio.CancelledError:
                break

            except Exception:
                logger.exception("[%s] Unexpected error in listen loop", self.name)
                if self._connected:
                    await asyncio.sleep(self.RECONNECT_DELAY)

    # ------------------------------------------------------------------
    # Metric collection
    # ------------------------------------------------------------------

    async def collect(self) -> list[Metric]:
        """Return the latest snapshot as a list of Prometheus metrics."""
        if not self._latest_data:
            logger.debug("[%s] No data yet", self.name)
            return []

        base_labels = {"source": self.name, "host": self.host}
        metrics: list[Metric] = []

        self._collect_mapped_metrics(base_labels, metrics)
        self._collect_external_devices(base_labels, metrics)
        self._collect_meter_info(base_labels, metrics)

        logger.debug("[%s] Collected %d metrics", self.name, len(metrics))
        return metrics

    def _collect_mapped_metrics(
        self, base_labels: dict[str, str], out: list[Metric]
    ) -> None:
        for mapping in self.METRIC_MAPPINGS:
            raw = self._latest_data.get(mapping.key)
            if raw is None:
                continue
            try:
                out.append(
                    Metric(
                        name=mapping.name,
                        value=float(raw),
                        labels=base_labels,
                        description=mapping.description,
                        metric_type=mapping.metric_type,
                    )
                )
            except (ValueError, TypeError):
                logger.warning(
                    "[%s] Cannot convert %s=%r to float", self.name, mapping.key, raw
                )

    def _collect_external_devices(
        self, base_labels: dict[str, str], out: list[Metric]
    ) -> None:
        for device in self._latest_data.get("external", []):
            raw = device.get("value")
            if raw is None:
                continue

            device_type: str = device.get("type", "unknown")
            unit: str = device.get("unit", "")
            device_labels = {
                **base_labels,
                "device_type": device_type,
                "device_id": device.get("unique_id", "unknown"),
            }

            match device_type:
                case "gas_meter":
                    name = "homewizard_external_gas_total_m3"
                    desc = f"External gas meter reading in {unit}"
                    mtype = MetricType.COUNTER
                case "water_meter":
                    name = "homewizard_external_water_total_m3"
                    desc = f"External water meter reading in {unit}"
                    mtype = MetricType.COUNTER
                case _:
                    safe = device_type.replace("-", "_").replace(" ", "_")
                    name = f"homewizard_external_{safe}_value"
                    desc = f"External {device_type} reading in {unit}"
                    mtype = MetricType.GAUGE
                    logger.info(
                        "[%s] Unknown external device type: %s", self.name, device_type
                    )

            try:
                out.append(
                    Metric(
                        name=name,
                        value=float(raw),
                        labels=device_labels,
                        description=desc,
                        metric_type=mtype,
                    )
                )
            except (ValueError, TypeError):
                logger.warning(
                    "[%s] Cannot convert external device %s=%r to float",
                    self.name,
                    device_type,
                    raw,
                )

    def _collect_meter_info(
        self, base_labels: dict[str, str], out: list[Metric]
    ) -> None:
        if not self._latest_data:
            return
        out.append(
            Metric(
                name="homewizard_meter_info",
                value=1,
                labels={
                    **base_labels,
                    "unique_id": str(self._latest_data.get("unique_id", "")),
                    "meter_model": str(self._latest_data.get("meter_model", "")),
                    "protocol_version": str(
                        self._latest_data.get("protocol_version", "")
                    ),
                },
                description="HomeWizard P1 meter information",
                metric_type=MetricType.GAUGE,
            )
        )

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------

    def get_status(self) -> dict:
        """Return a status dict for the ``/health`` endpoint."""
        return {
            "name": self.name,
            "host": self.host,
            "connected": self.is_connected,
            "has_data": self.is_ready,
            "metrics_count": len(self._latest_data),
            "meter_model": self._latest_data.get("meter_model", "unknown"),
            "protocol_version": self._latest_data.get("protocol_version", "unknown"),
        }
