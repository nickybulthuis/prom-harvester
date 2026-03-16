"""Peblar Home EV charger collector — Modbus TCP.

Reads the Peblar input-register map over Modbus TCP and exposes energy,
power, voltage, current, and EV-interface metrics to Prometheus.

Register map (input registers, big-endian — SPD6004230484R03)
--------------------------------------------------------------
Energy meter
  30000  EnergyTotal      Lifetime energy          Wh      int64  (4 regs)
  30004  SessionEnergy    Session energy            Wh      int64  (4 regs)
  30008  PowerPhase1      Instant power L1          W       int32  (2 regs)
  30010  PowerPhase2      Instant power L2          W       int32  (2 regs)
  30012  PowerPhase3      Instant power L3          W       int32  (2 regs)
  30014  PowerTotal       Combined power L1+L2+L3   W       int32  (2 regs)
  30016  VoltagePhase1    Instant voltage L1         V       int32  (2 regs)
  30018  VoltagePhase2    Instant voltage L2         V       int32  (2 regs)
  30020  VoltagePhase3    Instant voltage L3         V       int32  (2 regs)
  30022  CurrentPhase1    Instant current L1         mA      int32  (2 regs)
  30024  CurrentPhase2    Instant current L2         mA      int32  (2 regs)
  30026  CurrentPhase3    Instant current L3         mA      int32  (2 regs)

System information
  30086  WlanSignalStrength   WLAN RSSI              dB      int32  (2 regs)
  30088  CellularSignalStrength  LTE RSSI            dB      int32  (2 regs)
  30090  Uptime               System uptime          s       uint32 (2 regs)
  30092  Phasecount           Connected phases        —       uint16 (1 reg)

EV interface
  30113  ChargeCurrentLimitActual  Current to vehicle  mA    uint32 (2 regs)

Modbus input registers use function code 04.
Addresses are 0-based PDU addresses (register number - 30001).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import struct
from typing import ClassVar, Literal

from pydantic import Field
from pymodbus.client import AsyncModbusTcpClient
from pymodbus.exceptions import ModbusException

from harvester.collectors.base import BaseCollector
from harvester.collectors.registry import register
from harvester.models import BaseMeterConfig, Metric, MetricType

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Modbus helpers
# ---------------------------------------------------------------------------

_MODBUS_PORT = 502
_SLAVE_ID = 1


def _to_int32(high: int, low: int) -> int:
    """Combine two 16-bit registers into a signed 32-bit integer (big-endian)."""
    raw = (high << 16) | low
    return struct.unpack(">i", struct.pack(">I", raw))[0]


def _to_int64(r0: int, r1: int, r2: int, r3: int) -> int:
    """Combine four 16-bit registers into a signed 64-bit integer (big-endian)."""
    raw = (r0 << 48) | (r1 << 32) | (r2 << 16) | r3
    return struct.unpack(">q", struct.pack(">Q", raw))[0]


def _to_uint32(high: int, low: int) -> int:
    """Combine two 16-bit registers into an unsigned 32-bit integer (big-endian)."""
    return (high << 16) | low


# ---------------------------------------------------------------------------
# EV interface helpers
# ---------------------------------------------------------------------------

# CpState is stored as an ASCII char in the low byte of a uint16.
# Map the char to a numeric value so it can be exposed as a Gauge.
# Prometheus can't store strings, so we encode the state as an integer
# and document the mapping here. Use a Grafana value mapping to display
# human-readable labels in dashboards.
_CP_STATE_MAP: dict[str, float] = {
    "A": 0,  # No EV connected
    "B": 1,  # EV connected, suspended
    "C": 2,  # EV connected, charging
    "D": 3,  # EV connected, charging + ventilation requested
    "E": 4,  # Error (short to PE or powered off)
    "F": 5,  # Fault
    "I": 6,  # Invalid CP level
    "U": 7,  # Unknown
}

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


@register
class PeblarConfig(BaseMeterConfig):
    """Configuration for a Peblar Home EV charger (Modbus TCP)."""

    source: Literal["peblar"] = "peblar"

    host: str = Field(..., description="Hostname or IP address of the Peblar charger")
    port: int = Field(_MODBUS_PORT, description="Modbus TCP port (default 502)")
    slave_id: int = Field(_SLAVE_ID, description="Modbus slave / unit ID (default 1)")
    poll_interval: float = Field(
        10.0,
        gt=0,
        description="Seconds between Modbus polls (default 10 s)",
    )
    request_timeout: float = Field(
        10.0,
        gt=0,
        description="Modbus request timeout in seconds (default 10 s)",
    )

    def create_collector(self) -> PeblarCollector:
        return PeblarCollector(self)


# ---------------------------------------------------------------------------
# Collector
# ---------------------------------------------------------------------------


# Each entry: (metric_name, description, metric_type, scale_factor, phase_label)
# scale_factor converts the raw register value to the canonical unit:
#   current: mA → A (*0.001), energy: Wh → kWh (*0.001), power/voltage: no change.
# phase_label: if set, this value is added as a "phase" label on the metric,
#   allowing per-phase metrics to share a single metric name in Prometheus.
_METRIC_DEFS: dict[str, tuple[str, str, MetricType, float, str | None]] = {
    "energy_total": (
        "peblar_energy_total_kwh",
        "Lifetime energy delivered in kWh",
        MetricType.COUNTER,
        0.001,
        None,
    ),
    "session_energy": (
        "peblar_session_energy_kwh",
        "Energy delivered in the current/last session in kWh",
        MetricType.GAUGE,
        0.001,
        None,
    ),
    "power_l1": (
        "peblar_active_power_watts",
        "Instant power in Watts",
        MetricType.GAUGE,
        1.0,
        "L1",
    ),
    "power_l2": (
        "peblar_active_power_watts",
        "Instant power in Watts",
        MetricType.GAUGE,
        1.0,
        "L2",
    ),
    "power_l3": (
        "peblar_active_power_watts",
        "Instant power in Watts",
        MetricType.GAUGE,
        1.0,
        "L3",
    ),
    "power_total": (
        "peblar_active_power_total_watts",
        "Combined power on all phases in Watts",
        MetricType.GAUGE,
        1.0,
        None,
    ),
    "voltage_l1": (
        "peblar_voltage_volts",
        "Instant voltage in Volts",
        MetricType.GAUGE,
        1.0,
        "L1",
    ),
    "voltage_l2": (
        "peblar_voltage_volts",
        "Instant voltage in Volts",
        MetricType.GAUGE,
        1.0,
        "L2",
    ),
    "voltage_l3": (
        "peblar_voltage_volts",
        "Instant voltage in Volts",
        MetricType.GAUGE,
        1.0,
        "L3",
    ),
    "current_l1": (
        "peblar_current_amperes",
        "Instant current in Amperes",
        MetricType.GAUGE,
        0.001,
        "L1",
    ),
    "current_l2": (
        "peblar_current_amperes",
        "Instant current in Amperes",
        MetricType.GAUGE,
        0.001,
        "L2",
    ),
    "current_l3": (
        "peblar_current_amperes",
        "Instant current in Amperes",
        MetricType.GAUGE,
        0.001,
        "L3",
    ),
    "wlan_rssi": (
        "peblar_wlan_rssi_dbm",
        "WLAN signal strength in dBm",
        MetricType.GAUGE,
        1.0,
        None,
    ),
    "cellular_rssi": (
        "peblar_cellular_rssi_dbm",
        "Cellular signal strength in dBm",
        MetricType.GAUGE,
        1.0,
        None,
    ),
    "uptime": (
        "peblar_uptime_seconds",
        "System uptime in seconds",
        MetricType.COUNTER,
        1.0,
        None,
    ),
    "phase_count": (
        "peblar_phase_count",
        "Number of connected phases",
        MetricType.GAUGE,
        1.0,
        None,
    ),
    "charge_current_limit_actual": (
        "peblar_charge_current_limit_amperes",
        "Actual charge current communicated to the vehicle in A",
        MetricType.GAUGE,
        0.001,
        None,
    ),
    "cp_state": (
        "peblar_cp_state",
        "Control Pilot state (0=A/no EV, 1=B/suspended, 2=C/charging, "
        "3=D/vent, 4=E/error, 5=F/fault, 6=I/invalid, 7=U/unknown)",
        MetricType.GAUGE,
        1.0,
        None,
    ),
    "lock_state": (
        "peblar_lock_state",
        "Socket lock state (0=unlocked, 1=locked)",
        MetricType.GAUGE,
        1.0,
        None,
    ),
    "charge_current_limit_source": (
        "peblar_charge_current_limit_source",
        "Active charge current limiting source (see Peblar docs for enum values)",
        MetricType.GAUGE,
        1.0,
        None,
    ),
}


class PeblarCollector(BaseCollector):
    """Modbus TCP collector for the Peblar Home EV charger.

    A background task polls the charger every ``poll_interval`` seconds and
    caches the result.  :meth:`collect` returns the cached data instantly so
    that the Prometheus scrape endpoint is never blocked by a Modbus round-trip.
    """

    _METRIC_DEFS: ClassVar[
        dict[str, tuple[str, str, MetricType, float, str | None]]
    ] = _METRIC_DEFS

    def __init__(self, config: PeblarConfig) -> None:
        super().__init__(config.name, config)
        self._config: PeblarConfig = config

        self._client: AsyncModbusTcpClient | None = None
        self._poll_task: asyncio.Task | None = None
        self._last_data: dict[str, float] | None = None
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def host(self) -> str:
        return self._config.host

    @property
    def port(self) -> int:
        return self._config.port

    @property
    def poll_interval(self) -> float:
        return self._config.poll_interval

    @property
    def is_ready(self) -> bool:
        return self._last_data is not None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Open the Modbus TCP connection and start the background poll task."""
        logger.info(
            "[%s] Connecting to Peblar at %s:%d", self.name, self.host, self.port
        )

        self._client = AsyncModbusTcpClient(
            host=self.host,
            port=self.port,
            timeout=self._config.request_timeout,
        )

        connected = await self._client.connect()
        if not connected:
            raise ConnectionError(
                f"[{self.name}] Failed to connect to Peblar at {self.host}:{self.port}"
            )

        # Verify connectivity with a single read before starting the poll loop.
        await self._poll_once()

        self._poll_task = asyncio.create_task(
            self._poll_loop(), name=f"peblar-poll-{self.name}"
        )
        logger.info(
            "[%s] Connected; poll interval %.1f s", self.name, self.poll_interval
        )

    async def disconnect(self) -> None:
        """Cancel the background task and close the Modbus connection."""
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._poll_task
            self._poll_task = None

        if self._client:
            self._client.close()
            self._client = None

        logger.info("[%s] Disconnected", self.name)

    # ------------------------------------------------------------------
    # Background poll loop
    # ------------------------------------------------------------------

    async def _poll_loop(self) -> None:
        """Continuously poll the charger at the configured interval."""
        while True:
            await asyncio.sleep(self.poll_interval)
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                raise
            except ModbusException as exc:
                logger.warning("[%s] Modbus error during poll: %s", self.name, exc)
            except Exception:
                logger.exception("[%s] Unexpected error during poll", self.name)

    async def _poll_once(self) -> None:
        """Read all input registers in one shot and update _last_data."""
        if not self._client or not self._client.connected:
            raise ModbusException(f"[{self.name}] Modbus client is not connected")

        async with self._lock:
            data = await self._read_all()

        self._last_data = data
        logger.debug("[%s] Polled %d fields", self.name, len(data))

    # ------------------------------------------------------------------
    # Low-level Modbus reads
    # ------------------------------------------------------------------

    async def _read_input(self, address: int, count: int) -> list[int]:
        """Read *count* input registers starting at 0-based PDU *address*.

        Raises :class:`ModbusException` on any protocol or device error.
        """
        assert self._client is not None  # noqa: S101
        rr = await self._client.read_input_registers(
            address, count=count, device_id=self._config.slave_id
        )
        if rr.isError():
            raise ModbusException(
                f"[{self.name}] Error reading {count} regs @ PDU {address}: {rr}"
            )
        return list(rr.registers)

    async def _read_all(self) -> dict[str, float]:
        """Read the full set of monitored registers and return a parsed dict."""
        data: dict[str, float] = {}

        # ── Energy meter (30000-30027) ─────────────────────────────────────
        # We read 28 registers in a single request covering 30000-30027.
        regs = await self._read_input(30000, 28)

        # int64: 4 registers each
        data["energy_total"] = float(_to_int64(regs[0], regs[1], regs[2], regs[3]))
        data["session_energy"] = float(_to_int64(regs[4], regs[5], regs[6], regs[7]))

        # int32: 2 registers each; regs index = register_number - 30000
        data["power_l1"] = float(_to_int32(regs[8], regs[9]))
        data["power_l2"] = float(_to_int32(regs[10], regs[11]))
        data["power_l3"] = float(_to_int32(regs[12], regs[13]))
        data["power_total"] = float(_to_int32(regs[14], regs[15]))
        data["voltage_l1"] = float(_to_int32(regs[16], regs[17]))
        data["voltage_l2"] = float(_to_int32(regs[18], regs[19]))
        data["voltage_l3"] = float(_to_int32(regs[20], regs[21]))
        data["current_l1"] = float(_to_int32(regs[22], regs[23]))
        data["current_l2"] = float(_to_int32(regs[24], regs[25]))
        data["current_l3"] = float(_to_int32(regs[26], regs[27]))

        # ── System information (30086-30093) ───────────────────────────────
        # 8 registers: 30086-30093
        sys_regs = await self._read_input(30086, 8)

        data["wlan_rssi"] = float(_to_int32(sys_regs[0], sys_regs[1]))
        data["cellular_rssi"] = float(_to_int32(sys_regs[2], sys_regs[3]))
        data["uptime"] = float(_to_uint32(sys_regs[4], sys_regs[5]))
        data["phase_count"] = float(sys_regs[6])  # uint16, single register

        # ── EV interface (30110-30114) ─────────────────────────────────────
        # Read 5 regs in one request: 30110 CpState, 30111 LockState,
        # 30112 ChargeCurrentLimitSource, 30113-30114 ChargeCurrentLimitActual
        ev_regs = await self._read_input(30110, 5)

        # CpState: ASCII char in the low byte of a uint16
        cp_char = chr(ev_regs[0] & 0xFF)
        data["cp_state"] = _CP_STATE_MAP.get(cp_char, 7.0)  # default to Unknown

        data["lock_state"] = float(ev_regs[1])
        data["charge_current_limit_source"] = float(ev_regs[2])
        data["charge_current_limit_actual"] = float(_to_uint32(ev_regs[3], ev_regs[4]))

        return data

    # ------------------------------------------------------------------
    # Collect
    # ------------------------------------------------------------------

    async def collect(self) -> list[Metric]:
        """Return the latest cached readings as Prometheus metrics.

        Never blocks on I/O — the background task updates the cache independently
        of the scrape cycle.
        """
        if not self._last_data:
            logger.debug("[%s] No data yet", self.name)
            return []

        base_labels = {"source": self.name, "host": self.host, "device_type": "peblar"}
        metrics: list[Metric] = []

        for key, (
            metric_name,
            description,
            metric_type,
            scale,
            phase,
        ) in self._METRIC_DEFS.items():
            raw = self._last_data.get(key)
            if raw is None:
                continue

            labels = {**base_labels, "phase": phase} if phase else base_labels

            metrics.append(
                Metric(
                    name=metric_name,
                    value=raw * scale,
                    labels=labels,
                    description=description,
                    metric_type=metric_type,
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
            "host": self.host,
            "port": self.port,
            "connected": self._client is not None and self._client.connected,
            "has_data": self.is_ready,
            "poll_interval": self.poll_interval,
        }
