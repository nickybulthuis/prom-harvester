"""YouLess LS120 collector.

Polls ``GET /e?f=j`` on the Enelogic firmware on a **background timer** and
caches the result.  :meth:`collect` returns the cached data instantly so that
the ``/metrics`` scrape endpoint is never blocked by a network round-trip.

Typical ``/e?f=j`` response
----------------------------
.. code-block:: json

    {
        "tm":  1709739737,
        "net": "31777,244",
        "pwr": 1080,
        "p1":  "18728,542",
        "p2":  "13048,716",
        "n1":  "0,008",
        "n2":  "0,006",
        "gas": "5444,593",
        "gts": 1711032100,
        "ts0": 1509738000,
        "cs0": "0,000",
        "ps0": 0
    }

Field reference
---------------
- ``tm``  -- Unix timestamp of the last P1 telegram.
- ``net`` -- Net energy (import - export) in kWh; negative = net exporter.
- ``pwr`` -- Current power in Watts (negative = feeding to grid).
- ``p1``  -- Total energy imported tariff 1 (off-peak) in kWh.
- ``p2``  -- Total energy imported tariff 2 (peak) in kWh.
- ``n1``  -- Total energy exported tariff 1 (off-peak) in kWh.
- ``n2``  -- Total energy exported tariff 2 (peak) in kWh.
- ``gas`` -- Total gas usage in m3.
- ``gts`` -- Timestamp of the last gas measurement.
- ``ts0`` -- Timestamp of the last S0 pulse.
- ``cs0`` -- S0 counter value in kWh (solar / extra meter).
- ``ps0`` -- S0 current power in Watts.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import ClassVar, Literal

import aiohttp
from pydantic import Field

from harvester.collectors.base import BaseCollector
from harvester.collectors.registry import register
from harvester.models import BaseMeterConfig, Metric, MetricType

logger = logging.getLogger(__name__)

_DECIMAL_SEP = ","


def _parse_float(value: object) -> float | None:
    """Convert a YouLess numeric value to float.

    Handles both plain int/float and comma-decimal strings like "31777,244".
    Returns None when the value is absent or unparseable.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip().replace(_DECIMAL_SEP, "."))
        except ValueError:
            return None
    return None


@register
class YoulessConfig(BaseMeterConfig):
    """Configuration for a YouLess LS120 energy monitor."""

    source: Literal["youless"] = "youless"
    host: str = Field(
        ..., min_length=1, description="Hostname or IP of the YouLess device"
    )
    password: str | None = Field(
        default=None,
        description="Device password (leave empty if no password is set on the device)",
    )
    port: int = Field(default=80, ge=1, le=65535, description="HTTP port")
    request_timeout: float = Field(
        default=10.0, gt=0, description="Per-request HTTP timeout in seconds"
    )
    poll_interval: float = Field(
        default=10.0,
        gt=0,
        description=(
            "How often (in seconds) the background task fetches fresh data. "
            "The YouLess P1 telegram interval is typically 10 s, so values "
            "below that will not yield fresher energy readings."
        ),
    )

    def create_collector(self) -> YoulessCollector:
        return YoulessCollector(config=self)


class YoulessCollector(BaseCollector):
    """Maintain a background polling loop against the YouLess /e?f=j endpoint.

    A background asyncio.Task fetches new data every poll_interval seconds.
    collect() reads from the in-memory cache and returns immediately -- the
    /metrics scrape is never blocked by a network round-trip to the device.
    """

    _METRIC_DEFS: ClassVar[dict[str, tuple[str, str, MetricType]]] = {
        "pwr": (
            "youless_active_power_watts",
            "Current power consumption/production in watts "
            "(negative = feeding to grid)",
            MetricType.GAUGE,
        ),
        "net": (
            "youless_net_energy_kwh",
            "Net energy (import - export) in kWh",
            MetricType.GAUGE,
        ),
        "p1": (
            "youless_energy_import_t1_kwh",
            "Total energy imported tariff 1 (off-peak) in kWh",
            MetricType.COUNTER,
        ),
        "p2": (
            "youless_energy_import_t2_kwh",
            "Total energy imported tariff 2 (peak) in kWh",
            MetricType.COUNTER,
        ),
        "n1": (
            "youless_energy_export_t1_kwh",
            "Total energy exported tariff 1 (off-peak) in kWh",
            MetricType.COUNTER,
        ),
        "n2": (
            "youless_energy_export_t2_kwh",
            "Total energy exported tariff 2 (peak) in kWh",
            MetricType.COUNTER,
        ),
        "gas": (
            "youless_gas_total_m3",
            "Total gas usage in m3",
            MetricType.COUNTER,
        ),
        "cs0": (
            "youless_s0_energy_kwh",
            "S0 pulse counter energy in kWh (e.g. solar panels)",
            MetricType.COUNTER,
        ),
        "ps0": (
            "youless_s0_power_watts",
            "S0 current power in watts",
            MetricType.GAUGE,
        ),
    }

    def __init__(self, config: YoulessConfig) -> None:
        super().__init__(config.name, config)
        self.host = config.host
        self.port = config.port
        self.password = config.password
        self.poll_interval = config.poll_interval
        self.request_timeout = aiohttp.ClientTimeout(total=config.request_timeout)

        self._session: aiohttp.ClientSession | None = None
        self._last_data: dict = {}
        self._poll_task: asyncio.Task | None = None

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    @property
    def metrics_url(self) -> str:
        return f"{self.base_url}/e?f=j"

    @property
    def is_ready(self) -> bool:
        """True once the background task has completed at least one fetch."""
        return bool(self._last_data)

    async def connect(self) -> None:
        """Open the HTTP session and start the background polling task.

        Performs one blocking fetch first so a bad host or wrong password
        surfaces immediately at startup rather than silently at the first scrape.
        """
        logger.info("[%s] Connecting to %s", self.name, self.base_url)

        auth = aiohttp.BasicAuth("", self.password) if self.password else None
        self._session = aiohttp.ClientSession(
            auth=auth,
            timeout=self.request_timeout,
            headers={"Accept": "application/json"},
        )

        self._last_data = await self._fetch()

        self._poll_task = asyncio.create_task(
            self._poll_loop(), name=f"{self.name}-poll"
        )
        logger.info(
            "[%s] Connected -- polling every %.1fs", self.name, self.poll_interval
        )

    async def disconnect(self) -> None:
        """Stop the background task and close the HTTP session."""
        logger.info("[%s] Disconnecting", self.name)

        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._poll_task
            self._poll_task = None

        if self._session:
            await self._session.close()
            self._session = None

        logger.info("[%s] Disconnected", self.name)

    async def _poll_loop(self) -> None:
        """Fetch fresh data every poll_interval seconds."""
        while True:
            await asyncio.sleep(self.poll_interval)
            try:
                self._last_data = await self._fetch()
                logger.debug("[%s] Data refreshed", self.name)
            except asyncio.CancelledError:
                break
            except aiohttp.ClientResponseError as exc:
                logger.exception(
                    "[%s] HTTP %s fetching %s -- retrying in %.1fs",
                    self.name,
                    exc.status,
                    self.metrics_url,
                    self.poll_interval,
                )
            except aiohttp.ClientError:
                logger.exception(
                    "[%s] Request failed -- retrying in %.1fs",
                    self.name,
                    self.poll_interval,
                )

    async def _fetch(self) -> dict:
        """GET /e?f=j and return the parsed JSON dict.

        The YouLess returns a single-element JSON array, e.g. ``[{...}]``.
        We unwrap it and return the inner object.
        """
        if not self._session:
            raise RuntimeError(
                f"[{self.name}] Session not initialised -- call connect() first"
            )

        async with self._session.get(self.metrics_url) as response:
            response.raise_for_status()
            payload = await response.json(content_type=None)

        if not isinstance(payload, list) or not payload:
            raise ValueError(f"[{self.name}] Unexpected response format: {payload!r}")

        return payload[0]

    async def collect(self) -> list[Metric]:
        """Return the latest cached data as Prometheus metrics.

        This method never blocks on I/O -- the background task keeps
        _last_data up to date independently of the scrape cycle.
        """
        if not self._last_data:
            logger.debug("[%s] No data yet", self.name)
            return []

        base_labels = {"source": self.name, "host": self.host, "device_type": "youless"}
        metrics: list[Metric] = []

        for key, (metric_name, description, metric_type) in self._METRIC_DEFS.items():
            raw = self._last_data.get(key)
            value = _parse_float(raw)
            if value is None:
                if raw is not None:
                    logger.warning(
                        "[%s] Cannot parse field %s=%r as float", self.name, key, raw
                    )
                continue

            metrics.append(
                Metric(
                    name=metric_name,
                    value=value,
                    labels=base_labels,
                    description=description,
                    metric_type=metric_type,
                )
            )

        logger.debug("[%s] Collected %d metrics", self.name, len(metrics))
        return metrics

    def get_status(self) -> dict:
        """Return a status dict for the /health endpoint."""
        return {
            "name": self.name,
            "host": self.host,
            "connected": self._session is not None and not self._session.closed,
            "has_data": self.is_ready,
            "poll_interval": self.poll_interval,
        }
