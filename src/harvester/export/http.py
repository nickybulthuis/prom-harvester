from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from aiohttp import web

from harvester.models import Collector

if TYPE_CHECKING:
    from harvester.export.prometheus import PrometheusExporter

logger = logging.getLogger(__name__)

_COLLECT_TIMEOUT = 10.0  # seconds per collector per scrape

_INDEX_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><title>Prom Harvester</title></head>
<body>
  <h1>Prom Harvester</h1>
  <p>Prometheus metrics collector</p>
  <ul>
    <li><a href="/metrics">/metrics</a> — Prometheus metrics</li>
    <li><a href="/health">/health</a> — Health check</li>
    <li><a href="/ready">/ready</a> — Readiness probe</li>
  </ul>
</body>
</html>
"""


class MetricsServer:
    """Lightweight aiohttp server exposing Prometheus metrics and health endpoints."""

    def __init__(
        self,
        collectors: list[Collector],
        exporter: PrometheusExporter,
        host: str = "127.0.0.1",
        port: int = 9000,
    ) -> None:
        self._collectors = collectors
        self._exporter = exporter
        self._host = host
        self._port = port
        self._runner: web.AppRunner | None = None
        self._is_running = False

    @property
    def is_running(self) -> bool:
        return self._is_running

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Build and start the aiohttp application."""
        if self._is_running:
            logger.warning("Server is already running")
            return

        app = web.Application()
        app.router.add_get("/", self._handle_root)
        app.router.add_get("/metrics", self._handle_metrics)
        app.router.add_get("/health", self._handle_health)
        app.router.add_get("/ready", self._handle_ready)

        self._runner = web.AppRunner(app)
        await self._runner.setup()
        await web.TCPSite(self._runner, self._host, self._port).start()

        self._is_running = True
        logger.info("Metrics server listening on http://%s:%s", self._host, self._port)

    async def stop(self) -> None:
        """Shut down the server and release resources."""
        if not self._is_running:
            return
        if self._runner:
            await self._runner.cleanup()
        self._is_running = False
        logger.info("Metrics server stopped")

    # ------------------------------------------------------------------
    # Handlers
    # ------------------------------------------------------------------

    async def _handle_root(self, _: web.Request) -> web.Response:
        return web.Response(text=_INDEX_HTML, content_type="text/html")

    async def _handle_metrics(self, _: web.Request) -> web.Response:
        """Collect from all collectors concurrently, then render metrics."""
        results = await asyncio.gather(
            *[self._collect_safe(c) for c in self._collectors]
        )
        for metrics in results:
            self._exporter.update_metrics(metrics)

        return web.Response(
            body=self._exporter.get_metrics(),
            content_type="text/plain",
            charset="utf-8",
        )

    async def _handle_health(self, _: web.Request) -> web.Response:
        collectors_status = [
            collector.get_status()
            if hasattr(collector, "get_status")
            else {"name": collector.name, "connected": True}
            for collector in self._collectors
        ]
        return web.json_response({"status": "healthy", "collectors": collectors_status})

    async def _handle_ready(self, _: web.Request) -> web.Response:
        """Return 200 when at least one collector has data, 503 otherwise.

        Uses the ``is_ready`` property from the :class:`~harvester.models.Collector`
        protocol rather than inspecting private attributes.
        """
        ready = any(c.is_ready for c in self._collectors)
        if ready:
            return web.json_response({"status": "ready"})
        return web.json_response({"status": "not ready"}, status=503)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    async def _collect_safe(collector: Collector) -> list:
        """Run a single collector with a timeout, returning ``[]`` on any failure."""
        try:
            return await asyncio.wait_for(collector.collect(), timeout=_COLLECT_TIMEOUT)
        except TimeoutError:
            logger.warning("Timeout collecting from %s", collector.name)
        except Exception:
            logger.exception("Error collecting from %s", collector.name)
        return []
