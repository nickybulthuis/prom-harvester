from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from harvester.collectors import get_configuration
from harvester.collectors.base import BaseCollector
from harvester.export.http import MetricsServer
from harvester.export.prometheus import PrometheusExporter
from harvester.logging_config import setup_logging
from harvester.settings import get_settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Collector lifecycle helpers
# ---------------------------------------------------------------------------


@asynccontextmanager
async def _managed_collectors(meters) -> AsyncIterator[list[BaseCollector]]:
    """Async context manager that connects all collectors on entry and
    disconnects them on exit, even if an error occurs mid-way.

    Collectors that fail to connect are skipped (logged) rather than
    aborting the whole application.
    """
    collectors: list[BaseCollector] = []

    for meter in meters:
        collector = meter.create_collector()
        try:
            await collector.connect()
            collectors.append(collector)
        except Exception:
            logger.exception("Failed to connect collector %r — skipping", meter.name)

    try:
        yield collectors
    finally:
        for collector in collectors:
            try:
                await collector.disconnect()
            except Exception:
                logger.exception("Error disconnecting %r", collector.name)


def _register_shutdown(event: asyncio.Event) -> None:
    """Wire SIGTERM and SIGINT to *event* so the loop exits cleanly."""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, event.set)


# ---------------------------------------------------------------------------
# Main async entry point
# ---------------------------------------------------------------------------


async def run_application() -> None:
    """Connect collectors, start the metrics server, block until shutdown."""
    settings = get_settings()
    config = get_configuration()

    async with _managed_collectors(config.meters) as collectors:
        if not collectors:
            logger.error("No collectors connected — exiting")
            return

        exporter = PrometheusExporter(
            prefix=settings.metrics_prefix,
            disable_default_metrics=settings.disable_default_metrics,
        )
        server = MetricsServer(
            collectors=collectors,
            exporter=exporter,
            host=settings.app_host,
            port=settings.app_port,
        )
        await server.start()

        shutdown_event = asyncio.Event()
        _register_shutdown(shutdown_event)

        try:
            await shutdown_event.wait()
        except asyncio.CancelledError:
            pass
        finally:
            logger.info("Shutting down…")
            await server.stop()
            logger.info("Shutdown complete")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Parse settings, log startup info, and run the application."""
    setup_logging()

    settings = get_settings()
    config = get_configuration()

    logger.info("Log level   : %s", settings.log_level)
    logger.info("Config file : %s", settings.configuration_file)
    logger.info("Meters (%d) :", len(config.meters))
    for meter in config.meters:
        logger.info("  • %s  [source=%s]", meter.name, meter.source)  # type: ignore[attr-defined]

    asyncio.run(run_application())


if __name__ == "__main__":
    main()
