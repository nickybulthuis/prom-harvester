from __future__ import annotations

import logging
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from harvester.settings import LogLevel

_LEVEL_MAP: dict[str, int] = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
}

_LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# frozenset — unordered, no duplicates, communicates intent clearly
_QUIET_LOGGERS: frozenset[str] = frozenset(
    {
        "urllib3",
        "httpx",
        "asyncio",
        "aiohttp",
        "websockets",
        "nicegui",
    }
)


def setup_logging(level: LogLevel | None = None) -> None:
    """Configure root logging.

    Args:
        level: Override the log level from :func:`~harvester.settings.get_settings`.
               Useful when calling from tests.
    """
    # Local import — avoids a module-level circular dependency with settings.py
    from harvester.settings import get_settings  # noqa: PLC0415

    if level is None:
        level = get_settings().log_level

    numeric_level = _LEVEL_MAP.get(level, logging.INFO)

    logging.basicConfig(
        level=numeric_level,
        format=_LOG_FORMAT,
        datefmt=_DATE_FORMAT,
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,  # safe to call multiple times (e.g. in tests)
    )

    # In debug mode let all loggers through; otherwise silence noisy libs.
    noise_level = logging.DEBUG if numeric_level == logging.DEBUG else logging.WARNING
    for name in _QUIET_LOGGERS:
        logging.getLogger(name).setLevel(noise_level)
