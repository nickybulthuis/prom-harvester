"""Collectors sub-package.

Importing this package has a deliberate side-effect: it imports every known
collector module, which causes their ``@register`` decorators to run and
populate the registry before :func:`build_meter_config_union` is called.

To add a new collector:
1. Create ``harvester/collectors/mydevice.py`` with a ``@register`` config class.
2. Add ``from harvester.collectors import mydevice as _mydevice`` below.
"""

from __future__ import annotations

import logging
from functools import lru_cache

# --- Trigger @register side-effects for all known collector modules ---
from harvester.collectors import homewizard as _homewizard  # noqa: F401
from harvester.configuration import Configuration, load_config
from harvester.settings import get_settings

logger = logging.getLogger(__name__)

__all__ = ["Configuration", "get_configuration", "reload_configuration"]


@lru_cache(maxsize=1)
def get_configuration() -> Configuration:
    """Return the cached :class:`~harvester.configuration.Configuration` singleton.

    Raises :class:`~harvester.configuration.ConfigurationError` on invalid config.
    """
    settings = get_settings()
    logger.info("Loading configuration from: %s", settings.configuration_file)
    config = load_config(settings.configuration_file)
    logger.info("Configuration loaded: %d meter(s)", len(config.meters))
    return config


def reload_configuration() -> Configuration:
    """Invalidate the cache and reload configuration from disk."""
    get_configuration.cache_clear()
    return get_configuration()
