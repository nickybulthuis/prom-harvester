"""Configuration loading and validation.

The :class:`Configuration` model uses
:func:`~harvester.collectors.registry.build_meter_config_union`
to assemble the ``meters`` field type dynamically from whatever collector
modules have been registered.  This means ``configuration.py`` never needs to
import individual collector classes directly.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml
from pydantic import Field, ValidationError, field_validator

from harvester.collectors.registry import build_meter_config_union
from harvester.models import StrictModel

# ---------------------------------------------------------------------------
# Duration helpers (used by interval-based collectors)
# ---------------------------------------------------------------------------

_DURATION_RE = re.compile(r"^\d+(\.\d+)?(ms|s|m|h)$")
_DURATION_EXAMPLES = "500ms, 10s, 1m, 1h"


def parse_duration(value: object) -> str | None:
    """Validate and normalise a duration string (e.g. ``"10s"``).

    Returns the stripped string, or ``None`` if *value* is ``None``.
    Raises ``TypeError`` / ``ValueError`` on bad input.
    """
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"Duration must be a string, got {type(value).__name__}")
    cleaned = value.strip()
    if _DURATION_RE.fullmatch(cleaned):
        return cleaned
    raise ValueError(
        f"Invalid duration {value!r}. Expected format like {_DURATION_EXAMPLES}."
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _find_duplicates[T](items: list[T], key: callable) -> set[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for item in items:
        value = key(item)
        (duplicates if value in seen else seen).add(value)
    return duplicates


# ---------------------------------------------------------------------------
# Configuration model
# ---------------------------------------------------------------------------


class Configuration(StrictModel):
    """Root configuration model, parsed from the YAML file.

    The ``meters`` field type is built dynamically from the collector registry
    so this class never needs to know about concrete collector implementations.
    """

    # build_meter_config_union() is evaluated at class-definition time.
    # The collectors/__init__.py import chain ensures all @register decorators
    # have already run by the time Configuration is used.
    meters: list[build_meter_config_union()] = Field(..., min_length=1)  # type: ignore[valid-type]

    @field_validator("meters")
    @classmethod
    def _unique_names(cls, meters: list) -> list:
        dupes = _find_duplicates(meters, key=lambda m: m.name)
        if dupes:
            raise ValueError(f"Duplicate meter names: {sorted(dupes)}")
        return meters


# ---------------------------------------------------------------------------
# Error type
# ---------------------------------------------------------------------------


class ConfigurationError(Exception):
    """Raised when the configuration file cannot be loaded or is invalid."""


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------


def load_config(path: str | Path) -> Configuration:
    """Load, parse, and validate a YAML configuration file.

    Raises:
        FileNotFoundError: if *path* does not exist.
        ConfigurationError: if the YAML is malformed or fails Pydantic validation.
    """
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigurationError(f"Invalid YAML in {path}: {exc}") from exc

    if raw is None:
        raise ConfigurationError(f"Configuration file is empty: {path}")
    if not isinstance(raw, dict):
        raise ConfigurationError(
            f"Configuration must be a YAML mapping, got {type(raw).__name__}"
        )

    try:
        return Configuration.model_validate(raw)
    except ValidationError as exc:
        raise ConfigurationError(f"Configuration validation failed:\n{exc}") from exc
