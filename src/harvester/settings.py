from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Shared type alias — imported by logging_config and anywhere else that
# needs to reference a valid log-level string.
LogLevel = Literal["debug", "info", "warning", "error", "critical"]


class Settings(BaseSettings):
    """Application settings.

    Every field can be overridden via a ``HARVESTER_*`` environment variable
    or a ``.env`` file in the working directory.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="harvester_",
        case_sensitive=False,
    )

    # Paths
    configuration_file: str = Field(
        default="./configuration.yaml",
        description="Path to the YAML configuration file",
    )

    # HTTP server
    app_host: str = Field(
        default="127.0.0.1", description="Bind host for the metrics server"
    )
    app_port: int = Field(
        default=9000, ge=1, le=65535, description="Bind port for the metrics server"
    )

    # Prometheus export
    metrics_prefix: str = Field(
        default="",
        description="Optional prefix prepended to every metric name (e.g. 'mysite')",
    )
    disable_default_metrics: bool = Field(
        default=True,
        description="Strip the default prometheus-client "
        "process/GC metrics from output",
    )

    # Logging
    log_level: LogLevel = Field(default="info", description="Root log level")

    @field_validator("metrics_prefix")
    @classmethod
    def _validate_prefix(cls, v: str) -> str:
        if v and not v.replace("_", "").isalnum():
            raise ValueError(
                "metrics_prefix must be alphanumeric with underscores only"
            )
        return v.lower()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the cached :class:`Settings` singleton."""
    return Settings()
