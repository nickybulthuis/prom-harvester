from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Self

from harvester.models import Metric

if TYPE_CHECKING:
    from harvester.models import BaseMeterConfig


class BaseCollector(ABC):
    """Abstract base class for all metric collectors.

    Subclasses implement :meth:`connect`, :meth:`disconnect`, and
    :meth:`collect`.  The class also works as an **async context manager**,
    so callers can write::

        async with SomeCollector(config) as collector:
            metrics = await collector.collect()
    """

    def __init__(self, name: str, config: BaseMeterConfig) -> None:
        self.name = name
        self.config = config

    # ------------------------------------------------------------------
    # Async context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.disconnect()

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    async def connect(self) -> None:
        """Establish the connection to the data source."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Tear down the connection to the data source."""

    @abstractmethod
    async def collect(self) -> list[Metric]:
        """Collect and return the latest metrics."""

    @property
    @abstractmethod
    def is_ready(self) -> bool:
        """Return ``True`` once the collector has received its first data point.

        Used by the readiness probe — avoids the server reaching into private
        ``_latest_data`` attributes.
        """
