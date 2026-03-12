from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel as _PydanticBase
from pydantic import ConfigDict, Field

if TYPE_CHECKING:
    from harvester.models.metric import Collector


class StrictModel(_PydanticBase):
    """Pydantic base model with strict, whitespace-stripping settings."""

    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid",
        use_enum_values=True,
        str_strip_whitespace=True,
    )


class BaseMeterConfig(StrictModel):
    """Common fields shared by every meter configuration block.

    Subclasses **must** implement :meth:`create_collector` — this is enforced
    at class-definition time via ``__init_subclass__``.
    """

    name: str = Field(..., min_length=1, description="Unique meter identifier")

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        if "create_collector" not in cls.__dict__:
            raise TypeError(
                f"{cls.__name__} must implement create_collector(). "
                "Add a concrete 'def create_collector(self) -> Collector:' method."
            )

    def create_collector(self) -> Collector:  # pragma: no cover
        raise NotImplementedError
