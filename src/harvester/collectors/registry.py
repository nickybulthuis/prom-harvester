"""Collector registry.

Every concrete meter-config class calls :func:`register` (via the decorator)
so that :func:`build_meter_config_union` can assemble the Pydantic discriminated
union automatically.  Adding a new collector type is a single-file change:

1. Create ``harvester/collectors/mydevice.py``
2. Decorate the config class with ``@register``
3. Import the module somewhere before :func:`build_meter_config_union` is called
   (the ``collectors/__init__.py`` is the natural place).
"""

from __future__ import annotations

from typing import Annotated, Union

from pydantic import Field

from harvester.models import BaseMeterConfig

# Ordered dict preserves registration order (Python 3.7+)
_REGISTRY: dict[str, type[BaseMeterConfig]] = {}


def register(cls: type[BaseMeterConfig]) -> type[BaseMeterConfig]:
    """Class decorator that registers a meter-config class by its ``source`` value.

    Example::

        @register
        class MyDeviceConfig(BaseMeterConfig):
            source: Literal["my_device"] = "my_device"
            ...
    """
    source_field = cls.model_fields.get("source")
    if source_field is None:
        raise TypeError(f"{cls.__name__} must declare a 'source' field")

    # Pydantic stores the default as the discriminator value
    source_value: str = source_field.default
    if source_value in _REGISTRY:
        raise ValueError(
            f"Collector source {source_value!r} is already registered "
            f"by {_REGISTRY[source_value].__name__}"
        )

    _REGISTRY[source_value] = cls
    return cls


def build_meter_config_union() -> type:
    """Build a Pydantic-compatible discriminated union of all registered configs.

    Returns the ``Annotated[Union[...], Field(discriminator="source")]`` type
    that should be used as the element type of ``Configuration.meters``.

    Raises:
        RuntimeError: if no collectors have been registered yet.
    """
    if not _REGISTRY:
        raise RuntimeError(
            "No collector types registered. "
            "Make sure all collector modules are imported before calling "
            "build_meter_config_union()."
        )

    types = tuple(_REGISTRY.values())
    union = Union[types]  # noqa UP007
    return Annotated[union, Field(discriminator="source")]
