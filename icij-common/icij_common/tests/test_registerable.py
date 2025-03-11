# pylint: disable=redefined-outer-name
from __future__ import annotations

from abc import ABC
from typing import ClassVar

import pytest
from pydantic import Field

from icij_common.registrable import RegistrableConfig, RegistrableFromConfig
from icij_common.test_utils import fail_if_exception


class _MockedBaseClass(RegistrableFromConfig, ABC):
    pass


@pytest.fixture()
def clear_mocked_registry():
    # pylint: disable=protected-access
    try:
        yield
    finally:
        if _MockedBaseClass in RegistrableFromConfig._registry:
            del RegistrableFromConfig._registry[_MockedBaseClass]


def test_should_register_class(
    clear_mocked_registry,  # pylint: disable=unused-argument
):
    # Given
    base_class = _MockedBaseClass
    assert not base_class.list_available()

    # When
    @base_class.register("registered")
    class Registered(base_class):
        @classmethod
        def _from_config(cls, config: RegistrableConfig, **extras) -> Registered: ...

    # Then
    assert base_class.by_name("registered") is Registered
    available = base_class.list_available()
    assert len(available) == 1
    assert available[0] == "registered"


def test_register_should_raise_for_already_registered(
    clear_mocked_registry,  # pylint: disable=unused-argument
):
    # Given
    base_class = _MockedBaseClass

    @base_class.register("registered")
    class Registered(base_class):  # pylint: disable=unused-variable
        @classmethod
        def _from_config(cls, config: RegistrableConfig, **extras) -> Registered: ...

    # When
    expected = (
        "Cannot register registered as _MockedBaseClass;"
        " name already in use for Registered"
    )
    with pytest.raises(ValueError, match=expected):

        @base_class.register("registered")
        class Other(base_class):  # pylint: disable=unused-variable
            @classmethod
            def _from_config(cls, config: RegistrableConfig, **extras) -> Other: ...


def test_should_register_already_registered_with_exist_ok(
    clear_mocked_registry,  # pylint: disable=unused-argument
):
    # Given
    base_class = _MockedBaseClass

    @base_class.register("registered")
    class Registered(base_class):  # pylint: disable=unused-variable
        @classmethod
        def _from_config(cls, config: RegistrableConfig, **extras) -> Registered: ...

    # When
    msg = "Failed to register already registered class with exist_ok"
    with fail_if_exception(msg):

        @base_class.register("registered", exist_ok=True)
        class Other(base_class):  # pylint: disable=unused-variable
            @classmethod
            def _from_config(cls, config: RegistrableConfig, **extras) -> Other: ...


def test_resolve_class_name_for_fully_qualified_class(
    clear_mocked_registry,  # pylint: disable=unused-argument
):
    # Given
    fully_qualified = "unittest.mock.MagicMock"

    # When
    registered_cls = _MockedBaseClass.resolve_class_name(fully_qualified)

    # Then
    from unittest.mock import MagicMock

    assert registered_cls is MagicMock


def test_registrable_from_config(
    clear_mocked_registry,  # pylint: disable=unused-argument
):
    # Given
    base_class = _MockedBaseClass

    class _MockedBaseClassConfig(RegistrableConfig):
        registry_key: ClassVar[str] = Field(frozen=True, default="some_key")
        some_attr: str
        some_key: ClassVar[str] = Field(frozen=True, default="registered")

    @base_class.register("registered")
    class Registered(base_class):
        def __init__(self, some_attr):
            self.some_attr = some_attr

        @classmethod
        def _from_config(cls, config: RegistrableConfig, **extras) -> Registered:
            return cls(some_attr=config.some_attr)

    instance_config = _MockedBaseClassConfig(some_attr="some_value")

    # When
    instance = base_class.from_config(instance_config)

    # Then
    assert isinstance(instance, Registered)

    assert instance.some_attr == "some_value"
