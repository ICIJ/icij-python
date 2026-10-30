from abc import ABC
from collections.abc import Generator
from typing import Any, ClassVar, Self
from unittest.mock import MagicMock

import pytest
from pydantic import Field

from icij_common.registrable import RegistrableConfig, RegistrableFromConfig
from icij_common.test_utils import fail_if_exception


class _MockedBaseClass(RegistrableFromConfig, ABC):
    pass


@pytest.fixture
def clear_mocked_registry() -> Generator[None]:
    try:
        yield
    finally:
        if _MockedBaseClass in RegistrableFromConfig._registry:
            del RegistrableFromConfig._registry[_MockedBaseClass]


@pytest.mark.usefixtures("clear_mocked_registry")
def test_should_register_class() -> None:
    # Given
    base_class = _MockedBaseClass
    assert not base_class.list_available()

    # When
    @base_class.register("registered")
    class Registered(base_class):
        @classmethod
        def _from_config(cls, config: RegistrableConfig, **extras) -> Self: ...

    # Then
    assert Registered.registered_name == "registered"
    assert base_class.by_name("registered") is Registered
    available = base_class.list_available()
    assert len(available) == 1
    assert available[0] == "registered"


@pytest.mark.usefixtures("clear_mocked_registry")
def test_register_should_raise_for_already_registered() -> None:
    # Given
    base_class = _MockedBaseClass

    @base_class.register("registered")
    class Registered(base_class):
        @classmethod
        def _from_config(cls, config: RegistrableConfig, **extras) -> Self: ...

    # When
    expected = (
        "Cannot register registered as _MockedBaseClass;"
        " name already in use for Registered"
    )
    with pytest.raises(ValueError, match=expected):

        @base_class.register("registered")
        class Other(base_class):
            @classmethod
            def _from_config(cls, config: RegistrableConfig, **extras) -> Self: ...


@pytest.mark.usefixtures("clear_mocked_registry")
def test_should_register_already_registered_with_exist_ok() -> None:
    # Given
    base_class = _MockedBaseClass

    @base_class.register("registered")
    class Registered(base_class):
        @classmethod
        def _from_config(cls, config: RegistrableConfig, **extras) -> Self: ...

    # When
    msg = "Failed to register already registered class with exist_ok"
    with fail_if_exception(msg):

        @base_class.register("registered", exist_ok=True)
        class Other(base_class):
            @classmethod
            def _from_config(cls, config: RegistrableConfig, **extras) -> Self: ...


@pytest.mark.usefixtures("clear_mocked_registry")
def test_resolve_class_name_for_fully_qualified_class() -> None:
    # Given
    fully_qualified = "unittest.mock.MagicMock"

    # When
    registered_cls = _MockedBaseClass.resolve_class_name(fully_qualified)

    # Then
    assert registered_cls is MagicMock


@pytest.mark.usefixtures("clear_mocked_registry")
def test_registrable_from_config() -> None:
    # Given
    base_class = _MockedBaseClass

    class _MockedBaseClassConfig(RegistrableConfig):
        registry_key: ClassVar[str] = Field(frozen=True, default="some_key")
        some_attr: str  # noqa: F821
        some_key: ClassVar[str] = Field(frozen=True, default="registered")

    @base_class.register("registered")
    class Registered(base_class):
        def __init__(self, some_attr: Any) -> None:
            self.some_attr = some_attr

        @classmethod
        def _from_config(cls, config: RegistrableConfig, **extras) -> Self:  # noqa: ARG003
            return cls(some_attr=config.some_attr)

    instance_config = _MockedBaseClassConfig(some_attr="some_value")

    # When
    instance = base_class.from_config(instance_config)

    # Then
    assert isinstance(instance, Registered)

    assert instance.some_attr == "some_value"


def test_serialize_with_registry_key() -> None:
    # Given

    class _MockedBaseClassConfig(RegistrableConfig):
        registry_key: ClassVar[str] = Field(frozen=True, default="some_key")

    class _MockConfig(_MockedBaseClassConfig):
        some_key: ClassVar[str] = Field(frozen=True, default="registered")
        some_attr: str

    instance_config = _MockConfig(some_attr="some_value")

    # When
    serialized = instance_config.model_dump_json(indent=2)

    # Then
    expected = """{
  "some_attr": "some_value",
  "some_key": "registered"
}"""
    assert serialized == expected
