"""
Simplified implementation of AllenNLP Registrable:
https://github.com/allenai/allennlp
"""

from __future__ import annotations

import json
import logging
import os

from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Any, Callable, ClassVar, DefaultDict, TypeVar, cast

from pydantic import (
    BaseModel,
    Field,
    ModelWrapValidatorHandler,
    model_serializer,
    model_validator,
)
from pydantic.fields import FieldInfo
from pydantic_core.core_schema import SerializationInfo, SerializerFunctionWrapHandler
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing_extensions import Self

from icij_common.import_utils import VariableNotFound, import_variable
from icij_common.pydantic_utils import icij_config, merge_configs

logger = logging.getLogger(__name__)

_RegistrableT = TypeVar("_RegistrableT", bound="Registrable")
_SubclassRegistry = dict[str, _RegistrableT]
_SubclssNames = dict[_SubclassRegistry, str]

T = TypeVar("T", bound="FromConfig")


class RegistrableMixin(ABC):
    _registry: ClassVar[DefaultDict[type, _SubclassRegistry]] = defaultdict(dict)
    _registered_names: ClassVar[DefaultDict[type, _SubclssNames]] = defaultdict(dict)

    @classmethod
    def register(
        cls, name: str | None = None, exist_ok: bool = False
    ) -> Callable[[type[RegistrableMixin]], type[RegistrableMixin]]:
        # pylint: disable=protected-access
        registry = RegistrableMixin._registry[cls]
        registered_names = RegistrableMixin._registered_names[cls]

        def add_subclass_to_registry(
            subclass: type[RegistrableMixin],
        ) -> type[RegistrableMixin]:
            registered_name = name
            if registered_name is None:
                registered_key = subclass.model_fields.get(
                    subclass.registry_key.default
                )
                if registered_key is None:
                    raise ValueError(
                        "no name provided and the class doesn't define a registry key"
                    )
                registered_name = registered_key.default

            if registered_name in registry:
                if exist_ok:
                    msg = (
                        f"{registered_name} has already been registered as "
                        f"{registry[registered_name].__name__}, but exist_ok=True, "
                        f"so overwriting with {cls.__name__}"
                    )
                    logger.info(msg)
                else:
                    msg = (
                        f"Cannot register {registered_name} as {cls.__name__}; "
                        f"name already in use for {registry[registered_name].__name__}"
                    )
                    raise ValueError(msg)
            registry[registered_name] = subclass
            registered_names[subclass] = registered_name
            return subclass

        return add_subclass_to_registry

    @classmethod
    def by_name(cls: type[_RegistrableT], name: str) -> Callable[..., _RegistrableT]:
        logger.debug("instantiating registered subclass %s of %s", name, cls)
        subclass = cls.resolve_class_name(name)
        return cast(type[_RegistrableT], subclass)

    @classmethod
    def resolve_class_name(cls: type[_RegistrableT], name: str) -> type[_RegistrableT]:
        # pylint: disable=protected-access
        sub_registry = RegistrableMixin._registry.get(cls, None)
        if sub_registry is None:
            for k, v in RegistrableMixin._registry.items():
                if issubclass(cls, k):
                    sub_registry = v
                    break
        if sub_registry is not None:
            subclass = sub_registry.get(name, None)
            if subclass is not None:
                return subclass
        if "." in name:
            try:
                subclass = import_variable(name)
            except ModuleNotFoundError as e:
                raise ValueError(
                    f"tried to interpret {name} as a path to a class "
                    f"but unable to import module {'.'.join(name.split('.')[:-1])}"
                ) from e
            except VariableNotFound as e:
                split = name.split(".")
                raise ValueError(
                    f"tried to interpret {name} as a path to a class "
                    f"but unable to find class {split[-1]} in {split[:-1]}"
                ) from e
            return subclass
        available = "\n-".join(cls.list_available())
        msg = f"""{name} is not a registered name for '{cls.__name__}'.
Available names are:
{available}

If your registered class comes from custom code, you'll need to import the\
 corresponding modules and use fully-qualified paths: "my_module.submodule.MyClass"
"""
        raise ValueError(msg)

    @classmethod
    def list_available(cls) -> list[str]:
        # pylint: disable=protected-access
        keys = list(RegistrableMixin._registry[cls].keys())
        return keys

    @classmethod
    @property
    def registered_name(cls) -> str:
        for (
            names
        ) in (
            RegistrableMixin._registered_names.values()
        ):  # pylint: disable=protected-access
            name = names.get(cls)
            if name is not None:
                return name
        raise ValueError("registration inconsistency")


class RegistrableConfig(BaseModel, RegistrableMixin):
    registry_key: ClassVar[str] = Field(frozen=True, default="name")

    @model_validator(mode="wrap")
    @classmethod
    def deserialize_with_registry_key(
        cls, value: Any, handler: ModelWrapValidatorHandler[Self]
    ) -> Self:
        if isinstance(value, dict):
            if cls in cls._registry:
                subcls = cls.resolve_class_name(value[cls.registry_key.default])
                value = {
                    k: v for k, v in value.items() if k != cls.registry_key.default
                }
                return subcls.model_validate(value)
            if cls.registry_key.default in value:
                value = {
                    k: v for k, v in value.items() if k != cls.registry_key.default
                }
            return handler(value)
        return handler(value)


class RegistrableSettings(RegistrableConfig, BaseSettings):
    model_config = merge_configs(
        SettingsConfigDict(
            env_prefix="", env_nested_delimiter="__", validate_default=True
        )
    )

    @classmethod
    def from_env(cls):
        registry_key = cls.registry_key.default
        env_key = f"{cls.model_config['env_prefix']}{registry_key}"
        _, name = find_variable_loc_in_env(env_key, cls.model_config["case_sensitive"])
        config_cls = cls.resolve_class_name(name)
        return config_cls()


class FromConfig(ABC):
    @classmethod
    @abstractmethod
    def _from_config(cls, config: RegistrableConfig, **extras) -> FromConfig: ...


class RegistrableFromConfig(RegistrableMixin, FromConfig, ABC):
    @classmethod
    def from_config(cls, config: RegistrableConfig, **extras) -> Self:
        name = getattr(config, config.registry_key.default)
        if isinstance(name, FieldInfo):
            name = name.default
        subcls = cls.resolve_class_name(name)
        return subcls._from_config(config, **extras)  # pylint: disable=protected-access


def find_variable_loc_in_env(variable: str, case_sensitive: bool) -> tuple[str, str]:
    if case_sensitive:
        try:
            return variable, os.environ[variable]
        except KeyError as e:
            raise ValueError(f"couldn't find {variable} in env variables") from e
    lowercase = variable.lower()
    for k, v in os.environ.items():
        if k.lower() == lowercase:
            return k, v
    raise ValueError(f"couldn't find {variable.upper()} in env variables")


def find_in_env(variable: str, case_sensitive: bool) -> str:
    return find_variable_loc_in_env(variable, case_sensitive)[1]


class Registrable(BaseModel, RegistrableMixin, ABC):
    model_config = icij_config()

    registry_key: ClassVar[str] = Field(frozen=True, default="@type")

    @classmethod
    def model_validate_json(
        cls,
        json_data: str | bytes | bytearray,
        *,
        strict: bool | None = None,
        context: Any | None = None,
    ) -> Self:
        if isinstance(json_data, bytes):
            data = json.loads(json_data.decode("utf-8"))
        elif isinstance(json_data, str):
            data = json.loads(json_data)
        elif isinstance(json_data, bytearray):
            data = json.loads(str(json_data, encoding="utf-8"))
        else:
            raise TypeError(
                f"unsupported type {type(json_data)}, expected bytes, str or bytearray"
            )
        key = data[cls.registry_key.default]
        subcls = cls.resolve_class_name(key)
        # TODO: try to use model_validate_json instead
        return subcls.model_validate(data, strict=strict, context=context)

    @model_serializer(mode="wrap")
    def serialize_with_registry_key(
        self, nxt: SerializerFunctionWrapHandler, info: SerializationInfo
    ) -> dict[str, Any]:
        serialized = nxt(self)
        include_key = bool(info.include) and self.registry_key.default in info.include
        include_key = include_key or not (
            bool(info.exclude) and self.registry_key.default in info.exclude
        )
        if include_key:
            serialized[self.registry_key.default] = self.registered_name
        return serialized

    @model_validator(mode="wrap")
    @classmethod
    def deserialize_with_registry_key(
        cls, value: Any, handler: ModelWrapValidatorHandler[Self]
    ) -> Self:
        if isinstance(value, dict):
            if cls in cls._registry:
                subcls = cls.resolve_class_name(value[cls.registry_key.default])
                value = {
                    k: v for k, v in value.items() if k != cls.registry_key.default
                }
                return subcls.model_validate(value)
            if cls.registry_key.default in value:
                value = {
                    k: v for k, v in value.items() if k != cls.registry_key.default
                }
            return handler(value)
        return handler(value)
