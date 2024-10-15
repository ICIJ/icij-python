"""
Simplified implementation of AllenNLP Registrable:
https://github.com/allenai/allennlp
"""

from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from collections import defaultdict
from pathlib import Path
from typing import (
    Callable,
    ClassVar,
    DefaultDict,
    Dict,
    List,
    Optional,
    Protocol,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from pydantic import BaseModel, Field
from pydantic.parse import load_file
from typing_extensions import Self

from icij_common.pydantic_utils import ICIJSettings
from icij_worker.utils.imports import VariableNotFound, import_variable

logger = logging.getLogger(__name__)

_RegistrableT = TypeVar("_RegistrableT", bound="Registrable")
_SubclassRegistry = Dict[str, _RegistrableT]
_SubclssNames = Dict[_SubclassRegistry, str]

T = TypeVar("T", bound="FromConfig")


class RegistrableMixin(ABC):
    _registry: ClassVar[DefaultDict[type, _SubclassRegistry]] = defaultdict(dict)
    _registered_names: ClassVar[DefaultDict[type, _SubclssNames]] = defaultdict(dict)

    default_implementation: Optional[str] = None

    @classmethod
    def register(
        cls, name: Optional[str] = None, exist_ok: bool = False
    ) -> Callable[[Type[RegistrableMixin]], Type[RegistrableMixin]]:
        # pylint: disable=protected-access
        registry = RegistrableMixin._registry[cls]
        registered_names = RegistrableMixin._registered_names[cls]

        def add_subclass_to_registry(
            subclass: Type[RegistrableMixin],
        ) -> Type[RegistrableMixin]:
            registered_name = name
            if registered_name is None:
                registered_key = subclass.registry_key.default
                if registered_key is None:
                    raise ValueError(
                        "no name provided and the class doesn't define a registry key"
                    )
                registered_name = getattr(subclass, registered_key).default

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
    def by_name(cls: Type[_RegistrableT], name: str) -> Callable[..., _RegistrableT]:
        logger.debug("instantiating registered subclass %s of %s", name, cls)
        subclass = cls.resolve_class_name(name)
        return cast(Type[_RegistrableT], subclass)

    @classmethod
    def resolve_class_name(cls: Type[_RegistrableT], name: str) -> Type[_RegistrableT]:
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
    def list_available(cls) -> List[str]:
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
    registry_key: ClassVar[str] = Field(const=True, default="name")

    @classmethod
    def parse_file(
        cls,
        path: Union[str, Path],
        *,
        content_type: str = None,
        encoding: str = "utf8",
        proto: Protocol = None,
        allow_pickle: bool = False,
    ) -> RegistrableConfig:
        obj = load_file(
            path,
            proto=proto,
            content_type=content_type,
            encoding=encoding,
            allow_pickle=allow_pickle,
            json_loads=cls.__config__.json_loads,
        )
        key = obj.pop(cls.registry_key.default)
        subcls = cls.resolve_class_name(key)
        return subcls(**obj)


class RegistrableSettings(RegistrableConfig, ICIJSettings):

    @classmethod
    def from_env(cls):
        key = cls.registry_key.default
        if cls.__config__.env_prefix is not None:
            key = cls.__config__.env_prefix + key
        registry_key = find_in_env(key, cls.__config__.case_sensitive)
        subcls = cls.resolve_class_name(registry_key)
        return subcls()


class FromConfig(ABC):
    @classmethod
    @abstractmethod
    def _from_config(cls, config: RegistrableConfig, **extras) -> FromConfig: ...


class RegistrableFromConfig(RegistrableMixin, FromConfig, ABC):
    @classmethod
    def from_config(cls, config: RegistrableConfig, **extras) -> Self:
        name = getattr(config, config.registry_key.default).default
        subcls = cls.resolve_class_name(name)
        return subcls._from_config(config, **extras)  # pylint: disable=protected-access


def find_variable_loc_in_env(variable: str, case_sensitive: bool) -> Tuple[str, str]:
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
