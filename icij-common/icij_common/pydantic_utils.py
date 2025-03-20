from collections import defaultdict
from enum import Enum

from datetime import datetime
from functools import cached_property
from typing import Annotated, Any, Callable, Mapping, TypeVar, cast

from pydantic import BaseModel, ConfigDict, PlainSerializer
from pydantic.fields import FieldInfo
from pydantic_settings import BaseSettings

SetIntStr = set[int | str]
DictIntStrAny = dict[int | str, Any]


def to_lower_camel(field: str) -> str:
    return "".join(
        w.capitalize() if i > 0 else w for i, w in enumerate(field.split("_"))
    )


def to_lower_snake_case(s: str):
    snake = "".join("_" + c.lower() if c.isupper() else c for c in s)
    if snake.startswith("_"):
        snake = snake[1:]
    return snake


_SCHEMAS = dict()


def safe_copy(
    obj: BaseModel, update: Mapping[str, Any] | None = None, deep: bool = False
):
    if obj.__class__ not in _SCHEMAS:
        _SCHEMAS[obj.__class__] = dict()
        # Model.copy is always without alias
        _SCHEMAS[obj.__class__] = obj.model_json_schema(by_alias=False)
    schema = _SCHEMAS[obj.__class__]
    if update:
        for field in update.keys():
            props = schema.get("properties")
            if props is None:
                prop_key = schema["$ref"].replace("#/$defs/", "")
                props = schema["$defs"][prop_key]["properties"]
            if field not in props:
                msg = f'Unknown attribute "{field}" for {obj.__class__}'
                raise AttributeError(msg)

    return cast(obj.__class__, obj.model_copy(update=update, deep=deep))


def icij_config() -> ConfigDict:
    return ConfigDict(
        frozen=True,
        extra="forbid",
        populate_by_name=True,
        ignored_types=(cached_property,),
        use_enum_values=True,
        validate_default=True,
    )


class ICIJSettings(BaseSettings): ...  # pylint: disable=multiple-statements


def get_field_default_value(attr: FieldInfo):
    # Ugly work around pydantic v1 limitations on Field and default values
    if isinstance(attr, FieldInfo):
        if attr.default_factory is not None:
            return attr.default_factory()
        return attr.default
    return attr


def lowercamel_case_config() -> dict:
    return ConfigDict(alias_generator=to_lower_camel)


def ignore_extra_config() -> dict:
    return ConfigDict(extra="ignore")


def no_enum_values_config() -> dict:
    return ConfigDict(use_enum_values=False)


def merge_configs(*configs) -> ConfigDict:
    merged = dict()
    for c in configs:
        merged.update(c)
    return ConfigDict(**merged)


ISODatetime = Annotated[
    datetime,
    PlainSerializer(
        lambda x: x.isoformat(), return_type=datetime, when_used="json-unless-none"
    ),
]


def generate_encoders_by_class_tuples(
    type_encoder_map: dict[Any, Callable[[Any], Any]],
) -> dict[Callable[[Any], Any], tuple[Any, ...]]:
    encoders_by_class_tuples: dict[Callable[[Any], Any], tuple[Any, ...]] = defaultdict(
        tuple
    )
    for type_, encoder in type_encoder_map.items():
        encoders_by_class_tuples[encoder] += (type_,)
    return encoders_by_class_tuples


E = TypeVar("E", bound=Enum)


def make_enum_discriminator(key: str, enum_cls: type[E]) -> Callable[[Any], E]:
    def discriminator(data: Any) -> E:
        nonlocal key, enum_cls
        if isinstance(data, dict):
            discriminator_key = data[key]
        elif hasattr(data, key):
            discriminator_key = getattr(data, key)
        else:
            raise ValueError(f"could not find key '{enum_cls}' in {data}")
        if isinstance(discriminator_key, enum_cls):
            return discriminator_key
        return enum_cls(discriminator_key)

    return discriminator
