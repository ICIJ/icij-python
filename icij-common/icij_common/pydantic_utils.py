import dataclasses
from collections import defaultdict, deque
from datetime import datetime
from enum import Enum
from functools import cached_property
from pathlib import PurePath
from types import GeneratorType
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union, cast

from pydantic import BaseModel, BaseSettings, Extra
from pydantic.fields import FieldInfo
from pydantic.json import ENCODERS_BY_TYPE

SetIntStr = Set[Union[int, str]]
DictIntStrAny = Dict[Union[int, str], Any]


def to_lower_camel(field: str) -> str:
    return "".join(
        w.capitalize() if i > 0 else w for i, w in enumerate(field.split("_"))
    )


def to_lower_snake_case(s: str):
    snake = "".join("_" + c.lower() if c.isupper() else c for c in s)
    if snake.startswith("_"):
        snake = snake[1:]
    return snake


_FIELD_ARGS = ["include", "exclude", "update"]

_SCHEMAS = dict()


# TODO: remove this one when migrating to pydantic 2.0
def safe_copy(obj: BaseModel, **kwargs):
    if obj.__class__ not in _SCHEMAS:
        _SCHEMAS[obj.__class__] = dict()
        # Model.copy is always without alias
        _SCHEMAS[obj.__class__] = obj.__class__.schema(by_alias=False)
    schema = _SCHEMAS[obj.__class__]
    for k in _FIELD_ARGS:
        if k not in kwargs:
            continue
        for field in kwargs[k]:
            props = schema.get("properties")
            if props is None:
                prop_key = schema["$ref"].replace("#/definitions/", "")
                props = schema["definitions"][prop_key]["properties"]
            if field not in props:
                msg = f'Unknown attribute "{field}" for {obj.__class__}'
                raise AttributeError(msg)

    return cast(obj.__class__, obj.copy(**kwargs))


class ICIJModel(BaseModel):
    class Config:
        allow_mutation = False
        extra = Extra.forbid
        allow_population_by_field_name = True
        keep_untouched = (cached_property,)
        use_enum_values = True


class ICIJSettings(BaseSettings): ...  # pylint: disable=multiple-statements


def get_field_default_value(attr: FieldInfo):
    # Ugly work around pydantic v1 limitations on Field and default values
    if isinstance(attr, FieldInfo):
        if attr.default_factory is not None:
            return attr.default_factory()
        return attr.default
    return attr


class LowerCamelCaseModel(ICIJModel):
    class Config:
        alias_generator = to_lower_camel


class IgnoreExtraModel(ICIJModel):
    class Config:
        extra = "ignore"


class NoEnumModel(ICIJModel):
    class Config:
        use_enum_values = False


class ISODatetime(ICIJModel):
    class Config:
        json_encoders = {datetime: lambda x: x.isoformat()}


def generate_encoders_by_class_tuples(
    type_encoder_map: Dict[Any, Callable[[Any], Any]]
) -> Dict[Callable[[Any], Any], Tuple[Any, ...]]:
    encoders_by_class_tuples: Dict[Callable[[Any], Any], Tuple[Any, ...]] = defaultdict(
        tuple
    )
    for type_, encoder in type_encoder_map.items():
        encoders_by_class_tuples[encoder] += (type_,)
    return encoders_by_class_tuples


encoders_by_class_tuples_ = generate_encoders_by_class_tuples(ENCODERS_BY_TYPE)


def jsonable_encoder(
    obj: Any,
    include: Optional[Union[SetIntStr, DictIntStrAny]] = None,
    exclude: Optional[Union[SetIntStr, DictIntStrAny]] = None,
    by_alias: bool = True,
    exclude_unset: bool = False,
    exclude_defaults: bool = False,
    exclude_none: bool = False,
    custom_encoder: Optional[Dict[Any, Callable[[Any], Any]]] = None,
    sqlalchemy_safe: bool = True,
) -> Any:
    custom_encoder = custom_encoder or {}
    if custom_encoder:
        if type(obj) in custom_encoder:
            return custom_encoder[type(obj)](obj)
        for encoder_type, encoder_instance in custom_encoder.items():
            if isinstance(obj, encoder_type):
                return encoder_instance(obj)
    if include is not None and not isinstance(include, (set, dict)):
        include = set(include)
    if exclude is not None and not isinstance(exclude, (set, dict)):
        exclude = set(exclude)
    if isinstance(obj, BaseModel):
        encoder = getattr(obj.__config__, "json_encoders", {})
        if custom_encoder:
            encoder.update(custom_encoder)
        obj_dict = obj.dict(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            exclude_unset=exclude_unset,
            exclude_none=exclude_none,
            exclude_defaults=exclude_defaults,
        )
        if "__root__" in obj_dict:
            obj_dict = obj_dict["__root__"]
        return jsonable_encoder(
            obj_dict,
            exclude_none=exclude_none,
            exclude_defaults=exclude_defaults,
            custom_encoder=encoder,
            sqlalchemy_safe=sqlalchemy_safe,
        )
    if dataclasses.is_dataclass(obj):
        obj_dict = dataclasses.asdict(obj)
        return jsonable_encoder(
            obj_dict,
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
            custom_encoder=custom_encoder,
            sqlalchemy_safe=sqlalchemy_safe,
        )
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, PurePath):
        return str(obj)
    if isinstance(obj, (str, int, float, type(None))):
        return obj
    if isinstance(obj, dict):
        encoded_dict = {}
        allowed_keys = set(obj.keys())
        if include is not None:
            allowed_keys &= set(include)
        if exclude is not None:
            allowed_keys -= set(exclude)
        for key, value in obj.items():
            if (
                (
                    not sqlalchemy_safe
                    or (not isinstance(key, str))
                    or (not key.startswith("_sa"))
                )
                and (value is not None or not exclude_none)
                and key in allowed_keys
            ):
                encoded_key = jsonable_encoder(
                    key,
                    by_alias=by_alias,
                    exclude_unset=exclude_unset,
                    exclude_none=exclude_none,
                    custom_encoder=custom_encoder,
                    sqlalchemy_safe=sqlalchemy_safe,
                )
                encoded_value = jsonable_encoder(
                    value,
                    by_alias=by_alias,
                    exclude_unset=exclude_unset,
                    exclude_none=exclude_none,
                    custom_encoder=custom_encoder,
                    sqlalchemy_safe=sqlalchemy_safe,
                )
                encoded_dict[encoded_key] = encoded_value
        return encoded_dict
    if isinstance(obj, (list, set, frozenset, GeneratorType, tuple, deque)):
        encoded_list = []
        for item in obj:
            encoded_list.append(
                jsonable_encoder(
                    item,
                    include=include,
                    exclude=exclude,
                    by_alias=by_alias,
                    exclude_unset=exclude_unset,
                    exclude_defaults=exclude_defaults,
                    exclude_none=exclude_none,
                    custom_encoder=custom_encoder,
                    sqlalchemy_safe=sqlalchemy_safe,
                )
            )
        return encoded_list

    if type(obj) in ENCODERS_BY_TYPE:
        return ENCODERS_BY_TYPE[type(obj)](obj)
    for encoder, classes_tuple in encoders_by_class_tuples_.items():
        if isinstance(obj, classes_tuple):
            return encoder(obj)

    try:
        data = dict(obj)
    except Exception as e:  # pylint: disable=broad-exception-caught
        errors: List[Exception] = [e]
        try:
            data = vars(obj)
        except Exception as other_e:
            errors.append(other_e)
            raise ValueError(errors) from other_e
    return jsonable_encoder(
        data,
        include=include,
        exclude=exclude,
        by_alias=by_alias,
        exclude_unset=exclude_unset,
        exclude_defaults=exclude_defaults,
        exclude_none=exclude_none,
        custom_encoder=custom_encoder,
        sqlalchemy_safe=sqlalchemy_safe,
    )
