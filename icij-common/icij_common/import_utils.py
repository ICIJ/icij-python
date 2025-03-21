import importlib
import warnings

from typing import Any


class VariableNotFound(ImportError): ...  # pylint: disable=multiple-statements


def import_variable(name: str) -> Any:
    if ":" in name:
        return _import_variable(name)
    return _legacy_import_variable(name)


def _import_variable(name: str) -> Any:
    module, variable_name = name.split(":")
    if not module:
        raise VariableNotFound(f"{name} not found in available module")
    try:
        module = importlib.import_module(module)
    except ModuleNotFoundError as e:
        raise VariableNotFound(e.msg) from e
    try:
        variable = getattr(module, variable_name)
    except AttributeError as e:
        raise VariableNotFound(e) from e
    return variable


def _legacy_import_variable(name: str) -> Any:
    msg = (
        "importing using only dot will be soon deprecated,"
        " use the new path.to.module:variable syntax"
    )
    warnings.warn(msg, DeprecationWarning)
    parts = name.split(".")
    submodule = ".".join(parts[:-1])
    if not submodule:
        raise VariableNotFound(f"{name} not found in available module")
    variable_name = parts[-1]
    try:
        module = importlib.import_module(submodule)
    except ModuleNotFoundError as e:
        raise VariableNotFound(e.msg) from e
    try:
        variable = getattr(module, variable_name)
    except AttributeError as e:
        raise VariableNotFound(e) from e
    return variable
