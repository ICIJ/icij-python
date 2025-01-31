from __future__ import annotations

import inspect
from copy import deepcopy
from typing import Any, Callable, Collection, Dict, OrderedDict, Set

from .dependencies import run_deps
from .registrable import FromConfig, RegistrableConfig, RegistrableFromConfig


def add_missing_args(fn: Callable, args: Dict[str, Any], **kwargs) -> Dict[str, Any]:
    # We make the choice not to raise in case of missing argument here, the error will
    # be correctly raise when the function is called
    from_kwargs = dict()
    sig = inspect.signature(fn)
    for param_name in sig.parameters:
        if param_name in args:
            continue
        kwargs_value = kwargs.get(param_name)
        if kwargs_value is not None:
            from_kwargs[param_name] = kwargs_value
    if from_kwargs:
        args = deepcopy(args)
        args.update(from_kwargs)
    return args


def find_missing_args(fn: Callable, already_known: Collection[str]) -> Set[str]:
    already_known = set(already_known)
    sig = inspect.signature(fn)
    missing = set()
    for param_name in sig.parameters:
        if param_name in already_known or param_name == "progress":
            continue
        missing.add(param_name)
    return missing


class CacheDict(OrderedDict):

    def __init__(self, *args, cache_len: int = 10, **kwargs):
        assert cache_len > 0
        self.cache_len = cache_len

        super().__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        super().move_to_end(key)

        while len(self) > self.cache_len:
            oldkey = next(iter(self))
            super().__delitem__(oldkey)

    def __getitem__(self, key):
        val = super().__getitem__(key)
        super().move_to_end(key)

        return val
