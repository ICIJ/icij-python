from collections import namedtuple
from typing import Literal

import pytest

from icij_worker.utils import CacheDict


def test_cache_null():
    cache = CacheDict()
    assert len(cache) == 0


Case = namedtuple("Case", ["cache_len", "len", "init"])


@pytest.mark.parametrize(
    "case",
    [
        Case(9, 0, []),
        Case(9, 1, [("one", 1)]),
        Case(9, 2, [("one", 1), ("two", 2)]),
        Case(2, 2, [("one", 1), ("two", 2)]),
        Case(1, 1, [("one", 1), ("two", 2)]),
    ],
)
@pytest.mark.parametrize("method", ["assign", "init"])
def test_cache_init(case: Case, method: Literal["assign", "init"]):
    if method == "init":
        cache = CacheDict(case.init, cache_len=case.cache_len)
    elif method == "assign":
        cache = CacheDict(cache_len=case.cache_len)
        for key, val in case.init:
            cache[key] = val
    else:
        assert False

    # length is max(#entries, cache_len)
    assert len(cache) == case.len

    # make sure the first entry is the one ejected
    if case.cache_len > 1 and case.init:
        assert "one" in cache.keys()
    else:
        assert "one" not in cache.keys()


@pytest.mark.parametrize("method", ["init", "assign"])
def test_cache_overflow_default(method):
    """Test default overflow logic."""
    if method == "init":
        cache = CacheDict([("one", 1), ("two", 2), ("three", 3)], cache_len=2)
    elif method == "assign":
        cache = CacheDict(cache_len=2)
        cache["one"] = 1
        cache["two"] = 2
        cache["three"] = 3
    else:
        assert False

    assert "one" not in cache.keys()
    assert "two" in cache.keys()
    assert "three" in cache.keys()


@pytest.mark.parametrize("mode", ["get", "set"])
@pytest.mark.parametrize("add_third", [True, False])
def test_cache_lru_overflow(mode: Literal["get", "set"], add_third: bool):
    cache = CacheDict([("one", 1), ("two", 2)], cache_len=2)

    if mode == "get":
        _ = cache["one"]
    elif mode == "set":
        cache["one"] = 1
    else:
        raise ValueError(f"Unknown mode {mode}")

    if add_third:
        cache["three"] = 3
        assert "one" in cache.keys()
        assert "two" not in cache.keys()
        assert "three" in cache.keys()
    else:
        assert "one" in cache.keys()
        assert "two" in cache.keys()
        assert "three" not in cache.keys()


def test_cache_keyerror():
    cache = CacheDict()
    with pytest.raises(KeyError):
        _ = cache["foo"]


def test_cache_miss_doesnt_eject():
    cache = CacheDict([("one", 1), ("two", 2)], cache_len=2)
    with pytest.raises(KeyError):
        _ = cache["foo"]

    assert len(cache) == 2
    assert "one" in cache.keys()
    assert "two" in cache.keys()
