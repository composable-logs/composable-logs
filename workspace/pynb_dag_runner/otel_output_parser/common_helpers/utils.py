import json
from pathlib import Path
from typing import Any, Dict, TypeVar
import dateutil.parser as dp  # type: ignore

A = TypeVar("A")
B = TypeVar("B")


def bytes_to_json(b: bytes) -> Any:
    return json.loads(b.decode("utf-8"))


def ensure_dir_exist(p: Path) -> Path:
    """
    For a filepath `p` (eg /tmp/foo/bar/baz.txt) ensure that the path /tmp/foo/bar
    exists.

    The input filepath is returned, so that the function can be chained.
    """
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def del_key(a_dict: Dict[A, B], key: A) -> Dict[A, B]:
    return {k: v for k, v in a_dict.items() if k != key}


def iso8601_to_epoch_s(iso8601_datetime: str) -> float:
    # This may not correctly handle timezones correctly:
    # https://docs.python.org/3/library/datetime.html#datetime.datetime.timestamp
    return dp.parse(iso8601_datetime).timestamp()


def iso8601_to_epoch_ms(iso8601_datetime: str) -> int:
    return int(iso8601_to_epoch_s(iso8601_datetime) * 1000)
