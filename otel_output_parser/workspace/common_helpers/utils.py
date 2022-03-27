import json
from pathlib import Path
from typing import Any, Dict, TypeVar

A = TypeVar("A")
B = TypeVar("B")


def bytes_to_json(b: bytes) -> Any:
    return json.loads(b.decode("utf-8"))


def ensure_dir_exist(p: Path) -> Path:
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def del_key(a_dict: Dict[A, B], key: A) -> Dict[A, B]:
    return {k: v for k, v in a_dict.items() if k != key}
