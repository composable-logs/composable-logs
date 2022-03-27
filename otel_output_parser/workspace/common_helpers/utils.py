import json
from pathlib import Path
from typing import Any


def bytes_to_json(b: bytes) -> Any:
    return json.loads(b.decode("utf-8"))


def ensure_dir_exist(p: Path) -> Path:
    p.parent.mkdir(parents=True, exist_ok=True)
    return p
