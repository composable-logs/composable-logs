import json
from pathlib import Path
from typing import Any


def ranges_intersection(range1, range2):
    """
    Return intersection range of two non-empty Python ranges.
    """
    for r in [range1, range2]:
        assert r.step == 1
        assert r.start < r.stop

    last_start: int = max(range1.start, range2.start)
    first_stop: int = min(range1.stop, range2.stop)

    return range(last_start, first_stop)


def ranges_intersect(range1, range2) -> bool:
    """
    Return bool-ean representing whether two non-empty Python ranges intersect
    """
    range12 = ranges_intersection(range1, range2)
    return range12.start < range12.stop


def flatten(xss):
    assert isinstance(xss, list)
    result = []

    for xs in xss:
        if isinstance(xs, list):
            result += flatten(xs)
        else:
            result += [xs]

    return result


def compose(*fs):
    """
    Functional compositions of one or more functions. Eg. compose(f1, f2, f3, f4).

    The innermost function:
     - may take arbitrary (including zero) positional arguments.
     - should produce one output value

    The remaining functions (if there are more than one argument to compose):
     - should take one input positional argument and produce one output value.

    """
    assert len(fs) >= 1
    *fs_outers, f_innermost = fs

    if len(fs_outers) > 0:
        return lambda *xs: compose(*fs_outers)(f_innermost(*xs))
    else:
        return f_innermost


def read_json(filepath: Path) -> Any:
    with open(filepath, "r") as f:
        return json.load(f)


def write_json(filepath: Path, obj: Any):
    with open(filepath, "w") as f:
        json.dump(obj, f, indent=2)
