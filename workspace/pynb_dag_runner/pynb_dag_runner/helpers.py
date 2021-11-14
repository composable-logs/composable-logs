import json
from pathlib import Path
from typing import Any, TypeVar, List, Sequence, Tuple

A = TypeVar("A")


def range_is_empty(range):
    assert range.step == 1
    return not (range.start < range.stop)


def range_intersection(range1, range2):
    """
    Return intersection range of two Python ranges.
    """
    if range_is_empty(range1):
        return range1

    if range_is_empty(range2):
        return range2

    last_start: int = max(range1.start, range2.start)
    first_stop: int = min(range1.stop, range2.stop)

    return range(last_start, first_stop)


def range_intersect(range1, range2) -> bool:
    """
    Return bool-ean representing whether two non-empty Python ranges intersect
    """
    return not range_is_empty(range_intersection(range1, range2))


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


def pairs(xs: Sequence[A]) -> Sequence[Tuple[A, A]]:
    """
    From a list of entries return list of subsequent entries.

    The function assumes the input list has at least 2 entries.

    Eg. [1, 2, 3, 4] -> [(1, 2), (2, 3), (3, 4)]
    """
    if len(xs) <= 1:
        return []
    return list(zip(xs[:-1], xs[1:]))


def read_json(filepath: Path) -> Any:
    with open(filepath, "r") as f:
        return json.load(f)


def write_json(filepath: Path, obj: Any):
    with open(filepath, "w") as f:
        json.dump(obj, f, indent=2)


def read_jsonl(path: Path):
    assert path.is_file()

    return [json.loads(span_line) for span_line in path.read_text().splitlines()]


def one(xs: Sequence[A]) -> A:
    """
    Assert that input can be converted into list with only one element, and
    return that element.
    """
    xs_list = list(xs)
    if not len(xs_list) == 1:
        raise Exception(
            "one: Expected input with only one element, "
            f"but input has length {len(xs_list)}."
        )

    return xs_list[0]
