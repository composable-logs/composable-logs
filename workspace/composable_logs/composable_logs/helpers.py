import json
from pathlib import Path
from typing import (
    Any,
    Generic,
    TypeVar,
    Mapping,
    Iterable,
    Sequence,
    Tuple,
    Optional,
)

# -
from opentelemetry.trace import StatusCode, Status  # type: ignore
from opentelemetry.trace.span import Span

# -

A = TypeVar("A")
B = TypeVar("B")
K = TypeVar("K")
V = TypeVar("V")

# --- range helper functions ---


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


# --- sequence/data manipulation helper functions ---


def _is_iterable(maybe_iterable):
    # see: https://stackoverflow.com/questions/1952464
    # In particular, this should return False for strings
    return isinstance(maybe_iterable, (set, tuple, list))


def flatten(xss):
    result = []

    for xs in xss:
        if _is_iterable(xs):
            result += flatten(xs)
        else:
            result.append(xs)

    return result


def pairs(xs: Sequence[A]) -> Sequence[Tuple[A, A]]:
    """
    From a list of entries return list of subsequent entries.

    The function assumes the input list has at least 2 entries.

    Eg. [1, 2, 3, 4] -> [(1, 2), (2, 3), (3, 4)]
    """
    if len(xs) <= 1:
        return []
    return list(zip(xs[:-1], xs[1:]))


def one(xs: Iterable[A]) -> A:
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


# --- dict manipulation helper functions ---


def del_key(a_dict: Mapping[A, B], key: A, strict: bool = False) -> Mapping[A, B]:

    """
    Return new dictionary that is identical to `a_dict` with `key`
    removed (if `key` exists).

    If strict = True, then an exception is thrown unless key in a_dict.

    Input dictionary is not modified.
    """
    if strict and key not in a_dict:
        raise ValueError(
            f"del_key: strict is set and key='{key}' not a key in a_dict='{a_dict}'"
        )
    return {k: v for k, v in a_dict.items() if k != key}


def dict_prefix_keys(prefix: str, a_dict: Mapping[str, V]) -> Mapping[str, V]:
    """
    Return new dictionary Dict[str, V] that is identical to `a_dict` but where
    all keys have prefixed with a string.
    """
    return {prefix + k: v for k, v in a_dict.items()}


# --- function helper functions ---


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


class Try(Generic[A]):
    def __init__(self, value: Optional[A], error: Optional[Exception]):

        if error is not None:
            if not isinstance(error, Exception):
                raise Exception(
                    f"Try: non-empty error should be Exception;" f"got {error}"
                )

            if value is not None:
                raise Exception(f"Try: init with both value={value} and error={error}")
            else:
                # Failure(None, e)
                pass
        else:
            # value may be None;
            pass

        # -
        self.value = value
        self.error = error

    def get(self) -> A:
        if self.error is not None:
            raise Exception(f"Try does not contain any value (err={self.error})")
        return self.value  # type: ignore

    def is_success(self) -> bool:
        return self.error is None

    def is_failure(self) -> bool:
        return not self.is_success()

    def map_value(self, f):
        """
        Return new Try where f has been applied to the value
        (or do nothing and return self for failure)

        f is assumed to not throw an Exception
        """
        return self if self.is_failure() else Try(value=f(self.value), error=None)

    def __repr__(self) -> str:
        return f"Try(value={self.value}, error={self.error})"

    @classmethod
    def wrap(cls, f):
        def wrapped_f(*args, **kwargs):
            try:
                return cls(value=f(*args, **kwargs), error=None)
            except Exception as e:
                return cls(value=None, error=e)

        return wrapped_f

    def log_outcome_to_opentelemetry_span(self, span: Span, record_exception: bool):
        if self.is_success():
            span.set_status(Status(StatusCode.OK))
        else:
            assert self.error is not None
            span.set_status(Status(StatusCode.ERROR, "Failure"))

            if record_exception:
                span.record_exception(self.error)

        return self

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Try):
            self_tuple = (self.value, str(self.error))
            other_tuple = (other.value, str(other.error))
            return self_tuple == other_tuple
        else:
            return False


def Success(value):
    return Try(value=value, error=None)


def Failure(error):
    return Try(value=None, error=error)


# --- file I/O helper functions ---


def read_json(filepath: Path) -> Any:
    with open(filepath, "r") as f:
        return json.load(f)


def write_json(filepath: Path, obj: Any):
    """
    Write object as JSON object to filepath (and create directories if needed).
    """

    # ensure base directory for filepath exists
    filepath.parent.mkdir(parents=True, exist_ok=True)

    with open(filepath, "w") as f:
        json.dump(obj, f, indent=2)


def read_jsonl(path: Path):
    assert path.is_file()

    return [json.loads(span_line) for span_line in path.read_text().splitlines()]
