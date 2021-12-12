import glob
from pathlib import Path
from typing import Any, List

# Note "from opentelemetry import trace" fails mypy
import opentelemetry as ot
import dateutil.parser as dp  # type: ignore

#
from pynb_dag_runner.helpers import pairs, flatten, read_jsonl, one

# ---- helper functions to read OpenTelemetry span dictionaries ----


def has_keys(nested_dict, keys: List[str]) -> bool:
    assert len(keys) > 0

    first_key, *rest_keys = keys
    if len(rest_keys) == 0:
        return first_key in nested_dict
    else:
        return first_key in nested_dict and has_keys(nested_dict[first_key], rest_keys)


def read_key(nested_dict, keys: List[str]) -> Any:
    assert len(keys) > 0
    first_key, *rest_keys = keys

    if first_key not in nested_dict:
        raise Exception(f"read_key: {first_key} not found")

    if len(rest_keys) == 0:
        return nested_dict[first_key]
    else:
        return read_key(nested_dict[first_key], rest_keys)


# ---- span functions ----
Span = Any
SpanId = str


def get_span_exceptions(span: Span):
    return [event for event in span["events"] if event.get("name", "") == "exception"]


def get_span_id(span: Span) -> SpanId:
    try:
        result = read_key(span, ["context", "span_id"])
        assert result is not None
        return result
    except:
        raise Exception(f"Unable to read span_id from {str(span)}.")


def iso8601_to_epoch_s(iso8601_datetime: str) -> float:
    # This may not correctly handle timezones correctly:
    # https://docs.python.org/3/library/datetime.html#datetime.datetime.timestamp
    return dp.parse(iso8601_datetime).timestamp()


def get_duration_range_us(span: Span):
    start_epoch_us: int = int(iso8601_to_epoch_s(span["start_time"]) * 1e6)
    end_epoch_us: int = int(iso8601_to_epoch_s(span["end_time"]) * 1e6)
    return range(start_epoch_us, end_epoch_us)


def get_duration_s(span: Span) -> float:
    """
    Return time duration for span in seconds (as float)
    """
    start_epoch_s: float = iso8601_to_epoch_s(span["start_time"])
    end_epoch_s: float = iso8601_to_epoch_s(span["end_time"])
    return end_epoch_s - start_epoch_s


def is_parent_child(span_parent: Span, span_child: Span) -> bool:
    """
    Return True/False if span_parent is direct parent of span_child.
    """
    child_parent_id = read_key(span_child, ["parent_id"])
    return (child_parent_id is not None) and (
        child_parent_id == get_span_id(span_parent)
    )


class Spans:
    """
    Container for Python dictionaries with OpenTelemetry span:s
    """

    def __init__(self, spans: List[Span]):
        self.spans = spans

    def filter(self, keys: List[str], value: Any):
        def match(span, keys, value):
            try:
                return read_key(span, keys) == value
            except:
                # keys not found
                return False

        return Spans([span for span in self.spans if match(span, keys, value)])

    def get_by_span_id(self, span_id) -> Span:
        return one([span for span in self if get_span_id(span) == span_id])

    def sort_by_start_time(self):
        return Spans(
            list(sorted(self, key=lambda s: dp.parse(s["start_time"]).timestamp()))
        )

    def __len__(self):
        return len(self.spans)

    def __iter__(self):
        return iter(self.spans)

    def __getitem__(self, idx):
        return self.spans[idx]

    def contains_span_id(self, span_id: SpanId) -> bool:
        return span_id in map(get_span_id, self)

    def contains(self, span: Span) -> bool:
        return self.contains_span_id(get_span_id(span))

    def contains_path(self, *span_chain: Span, recursive: bool = True) -> bool:
        """
        Return true/false depending on whether there is a parent-child relationship
        link between the spans in span_chain.

        If recursive=False, the relation should be direct. Otherwise multiple
        parent-child relationships/links are allowed.

        Cycles in self are not detected.
        """
        assert len(span_chain) >= 2

        if len(span_chain) == 2:
            parent, child = span_chain
            assert self.contains(parent) and self.contains(child)

            if is_parent_child(parent, child):
                return True

            if recursive:
                child_subspans = [s for s in self if is_parent_child(parent, s)]
                return any(
                    self.contains_path(s, child, recursive=True) for s in child_subspans
                )
            else:
                return False
        else:
            return all(
                self.contains_path(*ps, recursive=recursive) for ps in pairs(span_chain)
            )

    def restrict_by_top(self, top: Span) -> "Spans":
        """
        Restrict this collection of Spans to spans that can be connected to
        the parent-span using one or many parent-child relationship(s).

        Note: the provided span `top` is not included in the result.
        """
        return Spans([s for s in self if self.contains_path(top, s, recursive=True)])

    def exceptions_in(self, top: Span):
        """
        Return list of Exception events in top and all sub-spans to top.
        """
        return flatten(
            [get_span_exceptions(s) for s in [top] + list(self.restrict_by_top(top))]
        )


def _get_all_spans():
    return flatten([read_jsonl(Path(f)) for f in glob.glob("/tmp/spans/*.txt")])


class SpanRecorder:
    """
    Recorder for getting logged OpenTelemetry spans emitted from a code block. Eg.,

    ```
    with SpanRecorder() as rec:
        # ...
        # code emitting OpenTelemetry spans
        # ...

    spans: Spans = rec.spans
    ```

    This below implementation assumes that spans are written using Ray's default to-file
    span logger. See ray.init for details of how this is enabled during unit testing.
    """

    def __init__(self):
        pass

    def __enter__(self):
        assert ot.trace.get_tracer_provider().force_flush()

        # get all span_id:s that exist before we start recording (inside with block)
        self._all_span_ids_pre_run = [get_span_id(s) for s in _get_all_spans()]

        return self

    def __exit__(self, type, value, traceback):
        assert ot.trace.get_tracer_provider().force_flush()

        # get new spans after test has run
        self.spans = Spans(
            [
                span
                for span in _get_all_spans()
                if get_span_id(span) not in self._all_span_ids_pre_run
            ]
        )
