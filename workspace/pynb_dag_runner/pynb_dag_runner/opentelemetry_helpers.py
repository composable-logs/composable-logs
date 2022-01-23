import glob
from pathlib import Path
from typing import Any, List, Mapping, Set, Optional, MutableMapping

# Note eg "from opentelemetry import trace" fails mypy
import opentelemetry as otel
from opentelemetry.trace.span import format_span_id, Span
import dateutil.parser as dp  # type: ignore
from opentelemetry import context, baggage  # type: ignore

#
from pynb_dag_runner.helpers import pairs, flatten, read_jsonl, one

AttributesDict = Mapping[str, Any]


# ---- baggage helpers ----


def otel_add_baggage(key: str, value: Any):
    """
    Add key=value to baggage (propagated also downstreams).

    Note: baggage content is not stored to logged Span:s.

    See:
    https://github.com/open-telemetry/opentelemetry-python/blob/main/opentelemetry-api/tests/baggage/test_baggage.py
    """
    _ = context.attach(baggage.set_baggage(key, value))


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
SpanDict = Any
SpanId = str


def get_span_hexid(span: SpanDict) -> str:
    # manually add "0x" to be compatible with OTEL json:s
    return "0x" + format_span_id(span.get_span_context().span_id)


def get_span_exceptions(span: SpanDict):
    return [event for event in span["events"] if event.get("name", "") == "exception"]


def get_span_id(span: SpanDict) -> SpanId:
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


def get_duration_range_us(span: SpanDict):
    start_epoch_us: int = int(iso8601_to_epoch_s(span["start_time"]) * 1e6)
    end_epoch_us: int = int(iso8601_to_epoch_s(span["end_time"]) * 1e6)
    return range(start_epoch_us, end_epoch_us)


def get_duration_s(span: SpanDict) -> float:
    """
    Return time duration for span in seconds (as float)
    """
    start_epoch_s: float = iso8601_to_epoch_s(span["start_time"])
    end_epoch_s: float = iso8601_to_epoch_s(span["end_time"])
    return end_epoch_s - start_epoch_s


def is_parent_child(span_parent: SpanDict, span_child: SpanDict) -> bool:
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

    def __init__(self, spans: List[SpanDict]):
        self.spans = spans

    def filter(self, keys: List[str], value: Any):
        def match(span, keys, value):
            try:
                return read_key(span, keys) == value
            except:
                # keys not found
                return False

        return Spans([span for span in self.spans if match(span, keys, value)])

    def get_by_span_id(self, span_id) -> SpanDict:
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

    def contains(self, span: SpanDict) -> bool:
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

    def _bound_by(self, top: SpanDict, inclusive: bool = False) -> "Spans":
        """
        Bound this span collection to spans that can be connected to
        the top-span using one or many parent-child relationships.

        Note: the provided span `top` is only included if inclusive=True.
        """
        top_optional: List[SpanDict] = [top] if inclusive else []

        return Spans(
            top_optional
            + [s for s in self if self.contains_path(top, s, recursive=True)]
        )

    def bound_under(self, top) -> "Spans":
        return self._bound_by(top, inclusive=False)

    def bound_inclusive(self, top) -> "Spans":
        return self._bound_by(top, inclusive=True)

    def exception_events(self):
        """
        Return list of all recorded exceptions in this span collection.
        """
        return flatten([get_span_exceptions(s) for s in self])

    def get_attributes(
        self, allowed_prefixes: Optional[Set[str]] = None
    ) -> AttributesDict:
        """
        Return union of all attributes in this span collection. Only include attribute
        keys that start with the allowed prefixes listed in `allowed_prefixes`.

        Raise an exception if the span collection spans contains an attribute key with
        multiple distinct values in different spans.
        """
        result: MutableMapping[str, Any] = dict()

        def filter_attribute_dict(d: AttributesDict) -> AttributesDict:
            if allowed_prefixes is None:
                return d
            else:
                return {
                    k: v
                    for k, v in d.items()
                    if any(k.startswith(prefix) for prefix in allowed_prefixes)
                }

        for span in self:
            for k, v in filter_attribute_dict(span["attributes"]).items():
                if k in result:
                    if result[k] != v:
                        raise ValueError(
                            f"Encountered key={k} with different values {result[k]} and {v}"
                        )
                    # do nothing: {k: v} is already in result
                else:
                    result[k] = v
        return result


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
        assert otel.trace.get_tracer_provider().force_flush()

        # get all span_id:s that exist before we start recording (inside with block)
        self._all_span_ids_pre_run = [get_span_id(s) for s in _get_all_spans()]

        return self

    def __exit__(self, type, value, traceback):
        assert otel.trace.get_tracer_provider().force_flush()

        # get new spans after test has run
        self.spans = Spans(
            [
                span
                for span in _get_all_spans()
                if get_span_id(span) not in self._all_span_ids_pre_run
            ]
        )
