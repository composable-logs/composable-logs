import glob
from pathlib import Path
from typing import Any, List

# Note "from opentelemetry import trace" fails mypy
import opentelemetry as ot
import dateutil.parser as dp  # type: ignore

#
from pynb_dag_runner.helpers import flatten, read_jsonl

# ---- helper functions to read OpenTelemetry span dictionaries ----


def read_key(nested_dict, keys: List[str]):
    first_key, *rest_keys = keys

    if first_key not in nested_dict:
        raise Exception(f"read_key: {first_key} not found")

    if len(rest_keys) == 0:
        return nested_dict[first_key]
    else:
        return read_key(nested_dict[first_key], rest_keys)


def get_span_id(span):
    result = read_key(span, ["context", "span_id"])
    assert result is not None
    return result


def get_duration_s(span) -> float:
    start_epoch_s: float = dp.parse(span["start_time"]).timestamp()
    end_epoch_s: float = dp.parse(span["end_time"]).timestamp()
    return end_epoch_s - start_epoch_s


def is_parent_child(span_parent, span_child):
    child_parent_id = read_key(span_child, ["parent_id"])
    return (child_parent_id is not None) and (
        child_parent_id == get_span_id(span_parent)
    )


class Spans:
    """
    Container for Python dictionaries with OpenTelemetry span:s
    """

    def __init__(self, spans):
        self.spans = spans

    def filter(self, keys: List[str], value: Any):
        return Spans([span for span in self.spans if read_key(span, keys) == value])

    def __len__(self):
        return len(self.spans)

    def __iter__(self):
        return iter(self.spans)


def _get_all_spans():
    return flatten([read_jsonl(Path(f)) for f in glob.glob("/tmp/spans/*.txt")])


class SpanRecorder:
    """
    Recorder for getting logged OpenTelemetry spans emitted from a code block. Eg.,

    with SpanRecorder() as rec:
        # ...
        # code emitting OpenTelemetry spans
        # ...

    spans: Spans = rec.spans

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