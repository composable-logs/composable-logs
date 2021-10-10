from pathlib import Path
import glob, json

# Note "from opentelemetry import trace" fails mypy
import opentelemetry as ot

#
from pynb_dag_runner.helpers import flatten
from pynb_dag_runner.opentelemetry_helpers import get_span_id


### This files contains helpers for testing logged OpenTelemetry spans emitted from
### pynb-dag-runner library

# ---- helpers for working with OpenTelemetry spans during local unit testing ----


def get_children(spans, span_id, recursive=False):
    if not recursive:
        return [span for span in spans if get_span_id(span) == span_id]
    else:
        raise Exception("Not implemented")


# ---- see ray.init for details of how tracing is enabled during unit testing ----


def read_jsonl(path: Path):
    return [json.loads(span_line) for span_line in path.read_text().splitlines()]


def _get_all_spans():
    return flatten([read_jsonl(Path(f)) for f in glob.glob("/tmp/spans/*.txt")])


class SpanRecorder:
    def __init__(self):
        pass

    def __enter__(self):
        assert ot.trace.get_tracer_provider().force_flush()

        # get all span_id:s that exist before we start recording (inside with block)
        self.all_span_ids_pre_run = [get_span_id(s) for s in _get_all_spans()]

        return self

    def __exit__(self, type, value, traceback):
        assert ot.trace.get_tracer_provider().force_flush()

        # get new spans after test has run
        self.spans = [
            span
            for span in _get_all_spans()
            if get_span_id(span) not in self.all_span_ids_pre_run
        ]

    def get_children(self, span_id, recursive=False):
        return get_children(self.spans, span_id, recursive)
