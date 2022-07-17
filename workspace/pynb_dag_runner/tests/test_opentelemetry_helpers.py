import time
from typing import Set, Tuple

#
import pytest

#
import opentelemetry as ot

#
from pynb_dag_runner.opentelemetry_helpers import (
    get_span_id,
    has_keys,
    read_key,
    is_parent_child,
    get_duration_s,
    iso8601_to_epoch_s,
    get_duration_range_us,
    get_span_exceptions,
    Spans,
    SpanRecorder,
    TreeSet,
)
from pynb_dag_runner.helpers import one


def test_nested_dict_helpers():
    a_dict = {
        "a": {
            "b": 123,
            "foo": "bar",
            "bar": "baz",
            "c": {"e": 1, "f": 2, "g": "hello", "h": None},
        }
    }
    assert has_keys(a_dict, ["a"])
    assert has_keys(a_dict, ["a", "b"])
    assert not has_keys(a_dict, ["key-does-not-exist"])
    assert not has_keys(a_dict, ["key-does-not-exist", "key-does-not-exist"])
    assert not has_keys(a_dict, ["a", "key-does-not-exist"])

    assert read_key(a_dict, ["a", "b"]) == 123


def test_iso8601_to_epoch_s():
    assert iso8601_to_epoch_s("2021-10-10T10:25:35.173367Z") == 1633861535173367 / 1e6


def test_tracing_get_span_id_and_duration():
    # Example span generated by Ray when calling a remote method on an Actor
    test_span = {
        "name": "ActorA.foo ray.remote_worker",
        "context": {
            "trace_id": "<hex-trace-id>",
            "span_id": "<hex-span-id>",
            "trace_state": "[]",
        },
        "kind": "SpanKind.CONSUMER",
        "parent_id": "<hex-parent-id>",
        "start_time": "2021-10-10T10:25:35.173367Z",
        "end_time": "2021-10-11T10:25:46.173381Z",
        "status": {"status_code": "UNSET"},
        "attributes": {
            "ray.remote": "actor",
            "ray.actor_class": "ActorA",
            "ray.actor_method": "foo",
            "ray.function": "ActorA.foo",
            "ray.pid": "1234",
            "ray.job_id": "01000000",
            "ray.node_id": "<hex-ray-node-id>",
            "ray.actor_id": "<hex-ray-actor-id>",
            "ray.worker_id": "<hex-ray-worker-id>",
        },
        "events": [],
        "links": [],
        "resource": {
            "telemetry.sdk.language": "python",
            "telemetry.sdk.name": "opentelemetry",
            "telemetry.sdk.version": "1.5.0",
            "service.name": "unknown_service",
        },
    }

    assert get_span_id(test_span) == "<hex-span-id>"
    assert get_duration_s(test_span) == 86411.0000140667
    assert get_duration_range_us(test_span) == range(1633861535173367, 1633947946173381)
    assert get_span_exceptions(test_span) == []


# --- test Spans ---

# --- test Spans: TreeSet ---


@pytest.fixture
def treeset_edges() -> Set[Tuple[int, int]]:
    """
    Test tree:

              0
              |
              1
            /   \
           2     3
         -----
        / | | \
       4  5 6  7
          |    |
          8    9
        / | \
      10 11  12
    """

    return {
        # (parent_id, child_id)
        (0, 1),
        #
        (1, 2),
        (1, 3),
        #
        (2, 4),
        (2, 5),
        (2, 6),
        (2, 7),
        #
        (5, 8),
        (7, 9),
        #
        (8, 10),
        (8, 11),
        (8, 12),
    }


def test__treeset__from_edges(treeset_edges):
    ts = TreeSet[int].from_edges(treeset_edges)

    assert ts.all_node_ids == set(range(13))
    assert ts.root_ids == {0}

    def get_child_ids(node_id: int) -> Set[int]:
        return set(ts.node_id_to_treenode[node_id].child_ids)

    def get_child_ids_from_edges(node_id: int) -> Set[int]:
        return set(
            child_id for (parent_id, child_id) in treeset_edges if parent_id == node_id
        )

    assert get_child_ids(node_id=0) == {1}
    assert get_child_ids(node_id=1) == {2, 3}
    assert get_child_ids(node_id=2) == {4, 5, 6, 7}

    for node_id in ts.all_node_ids:
        assert get_child_ids(node_id) == get_child_ids_from_edges(node_id)
