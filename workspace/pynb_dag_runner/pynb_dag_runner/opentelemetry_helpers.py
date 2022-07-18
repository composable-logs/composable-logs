import glob
from pathlib import Path
from typing import (
    Any,
    Dict,
    Generic,
    List,
    Iterator,
    Iterable,
    Mapping,
    MutableMapping,
    Optional,
    Set,
    Tuple,
    TypeVar,
)

# Note eg "from opentelemetry import trace" fails mypy
import opentelemetry as otel
from opentelemetry.trace.span import format_span_id, Span
import dateutil.parser as dp  # type: ignore
from opentelemetry import context, baggage  # type: ignore

#
from pynb_dag_runner.helpers import pairs, flatten, read_jsonl

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


# --- span id/parent id helpers ---


def get_span_id(span: SpanDict) -> SpanId:
    try:
        result = read_key(span, ["context", "span_id"])
        assert result is not None
        return result
    except:
        raise Exception(f"Unable to read span_id from {str(span)}.")


def get_parent_span_id(span: SpanDict) -> Optional[SpanId]:
    """
    Return:
    - span_id of parent if Span has a parent
    - return None otherwise (ie., if span has no parent)
    """
    try:
        return read_key(span, ["parent_id"])
    except:
        raise Exception(f"Unable to read parent_id from {str(span)}.")


def is_parent_child(span_parent: SpanDict, span_child: SpanDict) -> bool:
    """
    Return True/False if span_parent is direct parent of span_child.
    """
    return get_span_id(span_parent) == get_parent_span_id(span_child)


# --- span timestamp helpers ---


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


### --- UDT data structure helper ---

NodeId = TypeVar("NodeId")


# Represent graph edge (n1 -> n2) as tuple (n1, n2).
#
#     n1
#      |
#      v
#     n2
#
# Terminology:
#    n1 is the "parent node";
#    n2 is the "child node".
Edge = Tuple[NodeId, NodeId]


class _UDT_Node(Generic[NodeId]):
    """
    Internal representation of a node in a UDT (see below).

    The id is immutable, and list of child id:s is mutable.
    """

    def __init__(self, node_id: NodeId):
        self.node_id: NodeId = node_id
        self.child_ids: List[NodeId] = []

    def add_child_id(self, child_id: NodeId) -> None:
        if child_id in self.child_ids:
            raise ValueError(
                f"_UDT_Node {self.node_id}: "
                f"The id={child_id} is already a child id for this node. "
                f"Current child_id:s = {self.child_ids}."
            )
        self.child_ids.append(child_id)


class UDT(Generic[NodeId]):
    """
    Represent a "Union of Directed Trees" (UDT) and we use these as directed graphs
    such that:

     - every node has at most one parent;
     - there are no cycles.

    Notes:
    - We will use this to represent OpenTelemetry spans (where each span has at most one
      parent span).
    - The entire collection of spans logged from a process will have one top span (
      so the graph will have one root node). However, if one bound the log there
      might be multiple root nodes.

    Below is example of a UDT with one root (and A -> B indicate that A is parent and
    B is child node)

              1
              | \
              v   \
              2     v
            / | \    10
           v  v  v
           3  4  5

    We can omit the arrows and draw the tree with understanding that if
    (A and B are connected) and (A is above B), then A is parent, and B is child node.
    Then the above graph can be drawn as:

              1
              | \
              2   \
            / | \  10
           3  4  5

    For example, if we bound the above graph to child nodes from 2, we get a graph with
    nodes 3, 4, 5 and no edges (and three root nodes).
    """

    def __init__(
        self,
        all_node_ids: Set[NodeId],
        _node_id_dict: Dict[NodeId, _UDT_Node[NodeId]],  # see above
    ):
        assert set(_node_id_dict.keys()) == all_node_ids

        self.all_node_ids: Set[NodeId] = all_node_ids
        self._node_id_dict: Dict[NodeId, _UDT_Node[NodeId]] = _node_id_dict

    @classmethod
    def from_edges(cls, edges: Set[Edge[NodeId]]) -> "UDT[NodeId]":
        all_node_ids: Set[NodeId] = set(flatten(edges))

        # Create _UDT_Node:s and add child nodes according to the edge data
        _node_id_dict: Dict[NodeId, _UDT_Node[NodeId]] = {
            node_id: _UDT_Node(node_id) for node_id in all_node_ids
        }

        for parent_id, child_id in edges:
            _node_id_dict[parent_id].add_child_id(child_id)

        return cls(all_node_ids, _node_id_dict)

    def __iter__(self) -> Iterator[NodeId]:
        return iter(self.all_node_ids)

    def __len__(self) -> int:
        return len(self.all_node_ids)

    def __contains__(self, node_id: NodeId) -> bool:
        return node_id in self.all_node_ids

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, UDT):
            return (
                self.edges() == other.edges()
                and
                # -
                self.all_node_ids == other.all_node_ids
            )
        else:
            return False

    def _edges(self) -> Iterator[Edge[NodeId]]:
        for parent_node_id, node in self._node_id_dict.items():
            for child_node_id in node.child_ids:
                yield parent_node_id, child_node_id

    def edges(self) -> Set[Edge[NodeId]]:
        """
        Return set of edges for this directed graph
        """
        return set(self._edges())

    def root_nodes(self) -> Set[NodeId]:
        """
        Return root nodes (node(s) that have no parent).
        """
        return self.all_node_ids - set(child_id for (_, child_id) in self.edges())

    def traverse_from(self, root_node_id: NodeId, inclusive: bool) -> Iterable[NodeId]:
        """
        Returns iterator over node_id:s in this UDT under root_node_id.

        root_node_id is included/not included depending on inclusive=True/False.
        """
        assert root_node_id in self

        if inclusive:
            yield root_node_id

        for node_id in self._node_id_dict[root_node_id].child_ids:
            for child_node_id in self.traverse_from(node_id, inclusive=True):
                yield child_node_id

    def bound_by(self, node_id: NodeId, inclusive: bool) -> "UDT[NodeId]":
        """
        Notes:
        - assumes no cycles
        - not currently used
        """
        bounded_node_ids = set(self.traverse_from(node_id, inclusive=inclusive))

        return UDT[NodeId](
            all_node_ids=bounded_node_ids,
            _node_id_dict={k: self._node_id_dict[k] for k in bounded_node_ids},
        )

    def contains_path(self, *node_id_path: NodeId) -> bool:
        """
        For a sequence node_id_path = (n_1, n_2, ..., n_k) of NodeId:s in the
        UDT, return:
          True if the nodeId:s in the path can be connected in the UDT,
          otherwise False

        As shown below, the path (may, but) does not need to be a direct sequence in
        the graph. There can be intermediate nodes.

        Eg. in the UDT
              1
              |
              2
            / | \
           3  4  5

          True == contains_path(1, 2)      [1 is parent of 2 and there is path 1 -> 2]
               == contains_path(1, 3)      [there is path 1 -> 2 -> 3]
               == contains_path(1, 2, 3)

          False == contains_path(3, 4)
                == contains_path(5, 1)     [there is path 1 -> 5, but graph is directed]

        Note: assumes no cycles
        """
        assert set(node_id_path) <= self.all_node_ids
        assert len(node_id_path) >= 2

        # Note: each node can have at most one parent we could first compute
        # maximum path from last element provided, and see if the provided path
        # is subset of this. However, currently we do not have child -> parent
        # easily accessible.

        for node_1, node_2 in pairs(node_id_path):
            assert node_1 != node_2
            if node_2 not in self.traverse_from(node_1, inclusive=False):
                return False

        return True


class Spans:
    """
    Container for Python dictionaries with OpenTelemetry span:s
    """

    def __init__(self, spans: List[SpanDict]):
        self.spans = spans

    def filter(self, keys: List[str], value: Any) -> "Spans":
        def match(span, keys, value):
            try:
                return read_key(span, keys) == value
            except:
                # keys not found
                return False

        return Spans([span for span in self.spans if match(span, keys, value)])

    def sort_by_start_time(self, reverse=False) -> "Spans":
        return Spans(
            list(
                sorted(
                    self,
                    key=lambda s: dp.parse(s["start_time"]).timestamp(),
                    reverse=reverse,
                )
            )
        )

    def __len__(self) -> int:
        return len(self.spans)

    def __iter__(self) -> Iterator[SpanDict]:
        return iter(self.spans)

    def __getitem__(self, idx) -> SpanDict:
        # TODO: deprecate, we should not need to access span:s by their index
        return self.spans[idx]

    def contains_span_id(self, span_id: SpanId) -> bool:
        return span_id in map(get_span_id, self)

    def _get_graph(self) -> UDT[SpanId]:
        # (TODO: Check if with later Python versions this could be done with lru_cache)
        if not hasattr(self, "_cached_graph"):
            edges: List[Edge[SpanId]] = []

            for span in self:
                parent_span_id_o: Optional[SpanId] = get_parent_span_id(span)
                if parent_span_id_o is not None:
                    edges.append((parent_span_id_o, get_span_id(span)))

            self._cached_graph = UDT[SpanId].from_edges(set(edges))

        return self._cached_graph

    def contains_path(self, *span_chain: Span) -> bool:
        """
        Return true/false depending on whether there is a parent-child relationship
        link between the spans in span_chain.

        Cycles in self are not detected.
        """
        span_id_chain: List[SpanId] = [get_span_id(span) for span in span_chain]
        return self._get_graph().contains_path(*span_id_chain)

    def _bound_by(self, top: SpanDict, inclusive: bool) -> "Spans":
        """
        Bound this span collection to spans that can be connected to
        the top-span using one or many parent-child relationships.

        The provided span `top` is only included if inclusive=True.
        """
        bounded_ids: Set[SpanId] = set(
            self._get_graph()
            # -
            .traverse_from(root_node_id=get_span_id(top), inclusive=inclusive)
        )
        return Spans([span for span in self if get_span_id(span) in bounded_ids])

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


# --- helper functions to record Span:s ---
#
# The below currently uses Ray's default OpenTelemetry (log to local /tmp/) logger


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
