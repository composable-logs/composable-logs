import pytest

from pynb_dag_runner.core.dag_syntax import Node, Edge, Edges


def test_node_and_single_edges_equality():
    n0, n1 = [Node() for _ in range(2)]

    assert n0 == n0
    assert n0 != n1

    def make_collection(n: Node, m: Node):
        return Edges(Edge(from_node=n, to_node=m))

    assert make_collection(n0, n1) == make_collection(n0, n1)
    assert make_collection(n0, n1) != make_collection(n1, n0)


### test Edges properties


def test_can_create_empty_collection_of_edges():
    assert len(Edges()) == 0


def test_can_not_create_self_loops():
    n = Node()

    with pytest.raises(Exception):
        Edge(from_node=n, to_node=n)


def test_edges_properties():
    n0, n1, n2 = [Node() for _ in range(3)]

    e1 = Edge(from_node=n0, to_node=n1)
    e2 = Edge(from_node=n1, to_node=n2)

    edges = Edges(e1, e2)
    assert len(edges) == 2

    # Edges instances can be iterated
    assert [e1, e2] == [t for t in edges] == list(edges)

    # assert that duplicate edges are removed
    e1a = Edge(from_node=n0, to_node=n1)  # same edge as e1
    assert Edges(e1a, e1) == Edges(e1)
    assert Edges(e1, e2) == Edges(e1, e1, e2, e2, e2)

    # Note: equality currently require same order
    assert Edges(e1, e2) != Edges(e2, e1)


def test_edges_addition():
    n0, n1, n2 = [Node() for _ in range(3)]

    e1a = Edge(from_node=n0, to_node=n1)
    e1b = Edge(from_node=n0, to_node=n1)
    e2 = Edge(from_node=n1, to_node=n2)
    e3 = Edge(from_node=n0, to_node=n2)

    def assert_eq(edges):
        assert len(edges) == 3
        assert Edges(e1a, e2, e3) == edges
        assert Edges(e1b, e2, e3) == edges

    assert_eq(Edges(e1a) + Edges(e2) + Edges(e3))
    assert_eq(Edges(e1b) + Edges(e2) + Edges(e3))

    # check addition and that duplicates are removed in addition
    assert_eq(Edges(e1b, e2, e3) + Edges(e2) + Edges(e3))
    assert_eq(Edges(e1b, e2) + Edges(e2, e2, e2, e3) + Edges(e3))


@pytest.fixture
def test_dag():
    # Return nodes and edges for the following DAG:
    #
    #     n1 ----> n0        n3
    #        \     |
    #          \   |
    #            v v
    #              n2
    #
    nodes = [Node() for _ in range(4)]

    n0, n1, n2, n3 = nodes
    edges = Edges(
        Edge(from_node=n1, to_node=n0),
        Edge(from_node=n0, to_node=n2),
        Edge(from_node=n1, to_node=n2),
    )

    return nodes, edges


def test_edges_int_out_degrees(test_dag):
    (n0, n1, n2, n3), edges = test_dag

    # Test indegree and outdegree values in the test DAG
    assert edges.in_degree(n0) == 1
    assert edges.in_degree(n1) == 0
    assert edges.in_degree(n2) == 2
    assert edges.in_degree(n3) == 0

    assert edges.out_degree(n0) == 1
    assert edges.out_degree(n1) == 2
    assert edges.out_degree(n2) == 0
    assert edges.out_degree(n3) == 0


### test Python ">>"-syntax for defining edges between nodes


def test_syntax_dag_dependency():
    n0, n1, n2, n3, n4 = [Node() for _ in range(5)]

    assert n0 >> n1 == Edge(from_node=n0, to_node=n1)

    assert n0 >> n1 >> n2 == Edges(
        Edge(from_node=n0, to_node=n1),
        Edge(from_node=n1, to_node=n2),
    )

    assert n0 >> n1 >> n2 >> n3 == Edges(
        Edge(from_node=n0, to_node=n1),
        Edge(from_node=n1, to_node=n2),
        Edge(from_node=n2, to_node=n3),
    )

    assert n0 >> n1 >> n2 >> n3 >> n4 == Edges(
        Edge(from_node=n0, to_node=n1),
        Edge(from_node=n1, to_node=n2),
        Edge(from_node=n2, to_node=n3),
        Edge(from_node=n3, to_node=n4),
    )
    assert list(n0 >> n1 >> n2 >> n3 >> n4) == [n0 >> n1, n1 >> n2, n2 >> n3, n3 >> n4]


def test_syntax_edges_shift_node(test_dag):
    (n0, n1, n2, n3), edges = test_dag

    # n2 and n3 are sink nodes, but only n2 is listed in edges. Therefore
    # n3 is not in this list.
    nx = Node()
    assert (edges >> nx) == (edges + Edges(n2 >> nx))


def test_syntax_edges_node_with_two_sink_node_dag():
    # define DAG with two sink:s n1, n2 and one source n0
    n0, n1, n2 = [Node() for _ in range(3)]
    edges = Edges(n0 >> n1, n0 >> n2)

    nx = Node()
    assert (edges >> nx) == (edges + Edges(n1 >> nx, n2 >> nx))
