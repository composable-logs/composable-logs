from otel_output_parser.common_helpers.graph import Graph


def test_one_node_graph():
    g = Graph(set([(1, 1)]))

    assert g.children_of(1) == {1}
    assert g.parents_of(1) == {1}


def test_graph_parents_children():
    """
    Test graph:

        1 ---> 2 ---> 3
          \     \
           \     \
            -> 4 ---> 5

    """
    g = Graph({(1, 2), (2, 3), (1, 4), (4, 5), (2, 5)})

    assert g.children_of(1) == {2, 4}
    assert g.children_of(2) == {3, 5}
    assert g.children_of(3) == set()
    assert g.children_of(4) == {5}
    assert g.children_of(5) == set()
    assert g.parents_of(1) == set()
    assert g.parents_of(2) == {1}
    assert g.parents_of(3) == {2}
    assert g.parents_of(4) == {1}
    assert g.parents_of(5) == {2, 4}

    assert g.all_children_of(1) == {2, 3, 4, 5}
    assert g.all_children_of(2) == {3, 5}
    assert g.all_children_of(3) == set()
    assert g.all_children_of(4) == {5}
    assert g.all_children_of(5) == set()
