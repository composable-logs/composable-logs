from typing import Generic, Tuple, Iterable, Set, TypeVar

A = TypeVar("A")


def iter_to_set(xs: Iterable[A]) -> Set[A]:
    """
    Convert iterator into set, and raise exception if iterator contain duplicate values.
    """
    ys = list(xs)
    result = set(ys)
    if len(ys) != len(result):
        raise ValueError("Internal error: result should not contain duplicates")
    return result


Node = TypeVar("Node")
# Represent graph edge (n1 -> n2) as tuple (n1, n2)
Edge = Tuple[Node, Node]


class Graph(Generic[Node]):
    def __init__(self, edges: Set[Edge]):
        """
        Immutable graph from a set of edges (see above).
        """
        self.edges: Set[Edge] = edges

    def children_of(self, node: Node) -> Set[Node]:
        return iter_to_set(
            to_node for from_node, to_node in self.edges if from_node == node
        )

    def parents_of(self, node: Node) -> Set[Node]:
        return iter_to_set(
            from_node for from_node, to_node in self.edges if node == to_node
        )
