"""
The Node, Edge and Edges classes defined below allow us to represent DAGs
(Directed Acyclic Graphs).

In our application, we use DAGs to represent task dependencies. For this reason
the below implementation forbids self-loops (an edge from a node to itself). Otherwise
the below implementation is independent of what Node:s represent.

For terminology, see eg

- https://en.wikipedia.org/wiki/Directed_graph
- https://en.wikipedia.org/wiki/Directed_acyclic_graph
- https://en.wikipedia.org/wiki/Vertex_(graph_theory)

"""

from typing import List, Any


class Node:
    def __rshift__(self, other):
        if isinstance(other, Node):
            # Support syntax n1 >> n2 to represent Edge(n1, n2) when n1 and n2 are Node
            # instances (or instances of a subclass of Node).
            return Edge(from_node=self, to_node=other)
        else:
            raise Exception(f"Unknown argument self={self}, other={other}")


class Edge:
    """
    Represent an edge between two Node:s in a graph.
    """

    def __init__(self, from_node: Node, to_node: Node):
        # Ensure there are no self-loops. (In task-dependency graphs these would)
        # indicate that a task depends on itself)
        if from_node == to_node:
            raise Exception("'from_node' and 'to_node' must be distinct")

        self.from_node = from_node
        self.to_node = to_node

    def __repr__(self) -> str:
        return f"Edge(from={self.from_node}, to={self.to_node})"

    def __rshift__(self, other):
        if isinstance(other, Node):
            # Support syntax Edge(n1, n2) >> n3 for Edges(Edge(n1, n2), Edge(n2, n3)) for
            # Node instances n1, n2, n3.
            return Edges(self, Edge(from_node=self.to_node, to_node=other))
        else:
            raise Exception(f"Unknown argument self={self}, other={other}")

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, Edge)
            and (self.from_node == other.from_node)
            and (self.to_node == other.to_node)
        )


class Edges:
    def __init__(self, *args: Edge):
        """
        Edges-instances store a collection of Edge:s.

        The provided list of edges may be empty or include duplicates. In the latter
        case any duplicates are removed.
        """
        self._edges: List[Edge] = []

        for arg in args:
            if isinstance(arg, Edge):
                if arg not in self._edges:
                    self._edges.append(arg)
            else:
                raise Exception(f"Unknown input: {str(args)}")

    def __repr__(self) -> str:
        return f"Edges({self._edges})"

    def __rshift__(self, other) -> "Edges":
        if isinstance(other, Node):
            """
            Support syntax edges >> n when edges is instance of Edges and n is of type
            Node. This returns a new Edges-instance that to edges appends edges from all
            sink nodes in edges (=nodes where out_degree=0) to n.

            Eg., this allows us to write n1 >> n2 >> n3 and this expands to
            Edges(Edge(n1, n2), Edge(n2, n3)) and similarly for n1 >> n2 >> n3 >> n4,
            etc.
            """
            sink_nodes = []
            for edge in self:
                if edge.to_node not in sink_nodes:
                    sink_nodes.append(edge.to_node)

            return self + Edges(
                *[
                    Edge(
                        from_node=to_node,
                        to_node=other,
                    )
                    for to_node in sink_nodes
                    if self.out_degree(to_node) == 0
                ]
            )
        else:
            raise Exception(f"Unknown argument self={self}, other={other}")

    def __len__(self) -> int:
        """
        Return number of unique edges
        """
        return len(list(self._edges))

    def __iter__(self):
        """
        Support iteration, eg. [e for e in Edges(e1, e2)]
        """
        return iter(self._edges)

    def in_degree(self, node: Node) -> int:
        """
        Return indegree for a given node (=nr of edges that connect *to* the node)

        Notes:
         - in_degree = 0 if the given node is not referenced for any Edge:s in this
           Edges-instance.
         - The below computation require that this class does not list duplicate edges.

        """
        return len([edge for edge in self if edge.to_node == node])

    def out_degree(self, node: Node) -> int:
        """
        Return outdegree for a given node (=nr of edges that emit *from* the node)
        """
        return len([edge for edge in self if edge.from_node == node])

    def __eq__(self, other):
        return all(
            [
                isinstance(other, Edges),
                list(self) == list(other),
            ]
        )

    def __add__(self, other: "Edges") -> "Edges":
        """
        Support syntax edges1 + edges2 to join two Edges-instances.
        """
        return Edges(*(list(self) + list(other)))
