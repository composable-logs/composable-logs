from pathlib import Path
import random

#
import pytest

#
from pynb_dag_runner.helpers import (
    range_is_empty,
    range_intersection,
    range_intersect,
    flatten,
    compose,
    write_json,
    read_json,
    one,
    pairs,
)


def test_range_empty():
    assert range_is_empty(range(0, 0))
    assert not range_is_empty(range(0, 10))


def test_range_intersection_random():
    max_r = 25

    def random_range():
        while True:
            a = random.randint(-max_r, max_r)
            b = random.randint(-max_r, max_r)
            return range(a, b)

    for _ in range(1000):
        r1, r2 = random_range(), random_range()

        # check functions are symmetric in arguments
        for f in [range_intersection, range_intersect]:
            assert f(r1, r2) == f(r2, r1)

        # check implementations against (slower) set-based implementations
        assert set(r1) & set(r2) == set(range_intersection(r1, r2))
        assert (len(set(r1) & set(r2)) > 0) == range_intersect(r1, r2)


def test_ranges():
    assert range_intersection(range(1, 2), range(-4, 0)) == range(0, 0)
    assert range_intersection(range(1, 2), range(-4, 1)) == range(0, 0)
    assert range_intersection(range(1, 2), range(-4, 2)) == range(1, 2)
    assert range_intersection(range(1, 2), range(2, 10)) == range(2, 2)

    # intersection and empty ranges
    assert range_is_empty(range_intersection(range(1, 2), range(0, -10)))
    assert range_is_empty(range_intersection(range(-2, 0), range(0, -10)))
    assert range_is_empty(range_intersection(range(1, 10), range(100, 1000)))


def test_flatten():
    def flatten2(xss):
        assert flatten(flatten(xss)) == flatten(xss)
        return flatten(xss)

    assert flatten2([]) == flatten2([[]]) == flatten2([[], [], [[]]]) == []
    assert flatten2([1, [2]]) == [1, 2]
    assert flatten2([1, [2], [[[3]]]]) == [1, 2, 3]
    assert flatten2([[1, 2, 3, [4]]]) == [1, 2, 3, 4]
    assert flatten2(list(range(10))) == list(range(10))


def test_compose():
    f0 = lambda: f"f0()"
    f = lambda x: f"f({x})"
    g = lambda x: f"g({x})"
    h1 = lambda x: f"h1({x})"
    h2 = lambda x, y: f"h2({x}, {y})"

    assert compose(f0)() == "f0()" == f0()
    assert compose(f, f0)() == "f(f0())" == f(f0())
    assert compose(f)("x") == "f(x)" == f("x")
    assert compose(f, g)("y") == "f(g(y))" == f(g("y"))
    assert compose(f, g, f0)() == "f(g(f0()))" == f(g(f0()))
    assert compose(f, g, h1)("z") == "f(g(h1(z)))" == f(g(h1("z")))
    assert compose(f, g, h2)("u", "v") == "f(g(h2(u, v)))" == f(g(h2("u", "v")))
    assert compose(h2)("u", "v") == "h2(u, v)" == h2("u", "v")


def test_pairs():
    assert pairs([1, 2]) == [(1, 2)]
    assert pairs([1, 2, 3]) == [(1, 2), (2, 3)]
    assert pairs([1, 2, 3, 4]) == [(1, 2), (2, 3), (3, 4)]


def test_write_read_json(tmp_path: Path):
    test_obj = [42, {"42": [0, None, []]}]
    test_path = tmp_path / "test.json"

    write_json(test_path, test_obj)
    assert read_json(test_path) == test_obj


def test_one():
    assert one([1]) == 1


def test_one_raises_exception():
    with pytest.raises(Exception):
        assert one([1, 2])
