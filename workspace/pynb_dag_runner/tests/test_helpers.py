import random

from pynb_dag_runner.helpers import ranges_intersection, ranges_intersect, flatten


def test_ranges_intersection_random():
    max_r = 20

    def random_range():
        while True:
            a = random.randint(-max_r, max_r)
            b = random.randint(-max_r, max_r)
            if a < b:
                return range(a, b)

    for _ in range(1000):
        r1, r2 = random_range(), random_range()

        # check functions are symmetric in arguments
        for f in [ranges_intersection, ranges_intersect]:
            assert f(r1, r2) == f(r2, r1)

        # check implementations against (slower) set-based implementations
        assert set(r1) & set(r2) == set(ranges_intersection(r1, r2))
        assert (len(set(r1) & set(r2)) > 0) == ranges_intersect(r1, r2)


def test_ranges():
    assert ranges_intersection(range(1, 2), range(-4, 0)) == range(0, 0)
    assert ranges_intersection(range(1, 2), range(-4, 1)) == range(0, 0)
    assert ranges_intersection(range(1, 2), range(-4, 2)) == range(1, 2)
    assert ranges_intersection(range(1, 2), range(2, 10)) == range(2, 2)


def test_flatten():
    def flatten2(xss):
        assert flatten(flatten(xss)) == flatten(xss)
        return flatten(xss)

    assert flatten2([]) == flatten2([[]]) == flatten2([[], [], [[]]]) == []
    assert flatten2([1, [2]]) == [1, 2]
    assert flatten2([1, [2], [[[3]]]]) == [1, 2, 3]
    assert flatten2([[1, 2, 3, [4]]]) == [1, 2, 3, 4]
    assert flatten2(list(range(10))) == list(range(10))
