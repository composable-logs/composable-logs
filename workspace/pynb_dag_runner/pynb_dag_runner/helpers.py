def ranges_intersection(range1, range2):
    """
    Return intersection range of two non-empty Python ranges.
    """
    for r in [range1, range2]:
        assert r.step == 1
        assert r.start < r.stop

    last_start: int = max(range1.start, range2.start)
    first_stop: int = min(range1.stop, range2.stop)

    return range(last_start, first_stop)


def ranges_intersect(range1, range2) -> bool:
    """
    Return bool-ean representing whether two non-empty Python ranges intersect
    """
    range12 = ranges_intersection(range1, range2)
    return range12.start < range12.stop
