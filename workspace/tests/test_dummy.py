import pytest


def test_single_assert():
    assert 1 == 1


@pytest.mark.parametrize("x", range(10))
def test_dummy_parameterized_test(x):
    assert 0 <= x < 10
