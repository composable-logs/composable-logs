import pytest

#
from pynb_dag_runner.wrappers.runlog import Runlog


def test_runlog_as_dict():
    a_dict = {"input.a": 1, "input.b": 2, "out.foo": "bar"}

    assert Runlog(**a_dict).as_dict() == {**a_dict}
    assert Runlog(**a_dict).as_dict(prefix_filter="input.") == {"a": 1, "b": 2}


def test_runlog_read_as_dict():
    a_dict = {"a": 1, "b": "2", "c": [1, 2, 3]}

    r = Runlog(**a_dict)
    for k, v in a_dict.items():
        assert r[k] == v

    assert r.keys() == a_dict.keys()
    assert list(r.items()) == list(a_dict.items())

    with pytest.raises(Exception):
        r["non-existing key"]


def test_runlog_is_immmutable():
    r = Runlog()
    with pytest.raises(Exception):
        r["a_key"] = 3


def test_runlog_eq():
    r1 = Runlog(a=1, b="2", c=[1, 2, 3])
    r2 = Runlog(a=1, b="2", c=[1, 2, 4])

    assert r1 != r2
    assert Runlog(a=[1, 2]) == Runlog(a=[1, 2])


def test_runlog_add():
    assert Runlog(a=1, b="2").add(c=[1, 2, 3]) == Runlog(a=1, b="2", c=[1, 2, 3])


def test_repr():
    r = Runlog(a=1, b="2", c=[1, 2, 3])

    assert repr(r) == """Runlog(a=1, b='2', c=[1, 2, 3])"""
    assert eval(repr(r)) == r
