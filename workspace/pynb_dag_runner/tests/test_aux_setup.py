from pynb_dag_runner.aux import f


def test_dummy():
    assert 1 == 1


def test_imported_f():
    assert f() == 123
