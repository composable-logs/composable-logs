from pynb_dag_runner.tasks.task_opentelemetry_logging import (
    encode_to_wire,
    decode_from_wire,
)

# --
import pytest

# ---- test encode_to_wire, decode_to_wire ----


def test__encode_decode_to_wire__explicit_examples():
    test_bytes = bytes([0, 1, 2, 3, 4, 5])

    assert encode_to_wire("foo") == ("utf-8", "foo")
    assert encode_to_wire(test_bytes) == ("binary/base64", "AAECAwQF")

    assert decode_from_wire("utf-8", "foo") == "foo"
    assert decode_from_wire("binary/base64", "AAECAwQF") == test_bytes


def test__encode_decode_to_wire__is_identity():
    for msg in [
        "test-text-message",
        bytes([0, 1, 2, 3]),
        bytes(1000 * list(range(256))),
    ]:
        assert msg == decode_from_wire(*encode_to_wire(msg))


def test__encode_decode_to_wire__exceptions_for_invalid_data():
    for invalid_data in [None, 123, {"a": 123}, 123.4, True]:
        with pytest.raises(ValueError):
            encode_to_wire(invalid_data)

    with pytest.raises(ValueError):
        decode_from_wire("utf8", None)
