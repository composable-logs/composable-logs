import json
from pathlib import Path

#
from common_helpers.utils import (
    bytes_to_json,
    del_key,
    ensure_dir_exist,
    del_key,
    iso8601_to_epoch_ms,
)


def test_bytes_to_json():
    python_data = ["Smile 😊😊😊😊😊!!", 123]
    byte_array = bytes(json.dumps(python_data), "utf8")

    assert python_data == bytes_to_json(byte_array)


def test_ensure_dir_exist(tmp_path: Path):
    filepath = tmp_path / "abc" / "def" / "readme.txt"
    test_data = "abc"

    assert filepath == ensure_dir_exist(filepath)
    ensure_dir_exist(filepath).write_text(test_data)
    ensure_dir_exist(filepath).write_text(test_data)

    assert test_data == filepath.read_text()


def test_del_key():
    a_dict = {"a": 1, "b": 2, "c": 3}
    b_dict = del_key(a_dict, "b")

    a_dict["a"] = 12345
    assert {"a": 1, "c": 3} == b_dict


def test_iso8601_to_epoch_ms():
    assert iso8601_to_epoch_ms("2022-03-19T14:47:02.118432Z") == 1647701222118
    assert iso8601_to_epoch_ms("1970-01-01T00:00:00.000000Z") == 0
    assert iso8601_to_epoch_ms("1970-01-01T00:00:00.123456Z") == 123
    assert iso8601_to_epoch_ms("1970-01-01T00:00:01.000000Z") == 1000
