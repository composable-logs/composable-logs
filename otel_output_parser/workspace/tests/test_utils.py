import json
from pathlib import Path

#
from common_helpers.utils import bytes_to_json, ensure_dir_exist


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
