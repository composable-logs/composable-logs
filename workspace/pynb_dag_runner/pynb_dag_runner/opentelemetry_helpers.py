from typing import List


def read_key(nested_dict, keys: List[str]):
    first_key, *rest_keys = keys

    if first_key not in nested_dict:
        raise Exception(f"read_key: {first_key} not found")

    if len(rest_keys) == 0:
        return nested_dict[first_key]
    else:
        return read_key(nested_dict[first_key], rest_keys)


def get_span_id(span):
    return read_key(span, ["context", "span_id"])
