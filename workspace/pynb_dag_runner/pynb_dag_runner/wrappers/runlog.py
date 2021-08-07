from typing import Set, Dict, Any


class Runlog:
    """
    Immutable append-only dict-like storage for runlog:s (input parameters, outputs).

    All keys are required to be strings
    """

    def __init__(self, **log_dict: Dict[str, Any]):
        assert all(isinstance(k, str) for k in log_dict.keys())

        self.log_dict = log_dict

    def __getitem__(self, k: str) -> Any:
        try:
            return self.log_dict[k]
        except KeyError:
            raise Exception(f"Key {k} not in {self.log_dict.keys()}")

    def keys(self) -> Set[str]:
        return set(self.log_dict.keys())

    def items(self) -> Dict[str, Any]:
        return self.log_dict.items()

    def as_dict(self, prefix_filter=""):
        """
        Return values as dict.

        Optionally include only keys that start with `prefix_filter`
        """
        return {
            k.replace(prefix_filter, "", 1): v
            for k, v in self.items()
            if k.startswith(prefix_filter)
        }

    def add(self, **dict_to_append: Dict[str, Any]) -> "Runlog":
        assert len(self.log_dict.keys() & dict_to_append.keys()) == 0

        return Runlog(**{**self.log_dict, **dict_to_append})

    def __repr__(self) -> str:
        values = ", ".join(f"{k}={repr(v)}" for k, v in sorted(self.items()))
        return f"Runlog({values})"

    def __eq__(self, other) -> bool:
        return isinstance(other, Runlog) and self.log_dict == other.log_dict
