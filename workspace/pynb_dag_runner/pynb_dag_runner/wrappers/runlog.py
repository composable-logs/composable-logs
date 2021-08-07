from typing import Set, Dict, Tuple, Iterable, Any


class Runlog:
    """
    Immutable append-only dict-like storage for runlog:s (input parameters, outputs).

    All keys are required to be strings
    """

    def __init__(self, **log_dict: Dict[str, Any]):
        assert all(isinstance(k, str) for k in log_dict.keys())

        self._log_dict: Dict[str, Any] = log_dict

    def __getitem__(self, k: str) -> Any:
        try:
            return self._log_dict[k]
        except KeyError:
            raise Exception(f"Key {k} not in {self._log_dict.keys()}")

    def keys(self) -> Set[str]:
        return set(self._log_dict.keys())

    def items(self) -> Iterable[Tuple[str, Any]]:
        return self._log_dict.items()

    def as_dict(self, prefix_filter: str = "") -> Dict[str, Any]:
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
        assert len(self.keys() & dict_to_append.keys()) == 0

        return Runlog(**{**self.as_dict(), **dict_to_append})

    def __repr__(self) -> str:
        values = ", ".join(f"{k}={repr(v)}" for k, v in sorted(self.items()))
        return f"Runlog({values})"

    def __eq__(self, other) -> bool:
        return isinstance(other, Runlog) and self.as_dict() == other.as_dict()
