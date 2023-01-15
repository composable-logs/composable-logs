# --- TODO: this file can be deleted after move to new Ray Workflow based API ---

from typing import Any, Awaitable, Generic, TypeVar, Protocol

# -
import ray

# Helper protocols for encoding Protocols for remote methods on Ray remote actors.

X = TypeVar("X", contravariant=True)
Y = TypeVar("Y", covariant=True)


class RemoteSetFunction(Generic[X]):
    def remote(self, *args: X) -> None:
        ...


class RemoteGetFunction(Generic[Y]):
    def remote(self) -> Any:  # return should be Awaitable[Y]
        ...
