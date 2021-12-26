from typing import Awaitable, TypeVar, Protocol

# Helper protocols for encoding Protocols for remote methods on Ray remote actors.

X = TypeVar("X", contravariant=True)
Y = TypeVar("Y", covariant=True)


class RemoteSetFunction(Protocol[X]):
    def remote(self, *args: X) -> None:
        ...


class RemoteGetFunction(Protocol[Y]):
    def remote(self) -> Awaitable[Y]:
        ...
