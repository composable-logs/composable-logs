from typing import Awaitable, TypeVar, Protocol

# We can not directly use TaskP-protocol since our Task-class will be remote Ray actor.
# The below encode a Ray actor with the above (remote) class methods.

X = TypeVar("X", contravariant=True)
Y = TypeVar("Y", covariant=True)


class RemoteSetFunction(Protocol[X]):
    def remote(self, *args: X) -> None:
        ...


class RemoteGetFunction(Protocol[Y]):
    def remote(self) -> Awaitable[Y]:
        ...
