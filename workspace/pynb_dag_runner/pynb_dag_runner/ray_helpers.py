from typing import TypeVar, Generic, Callable

import ray


A = TypeVar("A")
B = TypeVar("B")


class Future(Generic[A]):
    """
    Helper class that can be used to add type hints for Ray Futures (or object ref:s).

    We can not type check all of our Ray code, but type hints can be used to document
    the code and at least catch some type errors.
    """

    def map(future: "Future[A]", f: Callable[[A], B]) -> "Future[B]":
        """
        Return a new Future with the value of future mapped through f.
        """

        @ray.remote(num_cpus=0)
        def do_map(future_value: A) -> B:
            return f(future_value)

        return do_map.remote(future)
