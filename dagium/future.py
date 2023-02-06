from __future__ import annotations

from typing import Any, Union

from lithops.future import ResponseFuture
from lithops.utils import FuturesList

LithopsFuture = Union[ResponseFuture, FuturesList, list[ResponseFuture]]


class Future:
    def __init__(self, future: LithopsFuture | InputData):
        self._future = future

    def result(self) -> Any:
        if isinstance(self._future, (ResponseFuture, InputData)):
            return self._future.result()
        elif isinstance(self._future, FuturesList):
            return self._future.get_result()
        elif isinstance(self._future, list):
            return [f.result() for f in self._future]
        else:
            raise TypeError(f"Future type {type(self._future)} not supported")


class InputData(Future):
    def __init__(self, data: Any):
        super().__init__(data)
