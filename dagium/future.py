from __future__ import annotations

from abc import ABC
from typing import Any, Union, List, Optional

from lithops.future import ResponseFuture
from lithops.utils import FuturesList

LithopsFuture = Union[ResponseFuture, FuturesList, List[ResponseFuture]]


class Future:
    def __init__(self, future: Optional[LithopsFuture] = None):
        if future is not None:
            # self.__class__ = type(future.__class__.__name__, (self.__class__, future.__class__), {})
            # self.__dict__ = future.__dict__
            self.__future = future

    def result(self) -> Any:
        if isinstance(self.__future, ResponseFuture):
            return self.__future.result()
        elif isinstance(self.__future, FuturesList):
            return self.__future.get_result()
        elif isinstance(self.__future, list):
            return [f.result() for f in self.__future]
        else:
            raise TypeError(f"Future type {type(self.__future)} not supported")

    def error(self) -> bool:
        if isinstance(self.__future, ResponseFuture):
            return self.__future.error
        elif isinstance(self.__future, (FuturesList, list)):
            return any([f.error for f in self.__future])
        else:
            raise TypeError(f"Future type {type(self.__future)} not supported")

    def __getattr__(self, item):
        if item in vars(self):
            return getattr(self, item)
        elif '__future' in vars(self) and item in vars(self.__future):
            return getattr(self.__future, item)
        raise AttributeError(f"Future object has no attribute {item}")


class InputData(Future):
    def __init__(self, data: Any):
        super().__init__()
        self._data = data

    def result(self) -> Any:
        return self._data

    def error(self) -> bool:
        return False
