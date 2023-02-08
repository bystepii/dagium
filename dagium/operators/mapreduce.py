from __future__ import annotations

from typing import Any, Callable, Dict, Optional

from dagium import Future
from lithops import FunctionExecutor
from lithops.utils import FuturesList

from dagium.operators.operator import Operator


class MapReduce(Operator):
    """
    Map operator

    :param task_id: Task ID
    :param executor: Executor to use
    :param map_func: Function applied to each element
    :param reduce_func: Function applied to the results of the map function
    :param input_data: Input data for the operator
    :param metadata: Metadata to pass to the operator
    :param args: Arguments to pass to the operator
    :param kwargs: Keyword arguments to pass to the operator
    """

    def __init__(
            self,
            task_id: str,
            executor: FunctionExecutor,
            map_func: Callable[[Future, ...], Any] | Callable[[Future, str, ...], Any],
            reduce_func: Callable,
            input_data: Optional[Dict[str, Future] | Future] = None,
            metadata: Optional[Dict[str, Any]] = None,
            *args,
            **kwargs
    ):
        super().__init__(
            task_id,
            executor,
            input_data,
            metadata,
            *args,
            **kwargs
        )
        self._map_func = map_func
        self._reduce_func = reduce_func

    # TODO: Implement this, it's not working yet
    def __call__(
            self,
            input_data: Dict[str, Future] = None,
            *args,
            **kwargs
    ) -> FuturesList:
        """
        Execute the operator and return a future object.

        :param input_data: Input data
        :return: the future object
        """

        input_data = input_data or self._input_data

        if isinstance(input_data, dict):
            iterdata = [(v, k) for k, v in input_data.items()]
        else:
            iterdata = input_data

        return self._executor.map_reduce(
            self._wrap(self._map_func, input_data),
            iterdata,
            self._reduce_func,
            *self._args,
            **self._kwargs
        )

    def _wrap(
            self,
            func: Callable[[Future, ...], Any] | Callable[[Future, str, ...], Any],
            in_data: Optional[Dict[str, Future]] = None,
    ) -> Callable[[Future], Any] | Callable[[str, Future], Any]:
        """
        Wrap a function to be executed in the operator

        :param func: Function to wrap
        :param in_data: Input data
        :return: Wrapped function
        """

        def wrapped_func(input_data: Future, parent_id: Optional[str] = None, *args, **kwargs) -> Any:
            return func(input_data, parent_id, *args, **kwargs)

        return wrapped_func
