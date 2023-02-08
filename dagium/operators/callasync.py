from __future__ import annotations

from typing import Any, Dict, Callable, Union, Optional

from dagium import Future
from dagium.operators.operator import Operator
from lithops import FunctionExecutor
from lithops.future import ResponseFuture


class CallAsync(Operator):
    """
    CallAsync operator

    :param task_id: Task ID
    :param executor: Executor to use
    :param func: Function to call
    :param input_data: Input data for the operator
    :param metadata: Metadata to pass to the operator
    :param args: Arguments to pass to the operator
    :param kwargs: Keyword arguments to pass to the operator
    """

    def __init__(
            self,
            task_id: str,
            executor: FunctionExecutor,
            func: Callable[[Dict[str, Future], ...], Any],
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
        self._func = func

    def __call__(
            self,
            input_data: Dict[str, Future] = None,
            *args,
            **kwargs
    ) -> ResponseFuture:
        """
        Execute the operator and return a future object.

        :param input_data: Input data
        :return: the future object
        """
        return self._executor.call_async(
            self._wrap(self._func, input_data or self._input_data),
            {'input_data': input_data or self._input_data, 'args': args, 'kwargs': kwargs},
            *self._args,
            **self._kwargs
        )

    def _wrap(
            self,
            func: Callable[[Dict[str, Future], ...], Any],
            in_data: Dict[str, Future]
    ) -> Callable:
        """
        Wrap a function to be executed in the operator

        :param func: Function to wrap
        :param in_data: Input data
        :return: Wrapped function
        """
        def wrapped_func(input_data: Dict[str, Future], *args, **kwargs) -> Any:
            return func(input_data, *args, **kwargs)

        return wrapped_func
