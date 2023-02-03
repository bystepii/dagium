from __future__ import annotations

import inspect
from typing import Any, Callable, Union, Dict

from dagium import Future
from lithops import FunctionExecutor
from lithops.utils import FuturesList

from dagium.operators.operator import Operator


class Map(Operator):
    """
    Map operator

    :param task_id: Task ID
    :param executor: Executor to use
    :param map_func: Function applied to each element
    :param input_data: Input data for the operator
    :param metadata: Metadata to pass to the operator
    :param args: Arguments to pass to the operator
    :param kwargs: Keyword arguments to pass to the operator
    """

    def __init__(
            self,
            task_id: str,
            executor: FunctionExecutor,
            map_func: Callable[[Future], Any] | Callable[[Future, str], Any],
            input_data: Dict[str, Future] | Future = None,
            metadata: Dict[str, Any] = None,
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
        # outputs = {
        # k: DataObjectFactory.create_output_data_object(
        # output_data.bucket, output_data.path + '_' + k,
        # output_data.storage,
        #    output_data.metadata
        # )
        #    for k in input_data.keys()
        # }
        # iterdata = [{'input_data': input_data[i], 'output_data': outputs[i]} for i in input_data.keys()]

        sig = inspect.signature(self._map_func)

        input_data = input_data or self._input_data

        if isinstance(input_data, dict):
            iterdata = [(v, k) for k, v in input_data.items()]
        else:
            iterdata = input_data

        return self._executor.map(
            self._wrap(self._map_func, input_data),
            iterdata,
            *self._args,
            **self._kwargs
        )

    def _wrap(
            self,
            func: Callable[[Future], Any] | Callable[[Future, str], Any],
            in_data: Dict[str, Future] | Future = None,
    ) -> Callable[[Future], Any] | Callable[[str, Future], Any]:
        """
        Wrap a function to be executed in the operator

        :param func: Function to wrap
        :param in_data: Input data
        :return: Wrapped function
        """

        def wrapped_func(input_data: Future, parend_id: str = None):
            return func(input_data, parend_id)

        return wrapped_func
