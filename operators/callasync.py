from typing import Any

from lithops.future import ResponseFuture

from data import DataObject
from operators import Operator, Executor


class CallAsync(Operator):
    """
    CallAsync operator

    :param task_id: Task ID
    :param func: Function to call
    :param args: Arguments to pass to the operator
    :param kwargs: Keyword arguments to pass to the operator
    """

    def __init__(
            self,
            task_id: str,
            executor: Executor,
            func: callable,
            input_data: DataObject = None,
            output_data: DataObject = None,
            metadata: dict[str, Any] = None,
            *args,
            **kwargs
    ):
        super().__init__(
                task_id,
                executor,
                input_data,
                output_data,
                metadata,
                *args,
                **kwargs
        )
        self._func = func

    def __call__(
            self,
            input_data: dict[str, DataObject] = None,
            output_data: DataObject = None,
    ) -> ResponseFuture:
        input_data, output_data = super().get_input_output(input_data, output_data)

        # If the input data is a dict or a list, it must be wrapped in a tuple,
        # because the lithops has a special treatment for them.
        if isinstance(input_data, dict) or isinstance(input_data, list):
            input_data = (input_data,)
        return self._executor.call_async(
                super().input_data_wrapper(self._func),
                input_data,
                *self._args,
                **self._kwargs
        )
