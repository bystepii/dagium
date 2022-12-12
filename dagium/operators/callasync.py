from typing import Any

from lithops import FunctionExecutor
from lithops.future import ResponseFuture

from dagium.data import DataObject
from dagium.operators.operator import Operator
from data import InputDataObject, OutputDataObject


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
            executor: FunctionExecutor,
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
            input_data: dict[str, InputDataObject] = None,
            output_data: OutputDataObject = None,
    ) -> ResponseFuture:
        input_data, output_data = super().get_input_output(input_data, output_data)
        return self._executor.call_async(
                self._func,
                {'input_data': input_data, 'output_data': output_data},
                *self._args,
                **self._kwargs
        )
