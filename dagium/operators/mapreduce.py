from typing import Any

from lithops import FunctionExecutor
from lithops.utils import FuturesList

from dagium.operators.operator import Operator
from data import InputDataObject, OutputDataObject


class MapReduce(Operator):
    """
    Map operator

    :param task_id: Task ID
    :param executor: Executor to use
    :param map_func: Function applied to each element
    :param reduce_func: Function applied to the results of the map function
    :param input_data: Input data for the operator
    :param output_data: Output data for the operator
    :param metadata: Metadata to pass to the operator
    :param args: Arguments to pass to the operator
    :param kwargs: Keyword arguments to pass to the operator
    """

    def __init__(
            self,
            task_id: str,
            executor: FunctionExecutor,
            map_func: callable,
            reduce_func: callable,
            input_data: InputDataObject = None,
            output_data: OutputDataObject = None,
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
        self._map_func = map_func
        self._reduce_func = reduce_func

    # TODO: Implement this, it's not working yet
    def __call__(
            self,
            input_data: dict[str, InputDataObject] = None,
            output_data: OutputDataObject = None,
    ) -> FuturesList:
        """
        Execute the operator and return a future object.

        :param input_data: Input data
        :param output_data: Output data
        :return: the future object
        """
        input_data, output_data = super().get_input_output(input_data, output_data)
        return self._executor.map_reduce(
            self._map_func,
            {'input_data': input_data, 'output_data': output_data},
            self._reduce_func,
            *self._args,
            **self._kwargs
        )
