from typing import Any

from lithops.utils import FuturesList

from data import DataObject
from operators import Operator, Executor


class Map(Operator):
    """
    Map operator

    :param task_id: Task ID
    :param map_func: Function to map
    :param metadata: Metadata to pass to the operator
    :param args: Arguments to pass to the operator
    :param kwargs: Keyword arguments to pass to the operator
    """

    def __init__(
            self,
            task_id: str,
            executor: Executor,
            map_func: callable,
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
        self._map_func = map_func

    def __call__(
            self,
            input_data: dict[str, DataObject] = None,
            output_data: DataObject = None,
    ) -> FuturesList:
        """
        Execute the operator.
        :param context: Context of the execution
        """
        input_data, output_data = super().get_input_output(input_data, output_data)
        return self._executor.map(
                super().input_data_wrapper(self._map_func),
                input_data,
                *self._args,
                **self._kwargs
        )
