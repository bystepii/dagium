from abc import abstractmethod, ABC
from typing import List, Dict, Optional

from dagium import Future, LithopsFuture
from operators import Operator


class Executor(ABC):
    """
    Abstract base class for executors
    """

    def __init__(self):
        pass

    @abstractmethod
    def execute(
            self,
            task: Operator,
            input_data: Optional[Dict[str, Future]] = None,
            *args,
            **kwargs
    ) -> Future:
        """
        Execute a task and wait for it to finish

        :param task: Task to execute
        :param input_data: Input data
        :return: Output data of the tasks
        """
        pass


class CallableExecutor(Executor):
    """
    Executor that executes a callable
    """

    def __init__(self):
        super().__init__()

    def execute(
            self,
            task: Operator,
            input_data: Optional[Dict[str, Future]] = None,
            *args,
            **kwargs
    ) -> Future:
        """
        Execute a task and wait for it to finish

        :param task: Task to execute
        :param input_data: Input data
        :return: Output data of the tasks
        """
        future = task(input_data, *args, **kwargs)
        task.executor.wait(future)
        return Future(future)
