import logging
from abc import ABC
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from data import DataObject
from operators import Operator
from operators.operator import TaskState

logger = logging.getLogger(__name__)


class Processor(ABC):
    """
    Abstract class for processors
    """

    def __init__(self, num_threads: int = 10):
        self._num_threads = num_threads
        self._executor = ThreadPoolExecutor(max_workers=num_threads)

    def process(
            self, tasks: list[Operator],
            input_data: dict[str, DataObject] = None,
            output_data: dict[str, DataObject] = None
    ) -> dict[str, DataObject]:
        """
        Process a list of tasks

        :param tasks: List of tasks to process
        :param input_data: Input data
        :param output_data: Output data
        """
        raise NotImplementedError


class ThreadPoolProcessor(Processor):
    """
    Processor that uses a thread pool to process tasks
    """

    def __init__(self, num_threads: int = 10):
        super().__init__(num_threads)
        self._pool = ThreadPoolExecutor(max_workers=num_threads)

    def process(
            self, tasks: list[Operator],
            input_data: dict[str, dict[str, DataObject]] = None,
            output_data: dict[str, DataObject] = None
    ) -> dict[str, Any]:
        if len(tasks) > self._num_threads:
            logger.warning(
                    'The number of tasks is greater than the number of threads. This may cause performance issues.'
            )
        futures = []
        for task in tasks:
            futures.append(
                    self._pool.submit(
                            self._process_task,
                            task,
                            input_data[task.task_id] if input_data and task.task_id in input_data else None,
                            output_data[task.task_id] if output_data and task.task_id in output_data else None
                    )
            )

        results = {}

        for future, task in zip(futures, tasks):
            try:
                results[task.task_id] = future.result()
                logger.info(f'Task {task.task_id} completed successfully')
                task.state = TaskState.SUCCESS
            except Exception as e:
                logger.error(f'Error processing task {task.task_id}: {e}')
                task.state = TaskState.FAILED
                raise e

        return results

    @staticmethod
    def _process_task(
            task: Operator,
            input_data: DataObject = None,
            output_data: DataObject = None
    ) -> Any:
        """
        Process a task

        :param task: Task to process
        """
        logger.info(f"Processing task {task.task_id}")
        future = task(input_data, output_data)
        result = task.executor.get_result(future)
        logger.info(f"Finished processing task {task.task_id}")
        return result
