from __future__ import annotations

import logging
from abc import ABC

from lithops.future import ResponseFuture
from lithops.utils import FuturesList

from data import InputDataObject, OutputDataObject
from operators import Operator
from operators.operator import TaskState

logger = logging.getLogger(__name__)


class Processor(ABC):
    """
    Abstract class for processors

    :param max_parallelism: Maximum number of tasks to execute in parallel
    """

    def __init__(self, max_parallelism: int = 10):
        self._max_parallelism = max_parallelism

    def process(
            self, tasks: list[Operator],
            input_data: dict[str, dict[str, InputDataObject]] = None,
            output_data: dict[str, OutputDataObject] = None
    ) -> dict[str, OutputDataObject]:
        """
        Process a list of tasks

        :param tasks: List of tasks to process
        :param input_data: Input data
        :param output_data: Output data
        :return: Output data of the tasks
        """
        raise NotImplementedError


class DefaultProcessor(Processor):
    """
    Default processor for tasks
    """

    def process(
            self,
            tasks: list[Operator],
            input_data: dict[str, dict[str, InputDataObject]] = None,
            output_data: dict[str, OutputDataObject] = None
    ) -> dict[str, OutputDataObject]:
        """
        Process a list of tasks
        :param tasks: List of tasks to process
        :param input_data: Input data
        :param output_data: Output data
        :return: Output data of the tasks
        :raises ValueError: If there are no tasks to process or if there are more tasks than the maximum parallelism
        """
        if len(tasks) == 0:
            raise ValueError('No tasks to process')

        if len(tasks) > self._max_parallelism:
            raise ValueError(f'Too many tasks to process. Max parallelism is {self._max_parallelism}')

        futures = {}

        for task in tasks:
            futures[task] = self._process_task(
                task,
                input_data[task.task_id] if input_data and task.task_id in input_data else None,
                output_data[task.task_id] if output_data and task.task_id in output_data else None
            )

        # wait for all tasks to complete
        logger.info('Waiting for batch to complete')
        tasks[0].executor.wait(list(futures.values()))

        # TODO: Handle exceptions
        for task in tasks:
            task.state = TaskState.SUCCESS
            logger.info(f'Task {task.task_id} completed successfully')

        return {task.task_id: task.output_data for task in tasks}

    @staticmethod
    def _process_task(
            task: Operator,
            input_data: InputDataObject = None,
            output_data: OutputDataObject = None
    ) -> ResponseFuture | FuturesList:
        """
        Process a task

        :param task: Task to process
        """
        logger.info(f"Submitting task {task.task_id}")
        task.state = TaskState.RUNNING
        return task(input_data, output_data)
