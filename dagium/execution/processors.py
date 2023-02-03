from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, wait
from typing import List, Dict, Callable, Collection, Sequence

from dagium import Future, MAX_CONCURRENCY
from execution import Executor
from lithops.future import ResponseFuture
from lithops.utils import FuturesList

from operators import Operator
from operators.operator import TaskState

logger = logging.getLogger(__name__)


class Processor(ABC):
    """
    Abstract class for processors
    """

    def __init__(self):
        pass

    @abstractmethod
    def process(
            self,
            tasks: Sequence[Operator],
            executor: Executor,
            input_data: Dict[str, Dict[str, Future]] = None,
            on_future_done: Callable[[Operator, ResponseFuture], None] = None,
    ) -> dict[str, Future]:
        """
        Process a list of tasks

        :param executor:
        :param tasks: List of tasks to process
        :param executor: Executor to use
        :param input_data: Input data
        :param on_future_done: Callback to execute every time a future is done
        :return: Output data of the tasks
        """
        pass


class ThreadPoolProcessor(Processor):
    """
    Processor that uses a thread pool to execute tasks
    """

    def __init__(self, max_concurrency=MAX_CONCURRENCY):
        super().__init__()
        self._max_concurrency = max_concurrency
        self._pool = ThreadPoolExecutor(max_workers=max_concurrency)
        self._futures: Dict[str, Future] = {}

    def process(
            self,
            tasks: Sequence[Operator],
            executor: Executor,
            input_data: Dict[str, Dict[str, Future]] = None,
            on_future_done: Callable[[Operator, ResponseFuture], None] = None,
    ) -> dict[str, Future]:
        """
        Process a list of tasks
        :param executor:
        :param tasks: List of tasks to process
        :param executor: Executor to use
        :param input_data: Input data
        :param on_future_done: Callback to execute every time a future is done
        :return: Futures of the tasks
        :raises ValueError: If there are no tasks to process or if there are more tasks than the maximum parallelism
        """
        if len(tasks) == 0:
            raise ValueError('No tasks to process')

        if len(tasks) > self._max_concurrency:
            raise ValueError(f'Too many tasks to process. Max concurrency is {self._max_concurrency}')

        ex_futures = []

        for task in tasks:
            logger.info(f"Submitting task {task.task_id}")
            task.state = TaskState.RUNNING
            ex_futures.append(self._pool.submit(
                self._process_task,
                task,
                executor,
                input_data[task.task_id] if input_data and task.task_id in input_data else None,
                on_future_done
            ))

        wait(ex_futures)

        return self._futures

    def _process_task(
            self,
            task: Operator,
            executor: Executor,
            input_data: Dict[str, Future] = None,
            on_future_done: Callable[[Operator, ResponseFuture], None] = None,
    ) -> None:
        """
        Process a task

        :param task: Task to process
        :param input_data: Input data
        :param on_future_done: Callback to execute every time a future is done
        """
        future = executor.execute(
            task,
            input_data[task.task_id] if input_data and task.task_id in input_data else None
        )

        self._futures[task.task_id] = future

        task.state = TaskState.SUCCESS
        if isinstance(future, FuturesList) or isinstance(future, list):
            for f in future:
                if f.error():
                    task.state = TaskState.FAILED
                    break
        else:
            if future.error():
                task.state = TaskState.FAILED

        if on_future_done:
            on_future_done(task, future)
        
