import logging
from typing import Dict, Set, List

from dagium import Future, MAX_CONCURRENCY
from dagium.dag import DAG
from dagium.execution.processors import Processor, ThreadPoolProcessor
from dagium.operators import TaskState
from execution import Executor, CallableExecutor, Selector, AllSelector
from execution.selectors import MaxConcurrencySelector
from operators import Operator

logger = logging.getLogger(__name__)


class DagExecutor:
    """
    Executor class that is responsible for executing the DAG

    :param dag: DAG to execute
    :param max_concurrency: Maximum number of tasks to execute in parallel, defaults to 10
    :param processor: Processor to use for executing tasks, defaults to DefaultProcessor
    """

    def __init__(
            self,
            dag: DAG,
            max_concurrency=MAX_CONCURRENCY,
            processor: Processor = None,
            executor: Executor = CallableExecutor(),
            selector: Selector = None,
    ):
        self._dag = dag
        self._max_concurrency = max_concurrency
        self._processor = processor or ThreadPoolProcessor(max_concurrency)
        self._executor = executor
        self._selector = selector or MaxConcurrencySelector(max_concurrency)

        self._futures: Dict[str, Future] = dict()
        self._num_final_tasks = 0
        self._dependence_free_tasks: List[Operator] = list()
        self._running_tasks: List[Operator] = list()
        self._finished_tasks: Set[Operator] = set()

    def execute(self) -> Dict[str, Future]:
        """
        Execute the DAG

        :return: A dictionary with the output data of the DAG tasks with the task ID as key
        """
        logger.info(f'Executing DAG {self._dag.dag_id}')

        self._num_final_tasks = len(self._dag.leaf_tasks)
        logger.info(f'DAG {self._dag.dag_id} has {self._num_final_tasks} final tasks')

        self._futures = dict()

        # Start by executing the root tasks
        self._dependence_free_tasks = set(self._dag.root_tasks)
        self._running_tasks = set()
        self._finished_tasks = set()

        # Execute tasks until all tasks have been executed
        while self._dependence_free_tasks or self._running_tasks:
            # Select the tasks to execute
            batch = self._selector.select(list(self._running_tasks), list(self._dependence_free_tasks))

            # Construct the input data for the batch
            input_data = {}
            for task in batch:
                task.state = TaskState.SCHEDULED
                # If the task has parents, then the input data is the output data of the parent tasks
                # passed as a dictionary with the parent task ID as the key and the output data as the value
                if task.parents:
                    input_data[task.task_id] = {
                        parent.task_id: self._futures[parent.task_id] for parent in task.parents
                    }
                else:
                    input_data[task.task_id] = task.input_data

            # Add the batch to the running tasks
            set_batch = set(batch)
            self._running_tasks |= set_batch

            # Call the processor to execute the batch
            futures = self._processor.process(batch, self._executor, input_data)

            self._running_tasks -= set_batch
            self._dependence_free_tasks -= set_batch
            self._finished_tasks |= set_batch

            for task in batch:
                self._futures[task.task_id] = futures[task.task_id]
                for child in task.children:
                    if child.parents.issubset(self._finished_tasks):
                        self._dependence_free_tasks.add(child)

        return self._futures

    def shutdown(self):
        """
        Shutdown the executor
        """
        self._processor.shutdown()
