import logging
from concurrent.futures import ThreadPoolExecutor
from threading import Semaphore

from dag import DAG
from execution.context import Context
from operators.operator import Operator

logger = logging.getLogger(__name__)


class DagExecutor:
    """
    Executor class that is responsible for executing the DAG

    :param num_threads: Number of threads to use for executing the DAG
    """

    def __init__(self, dag: DAG, num_threads: int = 10):
        self._dag = dag
        self._num_threads = num_threads
        self._context = Context([task.task_id for task in dag.tasks])
        self._threads = ThreadPoolExecutor(max_workers=self._num_threads)
        self._futures = []
        self._config = {'lithops': {'backend': 'localhost', 'storage': 'localhost'}}
        self._sem = None
        self._num_final_tasks = None

    def execute(self):
        """
        Execute the DAG

        """
        logger.info(f'Executing DAG {self._dag.dag_id}')

        self._num_final_tasks = len(self._dag.leaf_tasks)
        logger.info(f'DAG {self._dag.dag_id} has {self._num_final_tasks} final tasks')

        # Semaphore to keep track of the number of final tasks that have completed
        # It is initialized to 0 and is released (incremented) when a final task completes
        self._sem = Semaphore(0)

        # Start executing the root tasks of the DAG
        for task in self._dag.root_tasks:
            self._execute_task(task)

    def _execute_task(self, task: Operator):
        """
        Execute a task

        :param task: Task to execute
        """
        logger.info(f'Executing task {task.task_id}')
        self._context.futures[task.task_id] = task(
                self._context.input_data[task.task_id],
                self._context.output_data[task.task_id]
        )
        self._futures.append(self._threads.submit(self._wait_for_futures, task, self._context))

    def _wait_for_futures(self, task: Operator, context: Context):
        """
        Wait for the futures of a task to complete and then execute the children of the task if they are ready.
        This method is executed in a separate thread.

        :param task: Task to wait for
        :param context: Context to use
        """
        logger.info(f'Waiting for task {task.task_id} to complete')

        result = task.executor.get_result(context.futures[task.task_id])

        task.output_data.put(result)
        context.output_data[task.task_id] = task.output_data

        logger.info(f'Task {task.task_id} completed')
        context.done[task.task_id] = True

        # set the output data of the task as the input data of the children
        for child in task.children:
            context.input_data[child.task_id] = context.output_data[task.task_id]

        # If the task has no children, it is a final task, and we can release the semaphore
        if not task.children:
            self._sem.release()
            return

        # Check if the children of the task are ready to execute
        for child in task.children:
            # If the child has all its parents done, it is ready to execute
            if all(self._context.done[parent.task_id] for parent in child.parents):
                logger.info(f'Child task {child.task_id} is ready to execute')
                self._execute_task(child)

    def wait(self):
        """
        Wait for all tasks to complete
        """
        logger.info(f'Waiting for {self._num_final_tasks} tasks to complete')

        # Wait for the number of final tasks to complete using the semaphore
        for _ in range(self._num_final_tasks):
            self._sem.acquire()

    def shutdown(self):
        """
        Shutdown the executor
        """
        logger.info('Shutting down executor')
        self._threads.shutdown()
