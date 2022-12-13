import logging

from dagium.dag import DAG
from dagium.execution.context import Context
from dagium.execution.processors import Processor, DefaultProcessor
from dagium.operators import TaskState
from data import OutputDataObject, DataObjectFactory

logger = logging.getLogger(__name__)


class DagExecutor:
    """
    Executor class that is responsible for executing the DAG

    :param dag: DAG to execute
    :param max_parallelism: Maximum number of tasks to execute in parallel, defaults to 10
    :param processor: Processor to use for executing tasks, defaults to DefaultProcessor
    """

    def __init__(self, dag: DAG, max_parallelism: int = 10, processor: Processor = DefaultProcessor()):
        self._dag = dag
        self._max_parallelism = max_parallelism
        self._processor = processor
        self._context = Context([task.task_id for task in dag.tasks])
        self._config = {'lithops': {'backend': 'localhost', 'storage': 'localhost'}}
        self._sem = None
        self._num_final_tasks = None

    def execute(self) -> dict[str, OutputDataObject]:
        """
        Execute the DAG

        :return: A dictionary with the output data of the DAG tasks with the task ID as key
        """
        logger.info(f'Executing DAG {self._dag.dag_id}')

        self._num_final_tasks = len(self._dag.leaf_tasks)
        logger.info(f'DAG {self._dag.dag_id} has {self._num_final_tasks} final tasks')

        # Start by executing the root tasks
        dependence_free_tasks = self._dag.root_tasks
        running_tasks = set()
        finished_tasks = set()

        # Execute tasks until all tasks have been executed
        while dependence_free_tasks:
            # Construct the batch of tasks to execute
            # The batch size is the minimum of the number of dependence free tasks and the maximum parallelism
            batch_length = min(len(dependence_free_tasks), max(0, self._max_parallelism - len(running_tasks)))
            batch = list(dependence_free_tasks)[:batch_length]

            # Construct the input data for the batch
            input_data = {}
            for task in batch:
                task.state = TaskState.SCHEDULED
                # If the task has parents, then the input data is the output data of the parent tasks
                # passed as a dictionary with the parent task ID as the key and the output data as the value
                if task.parents:
                    input_data[task.task_id] = {
                        parent.task_id: DataObjectFactory.create_input_data_object(
                            data_object=self._context.output_data[parent.task_id]
                        ) for parent in task.parents
                    }
                else:
                    input_data[task.task_id] = task.input_data

            # Add the batch to the running tasks
            running_tasks |= set(batch)

            # Call the processor to execute the batch
            self._processor.process(batch, input_data, {
                task: output_data for task, output_data in self._context.output_data.items() if task in batch
            })

            # Remove the batch from the running tasks and add it to the finished tasks
            running_tasks -= set(batch)
            dependence_free_tasks -= set(batch)
            finished_tasks |= set(batch)

            # Check if any of the finished tasks have children that are now dependence free
            # If so, add them to the dependence free tasks
            for task in batch:
                self._context.output_data[task.task_id] = task.output_data
                for child in task.children:
                    if child.parents.issubset(finished_tasks):
                        dependence_free_tasks.add(child)

        return self._context.output_data
