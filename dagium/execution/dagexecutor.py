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

    :param num_threads: Number of threads to use for executing the DAG
    """

    def __init__(self, dag: DAG, num_threads: int = 10, processor: Processor = None):
        self._dag = dag
        self._num_threads = num_threads
        self._processor = processor or DefaultProcessor(num_threads)
        self._context = Context([task.task_id for task in dag.tasks])
        self._config = {'lithops': {'backend': 'localhost', 'storage': 'localhost'}}
        self._sem = None
        self._num_final_tasks = None

    def execute(self) -> dict[str, OutputDataObject]:
        """
        Execute the DAG

        """
        logger.info(f'Executing DAG {self._dag.dag_id}')

        self._num_final_tasks = len(self._dag.leaf_tasks)
        logger.info(f'DAG {self._dag.dag_id} has {self._num_final_tasks} final tasks')

        dependence_free_tasks = self._dag.root_tasks
        running_tasks = set()
        finished_tasks = set()

        while dependence_free_tasks:
            input_data = {}
            batch_length = min(len(dependence_free_tasks), max(0, self._num_threads - len(running_tasks)))
            batch = list(dependence_free_tasks)[:batch_length]
            for task in batch:
                task.state = TaskState.SCHEDULED
                if task.parents:
                    input_data[task.task_id] = {
                        parent.task_id: DataObjectFactory.create_input_data_object(
                            data_object=self._context.output_data[parent.task_id]
                        ) for parent in task.parents
                    }
                else:
                    input_data[task.task_id] = task.input_data

            running_tasks |= set(batch)

            self._processor.process(batch, input_data, {
                task: output_data for task, output_data in self._context.output_data.items() if task in batch
            })

            running_tasks -= set(batch)
            dependence_free_tasks -= set(batch)
            finished_tasks |= set(batch)

            for task in batch:
                self._context.output_data[task.task_id] = task.output_data
                for child in task.children:
                    if child.parents.issubset(finished_tasks):
                        dependence_free_tasks.add(child)

        return self._context.output_data
