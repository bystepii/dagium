import logging
from typing import Any

from lithops import FunctionExecutor, Storage

from dagium.dag import DAG
from dagium.data import DataObject, InMemoryDataSource, StorageDataSource
from dagium.execution import DagExecutor
from dagium.operators import CallAsync, Executor

config = {'lithops': {'backend': 'localhost', 'storage': 'localhost'}}

LOGGER_FORMAT = "%(asctime)s [%(levelname)s] %(filename)s:%(lineno)s -- %(message)s"
logging.basicConfig(format=LOGGER_FORMAT, level=logging.INFO)

logger = logging.getLogger(__name__)


class LithopsFunctionExecutor(Executor):
    """
    Executor class that contains the functions that are called by the operators
    """

    def __init__(self, func_exec: FunctionExecutor):
        super().__init__(func_exec)


def my_function(x):
    print(f'Executing my_function with x={x}')
    return x + 1


def another_function(args: dict[str, Any]):
    print(f'Executing another_function with args={args}')
    pass


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    dag = DAG('dag')

    ex = LithopsFunctionExecutor(FunctionExecutor(config=config))
    storage = Storage()
    task1 = CallAsync(
            'task1',
            executor=ex,
            func=my_function,
            input_data=DataObject(InMemoryDataSource(), 1),
            output_data=DataObject(StorageDataSource('tmp/output1.txt', 'my_bucket', storage))
    )
    task2 = CallAsync(
            'task2',
            executor=ex,
            func=my_function,
            output_data=DataObject(InMemoryDataSource())
    )
    task3 = CallAsync(
            'task3',
            executor=ex,
            func=my_function,
            output_data=DataObject(InMemoryDataSource())
    )
    task4 = CallAsync(
            'task4',
            executor=ex,
            func=my_function,
            output_data=DataObject(InMemoryDataSource())
    )
    task5 = CallAsync(
            'task5',
            executor=ex,
            func=another_function,
            output_data=DataObject(InMemoryDataSource())
    )

    task1 >> task2 >> [task3, task4] >> task5

    dag.add_tasks([task1, task2, task3, task4, task5])
    executor = DagExecutor(dag, num_threads=10)
    executor.execute()

    print('Waiting for tasks to complete')
    executor.wait()
    print('Tasks completed')
    print('Shutting down executor')
    executor.shutdown()
