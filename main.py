import logging
import pickle

from lithops import Storage, LocalhostExecutor

from dagium.dag import DAG
from dagium.execution import DagExecutor
from dagium.operators import CallAsync
from data import InputDataObject, OutputDataObject, DataObjectFactory

config = {'lithops': {'backend': 'localhost', 'storage': 'localhost'}}

LOGGER_FORMAT = "%(asctime)s [%(levelname)s] %(filename)s:%(lineno)s -- %(message)s"
logging.basicConfig(format=LOGGER_FORMAT, level=logging.INFO)

logger = logging.getLogger(__name__)


def inc_func(input_data: dict[str, InputDataObject], output_data: OutputDataObject):
    print(f'This function has the following parent tasks: {input_data.keys()}')
    if len(input_data) == 1:
        in_data = input_data.popitem()[1]
        data = in_data.get()
        print(f'Input data is {data}')
        if in_data.metadata.get('type') == int:
            output_data.put(pickle.dumps(data + 1))
        else:
            data = pickle.loads(data)
            output_data.put(pickle.dumps(data + 1))
    else:
        raise ValueError('This function should only have one parent task')


def another_function(input_data: dict[str, InputDataObject], output_data: OutputDataObject):
    print(f'Executing another_function with input data: {input_data}')
    pass


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    dag = DAG('dag')

    ex = LocalhostExecutor()
    storage = Storage()
    task1 = CallAsync(
        'task1',
        executor=ex,
        func=inc_func,
        input_data=DataObjectFactory.create_input_data_object(1),
        output_data=OutputDataObject('my_bucket', 'tmp/output1.txt', storage)
    )
    task2 = CallAsync(
        'task2',
        executor=ex,
        func=inc_func,
        output_data=OutputDataObject('my_bucket', 'tmp/output2.txt', storage)
    )
    task3 = CallAsync(
        'task3',
        executor=ex,
        func=inc_func,
        output_data=OutputDataObject('my_bucket', 'tmp/output3.txt', storage)
    )
    task4 = CallAsync(
        'task4',
        executor=ex,
        func=inc_func,
        output_data=OutputDataObject('my_bucket', 'tmp/output4.txt', storage)
    )
    task5 = CallAsync(
        'task5',
        executor=ex,
        func=another_function,
        output_data=OutputDataObject('my_bucket', 'tmp/output5.txt', storage)
    )

    task1 >> task2 >> [task3, task4] >> task5

    dag.add_tasks([task1, task2, task3, task4, task5])
    executor = DagExecutor(dag, num_threads=10)
    results = executor.execute()
    print('Tasks completed')
    print(results)
