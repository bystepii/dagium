from __future__ import annotations

import logging
from typing import Dict

from dagium import Future, InputData
from lithops import Storage, LocalhostExecutor

from dagium.dag import DAG, DagExecutor
from dagium.operators import CallAsync
from operators import Map

config = {'lithops': {'backend': 'localhost', 'storage': 'localhost'}}

LOGGER_FORMAT = "%(asctime)s [%(levelname)s] %(filename)s:%(lineno)s -- %(message)s"
logging.basicConfig(format=LOGGER_FORMAT, level=logging.INFO)

logger = logging.getLogger(__name__)


def inc_func(input_data: Dict[str, Future], *args, **kwargs):
    logger.info(f'Executing inc_func with input data: {input_data}')
    s = 0
    for key, value in input_data.items():
        s += value.result()
    return s


def map_func(input_data: Future, *args, **kwargs):
    logger.info(f'Executing map_func with input data: {input_data}')
    return [input_data.result() + 1, input_data.result() + 2]


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    dag = DAG('dag')

    ex = LocalhostExecutor()
    storage = Storage()
    task1 = CallAsync(
        'task1',
        executor=ex,
        func=inc_func,
        input_data=InputData(1)
    )
    task2 = CallAsync(
        'task2',
        executor=ex,
        func=inc_func,
    )
    task3 = CallAsync(
        'task3',
        executor=ex,
        func=inc_func,
    )
    task4 = CallAsync(
        'task4',
        executor=ex,
        func=inc_func,
    )
    task5 = Map(
        'task5',
        executor=ex,
        map_func=map_func,
    )
    task6 = CallAsync(
        'task6',
        executor=ex,
        func=inc_func,
    )

    task1 >> task2 >> [task3, task4] >> task5 >> task6

    dag.add_tasks([task1, task2, task3, task4, task5, task6])
    executor = DagExecutor(dag)
    futures = executor.execute()
    fexec = LocalhostExecutor()
    result = futures['task6'].result()
    print('Tasks completed')
    print(result)
