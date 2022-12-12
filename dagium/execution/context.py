from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from data import OutputDataObject


@dataclass
class Context:
    """
    Context class that is used to store the state of the execution

    :param fexec: FunctionExecutor to use for executing the tasks
    """

    def __init__(self, tasks_ids: list[str]):
        self._output_data: dict[str, OutputDataObject] = dict.fromkeys(tasks_ids, None)
        self._futures: dict[str, Any] = dict.fromkeys(tasks_ids, None)
        self._done = dict.fromkeys(tasks_ids, None)

    @property
    def futures(self) -> dict[str, Any]:
        """ The futures of the tasks """
        return self._futures

    @property
    def output_data(self) -> dict[str, OutputDataObject]:
        """ The output data of the tasks """
        return self._output_data

    @property
    def done(self) -> dict[str, bool]:
        """ The done status of the tasks """
        return self._done
