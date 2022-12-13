from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from data import OutputDataObject


@dataclass
class Context:
    """
    Context class that is used to store the output data of the tasks, the futures and the done status.

    :param tasks_ids: List of tasks IDs
    """

    def __init__(self, tasks_ids: list[str]):
        self._output_data: dict[str, OutputDataObject] = dict.fromkeys(tasks_ids, None)
        self._futures: dict[str, Any] = dict.fromkeys(tasks_ids, None)
        self._done = dict.fromkeys(tasks_ids, None)

    @property
    def futures(self) -> dict[str, Any]:
        """Return a dictionary with the task IDs as keys and the futures as values"""
        return self._futures

    @property
    def output_data(self) -> dict[str, OutputDataObject]:
        """Return a dictionary with the task IDs as keys and the output data as values"""
        return self._output_data

    @property
    def done(self) -> dict[str, bool]:
        """Return a dictionary with the task IDs as keys and the done status as values"""
        return self._done
