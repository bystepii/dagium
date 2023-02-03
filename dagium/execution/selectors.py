from abc import ABC, abstractmethod
from typing import List, Iterable, Sized, Sequence, Collection

from dagium.operators import Operator


class Selector(ABC):
    """
    Abstract base class for selectors
    """

    def __init__(self):
        pass

    @abstractmethod
    def select(self, running_tasks: Sequence[Operator], waiting_tasks: Sequence[Operator]) -> Sequence[Operator]:
        """
        Select a task from a list of tasks

        :param running_tasks:
        :param waiting_tasks:
        :return: Selected tasks
        """
        pass


class AllSelector(Selector):
    """
    Selects all tasks
    """

    def __init__(self):
        super().__init__()

    def select(self, running_tasks: Sequence[Operator], waiting_tasks: Sequence[Operator]) -> Sequence[Operator]:
        """
        Select a task from a list of tasks

        :param running_tasks:
        :param waiting_tasks:
        :return: Selected tasks
        """
        return waiting_tasks


class MaxConcurrencySelector(Selector):
    """
    Selects tasks up to a maximum concurrency
    """

    def __init__(self, max_concurrency: int):
        super().__init__()
        self._max_concurrency = max_concurrency

    def select(self, running_tasks: Sequence[Operator], waiting_tasks: Sequence[Operator]) -> Sequence[Operator]:
        """
        Select a task from a list of tasks

        :param running_tasks:
        :param waiting_tasks:
        :return: Selected tasks
        """
        return waiting_tasks[:min(len(waiting_tasks), max(0, self._max_concurrency - len(running_tasks)))]
