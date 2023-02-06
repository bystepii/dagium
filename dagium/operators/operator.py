from __future__ import annotations

from abc import abstractmethod, ABC
from enum import Enum
from typing import Any, Dict, Set, List, Optional

from dagium import Future, LithopsFuture

from lithops import FunctionExecutor


class TaskState(Enum):
    """
    State of a task
    """
    NONE = 0
    SCHEDULED = 1
    WAITING = 2
    RUNNING = 3
    SUCCESS = 4
    FAILED = 5


class Operator(ABC):
    """
    Abstract base class for operators

    An operator is a task that can be executed in a DAG.

    :param task_id: Task ID
    :param executor: Executor to use
    :param input_data: Input data for the operator
    :param metadata: Metadata to pass to the operator
    :param args: Arguments to pass to the operator
    :param kwargs: Keyword arguments to pass to the operator
    """

    def __init__(
            self,
            task_id: str,
            executor: FunctionExecutor,
            input_data: Optional[Dict[str, Future] | Future] = None,
            metadata: Optional[Dict[str, Any]] = None,
            *args,
            **kwargs
    ):
        self._task_id = task_id
        self._executor = executor
        self._input_data = input_data if isinstance(input_data, dict)\
            else {'root': input_data} if input_data else dict()
        self._metadata = metadata or dict()
        self._args = args
        self._kwargs = kwargs

        self._children: Set[Operator] = set()
        self._parents: Set[Operator] = set()
        self._state = TaskState.NONE

    @property
    def task_id(self) -> str:
        """Return the task ID."""
        return self._task_id

    @property
    def executor(self) -> FunctionExecutor:
        """Return the executor."""
        return self._executor

    @property
    def parents(self) -> Set[Operator]:
        """Return the parents of this operator."""
        return self._parents

    @property
    def children(self) -> Set[Operator]:
        """Return the children of this operator."""
        return self._children

    @property
    def input_data(self) -> Dict[str, Future]:
        """Return the input data."""
        return self._input_data

    @property
    def state(self) -> TaskState:
        """Return the state of the task."""
        return self._state

    @state.setter
    def state(self, value):
        """Set the state of the task."""
        self._state = value

    def _set_relation(self, operator_or_operators: Operator | List[Operator], upstream: bool = False):
        """
        Set relation between this operator and another operator or list of operators

        :param operator_or_operators: Operator or list of operators
        :param upstream: Whether to set the relation as upstream or downstream
        """
        if isinstance(operator_or_operators, Operator):
            operator_or_operators = [operator_or_operators]

        for operator in operator_or_operators:
            if upstream:
                self.parents.add(operator)
                operator.children.add(self)
            else:
                self.children.add(operator)
                operator.parents.add(self)

    def add_parent(self, operator: Operator | List[Operator]):
        """
        Add a parent to this operator.
        :param operator: Operator or list of operators
        """
        self._set_relation(operator, upstream=True)

    def add_child(self, operator: Operator | List[Operator]):
        """
        Add a child to this operator.
        :param operator: Operator or list of operators
        """
        self._set_relation(operator, upstream=False)

    @abstractmethod
    def __call__(
            self,
            input_data: Dict[str, Future] = None,
            *args,
            **kwargs
    ) -> LithopsFuture:
        """
        Execute the operator and return a future object.

        :param input_data: Input data
        :return: the future object
        """
        pass

    def __lshift__(self, other: Operator | List[Operator]) -> Operator | List[Operator]:
        """Overload the << operator to add a parent to this operator."""
        self.add_parent(other)
        return other

    def __rshift__(self, other: Operator | List[Operator]) -> Operator | List[Operator]:
        """Overload the >> operator to add a child to this operator."""
        self.add_child(other)
        return other

    def __rrshift__(self, other: Operator | List[Operator]) -> Operator:
        """Overload the >> operator for lists of operator. """
        self.add_parent(other)
        return self

    def __rlshift__(self, other: Operator | List[Operator]) -> Operator:
        """Overload the << operator for lists of operators."""
        self.add_child(other)
        return self
