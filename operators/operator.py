from __future__ import annotations

from abc import abstractmethod, ABC
from typing import Any

from data import DataObject


class Executor(ABC):
    """
    Executor class that contains the functions that are called by the operators
    """

    def __init__(self, executor: Any):
        self._executor = executor

    def __getattr__(self, item):
        return getattr(self._executor, item)


class Operator(ABC):
    """
    Abstract base class for operators. An operator is a task that can be executed in a DAG.

    :param task_id: Task ID
    :param metadata: Metadata to pass to the operator
    :param args: Arguments to pass to the operator
    :param kwargs: Keyword arguments to pass to the operator
    """

    def __init__(
            self,
            task_id: str,
            executor: Executor,
            input_data: DataObject = None,
            output_data: DataObject = None,
            metadata: dict[str, Any] = None,
            *args,
            **kwargs
    ):
        self._task_id = task_id
        self._executor = executor
        self._input_data = input_data
        self._output_data = output_data
        self._metadata = metadata or dict()
        self._args = args
        self._kwargs = kwargs
        self._children = set()
        self._parents = set()

    @property
    def task_id(self) -> str:
        """ Task ID """
        return self._task_id

    @property
    def executor(self) -> Executor:
        """ Executor """
        return self._executor

    @property
    def parents(self) -> set[Operator]:
        """ Parents of this operator """
        return self._parents

    @property
    def children(self) -> set[Operator]:
        """ Children of this operator """
        return self._children

    @property
    def input_data(self) -> DataObject:
        """ Input data object """
        return self._input_data

    @property
    def output_data(self) -> DataObject:
        """ Output data object """
        return self._output_data

    def _set_relation(self, operator_or_operators: Operator | list[Operator], upstream: bool = False):
        """ Set relation between this operator and another operator or list of operators. """
        if isinstance(operator_or_operators, Operator):
            operator_or_operators = [operator_or_operators]

        for operator in operator_or_operators:
            if upstream:
                self.parents.add(operator)
                operator.children.add(self)
            else:
                self.children.add(operator)
                operator.parents.add(self)

    def add_parent(self, operator: Operator | list[Operator]):
        """ Add a parent to this operator. """
        self._set_relation(operator, upstream=True)

    def add_child(self, operator: Operator | list[Operator]):
        """ Add a child to this operator. """
        self._set_relation(operator, upstream=False)

    def get_input_output(
            self,
            input_data: DataObject = None,
            output_data: DataObject = None
    ) -> tuple[DataObject, DataObject]:
        """ Get input and output data objects. """
        input_data, output_data = input_data or self._input_data, output_data or self._output_data
        if input_data is None:
            raise ValueError("Input data object is not set.")
        if output_data is None:
            raise ValueError("Output data object is not set.")
        self._input_data, self._output_data = input_data, output_data
        return input_data, output_data

    @abstractmethod
    def __call__(
            self,
            input_data: DataObject = None,
            output_data: DataObject = None,
    ) -> Any:
        """ Execute the operator. """
        pass

    def __lshift__(self, other: Operator | list[Operator]):
        """ Overload the << operator to add a parent to this operator. """
        self.add_parent(other)
        return other

    def __rshift__(self, other: Operator | list[Operator]):
        """ Overload the >> operator to add a child to this operator. """
        self.add_child(other)
        return other

    def __rrshift__(self, other: Operator | list[Operator]):
        """ Overload the >> operator for lists of operators. """
        self.add_parent(other)
        return self

    def __rlshift__(self, other: Operator | list[Operator]):
        """ Overload the << operator for lists of operators. """
        self.add_child(other)
        return self
