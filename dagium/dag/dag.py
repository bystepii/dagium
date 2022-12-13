from dagium.operators import Operator


class DAG:
    """
    Class to represent a DAG

    :param dag_id: DAG ID
    """

    def __init__(self, dag_id):
        self._dag_id = dag_id
        self._tasks = set()

    @property
    def dag_id(self):
        """Return the DAG ID"""
        return self._dag_id

    @property
    def tasks(self) -> set[Operator]:
        """Return all tasks in the DAG"""
        return self._tasks

    @property
    def root_tasks(self) -> set[Operator]:
        """
        Return all root tasks in the DAG

        A root task is a task that has no parents.
        """
        return {task for task in self.tasks if not task.parents}

    @property
    def leaf_tasks(self) -> set[Operator]:
        """
        Return all leaf tasks in the DAG

        A leaf task is a task that has no children.
        """
        return {task for task in self.tasks if not task.children}

    def add_task(self, task: Operator):
        """
        Add a task to this DAG

        :param task: Task to add
        :raises ValueError: if the task is already in the DAG
        """
        if task.task_id in {t.task_id for t in self.tasks}:
            raise ValueError(f"Task with id {task.task_id} already exists in DAG {self._dag_id}")

        self._tasks.add(task)

    def add_tasks(self, tasks: list[Operator]):
        """
        Add a list of tasks to this DAG

        :param tasks: List of tasks to add
        :raises ValueError: if any of the tasks is already in the DAG
        """
        for task in tasks:
            self.add_task(task)
