from abc import abstractmethod, ABC

from lithops import ServerlessExecutor, StandaloneExecutor, LocalhostExecutor, FunctionExecutor


class Executor(ABC):
    """
    Executor class that contains the functions that are called by the operators
    """

    def __init__(self):
        pass

    @abstractmethod
    def wait(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def get_result(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def call_async(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def map(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def map_reduce(self, *args, **kwargs):
        raise NotImplementedError


class LithopsFunctionExecutor(Executor):
    """
    Executor class that contains the functions that are called by the operators
    """

    def __init__(self, executor: FunctionExecutor):
        super().__init__()
        self._executor = executor

    def wait(self, *args, **kwargs):
        return self._executor.wait(*args, **kwargs)

    def get_result(self, *args, **kwargs):
        return self._executor.get_result(*args, **kwargs)

    def call_async(self, *args, **kwargs):
        return self._executor.call_async(*args, **kwargs)

    def map(self, *args, **kwargs):
        return self._executor.map(*args, **kwargs)

    def map_reduce(self, *args, **kwargs):
        return self._executor.map_reduce(*args, **kwargs)


class LithopsLocalhostExecutor(LithopsFunctionExecutor):
    """
    Executor class that contains the functions that are called by the operators
    """

    def __init__(self, *args, **kwargs):
        super().__init__(LocalhostExecutor(*args, **kwargs))


class LithopsStandaloneExecutor(LithopsFunctionExecutor):
    """
    Executor class that contains the functions that are called by the operators
    """

    def __init__(self, *args, **kwargs):
        super().__init__(StandaloneExecutor(*args, **kwargs))


class LithopsServerlessExecutor(LithopsFunctionExecutor):
    """
    Executor class that contains the functions that are called by the operators
    """

    def __init__(self, *args, **kwargs):
        super().__init__(ServerlessExecutor(*args, **kwargs))
