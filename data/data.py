from abc import abstractmethod, ABC
from typing import Any, Sized

from lithops import Storage


class DataSource(ABC):
    """Base class for all data sources. """

    def __init__(self, path: str):
        self._path = path

    @abstractmethod
    def get(self) -> Any:
        """ Get the data of this data source """
        raise NotImplementedError

    @abstractmethod
    def put(self, data: Any):
        """ Put data in this data object """
        raise NotImplementedError

    @property
    def path(self) -> str:
        """ The path of this data source """
        return self._path

    @property
    @abstractmethod
    def metadata(self) -> dict[str, Any]:
        """ The metadata of this data source """
        return {'path': self._path}


class StorageDataSource(DataSource):
    """ Data source for lithops storage """

    def __init__(self, path: str, bucket: str, storage: Storage):
        super().__init__(path)
        self._bucket = bucket
        self._storage = storage

    def get(self) -> Any:
        return self._storage.get_object(self._bucket, self._path)

    def put(self, data: Any):
        self._storage.put_object(self._bucket, self._path, data)

    @property
    def metadata(self) -> dict[str, Any]:
        return super().metadata | \
               {'bucket': self._bucket} | \
               {'storage_metadata': self._storage.head_object(self._bucket, self._path)}


class InMemoryDataSource(DataSource):
    """ Data source for in memory data """

    def __init__(self, data: Any = None):
        super().__init__("in_memory")
        self._data = data

    def get(self) -> Any:
        return self._data

    def put(self, data: Any):
        self._data = data

    @property
    def metadata(self) -> dict[str, Any]:
        return super().metadata | \
               {'size': len(self._data) if isinstance(self._data, Sized) else 1} | \
               {'type': type(self._data)}


class DataObject:
    """Base class for all data objects. """

    def __init__(self, data_source: DataSource, data: Any = None, metadata: dict = None):
        self._data_source = data_source
        self._metadata = metadata or {}
        self._data_source.put(data)

    @property
    def metadata(self) -> dict[str, Any]:
        """ The metadata of this data object """
        return {'data_source': self._data_source.metadata} | self._metadata

    def get(self) -> Any:
        """ Get the data of this data object """
        return self._data_source.get()

    def put(self, data: Any):
        """ Put data in this data object """
        self._data_source.put(data)
