import pickle
from abc import abstractmethod, ABC
from typing import Any, Sized

from lithops import Storage


class DataSource(ABC):
    """Base class for all data sources. """

    def __init__(self, path: str, metadata: dict[str, Any] = None):
        self._path = path
        self._metadata = metadata or dict()
        self._metadata['path'] = path

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
    def metadata(self) -> dict[str, Any]:
        """ Metadata of this data source """
        return self._metadata


class StorageDataSource(DataSource):
    """ Data source for lithops storage """

    def __init__(self, path: str, bucket: str, storage: Storage, metadata: dict[str, Any] = None):
        super().__init__(path, metadata)
        self._bucket = bucket
        self._storage = storage
        super().metadata['bucket'] = bucket
        super().metadata['storage_metadata'] = self._storage.head_object(self._bucket, self._path)

    def get(self) -> Any:
        return pickle.loads(self._storage.get_object(self._bucket, self._path))

    def put(self, data: Any):
        self._storage.put_object(self._bucket, self._path, pickle.dumps(data))


class InMemoryDataSource(DataSource):
    """ Data source for in memory data """

    def __init__(self, data: Any = None, metadata: dict[str, Any] = None):
        super().__init__("in_memory", metadata)
        self._data = data

    def get(self) -> Any:
        return self._data

    def put(self, data: Any):
        self._data = data


class DataObject:
    """Base class for all data objects. """

    def __init__(self, data_source: DataSource, data: Any = None, metadata: dict[str, Any] = None):
        self._data_source = data_source
        self._metadata = metadata or {}
        if data is not None:
            self.put(data)

    @property
    def metadata(self) -> dict[str, Any]:
        """ The metadata of this data object """
        return self._metadata | self._data_source.metadata

    def get(self) -> Any:
        """ Get the data of this data object """
        return self._data_source.get()

    def put(self, data: Any):
        """ Put data in this data object """
        self._data_source.put(data)
        self._metadata['size'] = len(data) if isinstance(data, Sized) else 1
        self._metadata['type'] = type(data)
