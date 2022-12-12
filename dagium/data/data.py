from __future__ import annotations

from abc import ABC
from typing import Any, Sized, BinaryIO, TextIO

from lithops import Storage
from lithops.storage.utils import StorageNoSuchKeyError


class DataObject(ABC):
    """Base class for all data objects. """

    def __init__(self, bucket: str, path: str, storage: Storage = None, metadata: dict[str, Any] = None):
        self._bucket = bucket
        self._path = path
        self._storage = storage
        self._metadata = metadata or dict()
        self._metadata['path'] = path
        self._metadata['bucket'] = bucket
        if storage:
            try:
                self._metadata['storage_metadata'] = self._storage.head_object(self._bucket, self._path) or dict()
            except StorageNoSuchKeyError:
                self._metadata['storage_metadata'] = dict()

    @property
    def bucket(self) -> str:
        """ The bucket of this data object """
        return self._bucket

    @property
    def path(self) -> str:
        """ The path of this data object """
        return self._path

    @property
    def storage(self) -> Storage:
        """ The storage of this data object """
        return self._storage

    @property
    def metadata(self) -> dict[str, Any]:
        """ The metadata of this data object """
        return self._metadata

    def get(self) -> str | bytes | TextIO | BinaryIO:
        """ Get the data of this data object """
        return self._storage.get_object(self._bucket, self._path)

    def put(self, data: str | bytes | TextIO | BinaryIO):
        """ Put data in this data object """
        self._storage.put_object(self._bucket, self._path, data)
        self._metadata['size'] = len(data) if isinstance(data, Sized) else 1
        self._metadata['type'] = type(data)


class InputDataObject(DataObject):
    """ Data object for input data """

    def __init__(self, bucket: str, path: str, storage: Storage = None, metadata: dict[str, Any] = None):
        super().__init__(bucket, path, storage, metadata)

    def put(self, data: Any):
        """ Put data in this data object """
        raise ValueError("Input data objects are read-only")

    def get(self) -> Any:
        """ Get the data of this data object """
        return super().get()


class OutputDataObject(DataObject):
    """ Data object for output data """

    def __init__(self, bucket: str, path: str, storage: Storage, metadata: dict[str, Any] = None):
        super().__init__(bucket, path, storage, metadata)


class InMemoryInputDataObject(InputDataObject):
    """ Data object for in memory data """

    def __init__(self, data: Any = None, metadata: dict[str, Any] = None):
        super().__init__("in_memory", "in_memory", None, metadata)
        self._data = data
        self._metadata = metadata or dict()
        self._metadata['size'] = len(data) if isinstance(data, Sized) else 1
        self._metadata['type'] = type(data)

    def get(self) -> Any:
        return self._data


class DataObjectFactory:
    """ Factory for data objects """

    @staticmethod
    def create_input_data_object(
            data: Any = None,
            bucket: str = None,
            path: str = None,
            storage: Storage = None,
            metadata: dict[str, Any] = None,
            data_object: DataObject = None
    ) -> InputDataObject:
        """ Create an input data object """
        if data is not None:
            return InMemoryInputDataObject(data, metadata)
        elif bucket is not None and path is not None and storage is not None:
            return InputDataObject(bucket, path, storage, metadata)
        elif data_object is not None:
            return InputDataObject(data_object.bucket, data_object.path, data_object.storage, data_object.metadata)
        else:
            raise ValueError("Invalid parameters for creating an input data object")

    @staticmethod
    def create_output_data_object(
            bucket: str,
            path: str,
            storage: Storage,
            metadata: dict[str, Any] = None,
            data_object: DataObject = None
    ) -> OutputDataObject:
        if bucket is not None and path is not None and storage is not None:
            return OutputDataObject(bucket, path, storage, metadata)
        elif data_object is not None:
            return OutputDataObject(data_object.bucket, data_object.path, data_object.storage, data_object.metadata)
        else:
            raise ValueError("Invalid parameters for creating an output data object")
