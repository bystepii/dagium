from __future__ import annotations

from abc import ABC
from typing import Any, Sized, BinaryIO, TextIO

from lithops import Storage
from lithops.storage.utils import StorageNoSuchKeyError


class DataObject(ABC):
    """
    Base class for all data objects.

    Represents a data object that can be stored in a storage backend. It has associated with it a path,
    a bucket, a reference to the storage backend and optional metadata.

    :param bucket: Bucket where the data object is stored
    :param path: Path where the data object is stored
    :param storage: Storage backend where the data object is stored
    :param metadata: Metadata associated with the data object
    """

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
        """Return the bucket where the data object is stored."""
        return self._bucket

    @property
    def path(self) -> str:
        """Return the path where the data object is stored."""
        return self._path

    @property
    def storage(self) -> Storage:
        """Return the storage backend where the data object is stored."""
        return self._storage

    @property
    def metadata(self) -> dict[str, Any]:
        """Return the metadata associated with the data object."""
        return self._metadata

    def get(self) -> str | bytes | TextIO | BinaryIO:
        """Get the data object."""
        return self._storage.get_object(self._bucket, self._path)

    def put(self, data: str | bytes | TextIO | BinaryIO):
        """
        Put the data object

        :param data: Data to put
        """
        self._storage.put_object(self._bucket, self._path, data)
        self._metadata['size'] = len(data) if isinstance(data, Sized) else 1
        self._metadata['type'] = type(data)


class InputDataObject(DataObject):
    """
    Data object that represents input data for an operator.

    :param bucket: Bucket where the data object is stored
    :param path: Path where the data object is stored
    :param storage: Storage backend where the data object is stored
    :param metadata: Metadata associated with the data object
    """

    def __init__(self, bucket: str, path: str, storage: Storage = None, metadata: dict[str, Any] = None):
        super().__init__(bucket, path, storage, metadata)

    def put(self, data: Any):
        """
        Not implemented for input data objects.

        :raises NotImplementedError: Always
        """
        raise ValueError("Input data objects are read-only")

    def get(self) -> Any:
        return super().get()


class OutputDataObject(DataObject):
    """
    Data object that represents output data for an operator.

    :param bucket: Bucket where the data object is stored
    :param path: Path where the data object is stored
    :param storage: Storage backend where the data object is stored
    :param metadata: Metadata associated with the data object
    """

    def __init__(self, bucket: str, path: str, storage: Storage, metadata: dict[str, Any] = None):
        super().__init__(bucket, path, storage, metadata)


class InMemoryInputDataObject(InputDataObject):
    """
    In-memory input data object.

    Used to pass relatively small data objects to operators without having to store them in a storage backend.

    :param data: Data to store
    :param metadata: Metadata associated with the data object
    """

    def __init__(self, data: Any, metadata: dict[str, Any] = None):
        super().__init__("in_memory", "in_memory", None, metadata)
        self._data = data
        self._metadata = metadata or dict()
        self._metadata['size'] = len(data) if isinstance(data, Sized) else 1
        self._metadata['type'] = type(data)

    def get(self) -> Any:
        return self._data


class DataObjectFactory:
    """
    Factory class for creating data objects.
    """

    @staticmethod
    def create_input_data_object(
            data: Any = None,
            bucket: str = None,
            path: str = None,
            storage: Storage = None,
            metadata: dict[str, Any] = None,
            data_object: DataObject = None
    ) -> InputDataObject:
        """
        Create an input data object.

        :param data: Data to store. If provided, the data object will be stored in memory
                     and the other parameters will be ignored.
        :param bucket: Bucket where the data object is stored
        :param path: Path where the data object is stored
        :param storage: Storage backend where the data object is stored
        :param metadata: Metadata associated with the data object
        :param data_object: Data object to use. If provided, the other parameters will be ignored.
        :return: Input data object
        :raises ValueError: If no data object is provided and no bucket or path is provided
        """
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
        """
        Create an output data object.

        :param bucket: Bucket where the data object is stored
        :param path: Path where the data object is stored
        :param storage: Storage backend where the data object is stored
        :param metadata: Metadata associated with the data object
        :param data_object: Data object to use. If provided, the other parameters will be ignored.
        :return: Output data object
        :raises ValueError: If the parameters are invalid
        """
        if bucket is not None and path is not None and storage is not None:
            return OutputDataObject(bucket, path, storage, metadata)
        elif data_object is not None:
            return OutputDataObject(data_object.bucket, data_object.path, data_object.storage, data_object.metadata)
        else:
            raise ValueError("Invalid parameters for creating an output data object")
