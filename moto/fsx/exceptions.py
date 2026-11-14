"""Exceptions raised by the fsx service."""

from moto.core.exceptions import ServiceException


class FSxException(ServiceException):
    pass


class ResourceNotFoundException(FSxException):
    code = "ResourceNotFoundException"


class FileSystemNotFound(FSxException):
    code = "FileSystemNotFound"

    def __init__(self, file_system_id: str):
        super().__init__(f"File system '{file_system_id}' does not exist.")
