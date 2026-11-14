"""Exceptions raised by the backup service."""

from moto.core.exceptions import ServiceException


class BackupClientError(ServiceException):
    pass


class AlreadyExistsException(BackupClientError):
    code = "AlreadyExistsException"


class ResourceNotFoundException(BackupClientError):
    code = "ResourceNotFoundException"


class InvalidParameterValueException(BackupClientError):
    code = "InvalidParameterValueException"


class InvalidRequestException(BackupClientError):
    code = "InvalidRequestException"
