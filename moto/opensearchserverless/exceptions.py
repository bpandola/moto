"""Exceptions raised by the opensearchserverless service."""

from moto.core.exceptions import ServiceException


class BackupClientError(ServiceException):
    pass


class ConflictException(BackupClientError):
    code = "ConflictException"


class ValidationException(BackupClientError):
    code = "ValidationException"


class ResourceNotFoundException(BackupClientError):
    code = "ResourceNotFoundException"
