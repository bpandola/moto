"""Exceptions raised by the timestreamwrite service."""

from moto.core.exceptions import ServiceException


class TimestreamWriteException(ServiceException):
    pass


class ResourceNotFound(TimestreamWriteException):
    code = "ResourceNotFoundException"
