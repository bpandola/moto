"""Exceptions raised by the timestreaminfluxdb service."""

from moto.core.exceptions import ServiceException


class TimestreamInfluxDBException(ServiceException):
    pass


class ValidationException(TimestreamInfluxDBException):
    code = "ValidationException"


class ConflictException(TimestreamInfluxDBException):
    code = "ConflictException"


class ResourceNotFoundException(TimestreamInfluxDBException):
    code = "ResourceNotFoundException"
