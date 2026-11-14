"""Exceptions raised by the connect service."""

from moto.core.exceptions import ServiceException


class ConnectException(ServiceException):
    pass


class ResourceNotFoundException(ConnectException):
    code = "ResourceNotFoundException"


class InvalidParameterException(ConnectException):
    code = "InvalidParameterException"
