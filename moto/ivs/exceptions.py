"""Exceptions raised by the ivs service."""

from moto.core.exceptions import ServiceException


class IVSException(ServiceException):
    pass


class ResourceNotFoundException(IVSException):
    code = "ResourceNotFoundException"
