"""Exceptions raised by the fis service."""

from moto.core.exceptions import ServiceException


class FISException(ServiceException):
    pass


class ResourceNotFoundException(FISException):
    code = "ResourceNotFoundException"
