"""Exceptions raised by the ssoadmin service."""

from moto.core.exceptions import ServiceException


class SSOAdminException(ServiceException):
    pass


class ResourceNotFoundException(SSOAdminException):
    code = "ResourceNotFoundException"
    message = ""


class ConflictException(SSOAdminException):
    code = "ConflictException"
    message = ""


class ServiceQuotaExceededException(SSOAdminException):
    code = "ServiceQuotaExceededException"
    message = ""
