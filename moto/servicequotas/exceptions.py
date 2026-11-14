"""Exceptions raised by the servicequotas service."""

from moto.core.exceptions import ServiceException


class ServiceQuotasException(ServiceException):
    pass


class NoSuchResource(ServiceQuotasException):
    code = "NoSuchResourceException"
    message = "This service is not available in the current Region. Choose a different Region or a different service."
