"""Exceptions raised by the servicecatalogappregistry service."""

from moto.core.exceptions import ServiceException


class AppRegistryException(ServiceException):
    pass


class ResourceNotFoundException(AppRegistryException):
    code = "ResourceNotFoundException"
    message = "The specified resource does not exist."


class ValidationException(AppRegistryException):
    code = "ValidationException"
    message = "The request has invalid or missing parameters."
