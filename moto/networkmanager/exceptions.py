"""Exceptions raised by the networkmanager service."""

from moto.core.exceptions import ServiceException


class NetworkManagerException(ServiceException):
    pass


class ValidationError(NetworkManagerException):
    code = "ValidationException"


class ResourceNotFound(NetworkManagerException):
    code = "ResourceNotFoundException"
    message = "Resource not found."

    def __init__(self, resource_id: str):
        super().__init__()
        self.ResourceId = resource_id
