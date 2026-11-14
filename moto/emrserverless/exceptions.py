"""Exceptions raised by the emrserverless service."""

from moto.core.exceptions import ServiceException


class EMRServerlessException(ServiceException):
    pass


class ResourceNotFoundException(EMRServerlessException):
    code = "ResourceNotFoundException"

    def __init__(self, resource: str, resource_type: str = "Application"):
        super().__init__(f"{resource_type} {resource} does not exist")


class ValidationException(EMRServerlessException):
    code = "ValidationException"


class AccessDeniedException(EMRServerlessException):
    code = "AccessDeniedException"
