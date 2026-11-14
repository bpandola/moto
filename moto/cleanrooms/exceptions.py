"""Exceptions raised by the cleanrooms service."""

from moto.core.exceptions import ServiceException


class CleanRoomsException(ServiceException):
    pass


class ResourceNotFoundException(CleanRoomsException):
    code = "ResourceNotFoundException"

    def __init__(self, resource_type: str, resource_id: str):
        super().__init__(f"Could not find {resource_type} with id {resource_id}")
        self.resourceId = resource_id
        self.resourceType = resource_type.upper().replace(" ", "_")
