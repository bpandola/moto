from moto.core.exceptions import ServiceException


class ResourceGroupsException(ServiceException):
    pass


class BadRequestException(ResourceGroupsException):
    code = "BadRequestException"


class NotFoundException(ResourceGroupsException):
    code = "NotFoundException"
    message = "One or more of the specified resources don't exist."
