from moto.core.exceptions import ServiceException


class MacieException(ServiceException):
    pass


class ValidationException(MacieException):
    code = "ValidationException"


class AccessDeniedException(MacieException):
    code = "AccessDeniedException"


class ResourceNotFoundException(MacieException):
    code = "ResourceNotFoundException"
