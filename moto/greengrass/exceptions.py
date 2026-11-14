from moto.core.exceptions import ServiceException


class GreengrassClientError(ServiceException):
    pass


class IdNotFoundException(GreengrassClientError):
    code = "IdNotFoundException"


class InvalidContainerDefinitionException(GreengrassClientError):
    code = "InvalidContainerDefinitionException"


class VersionNotFoundException(GreengrassClientError):
    code = "VersionNotFoundException"


class InvalidInputException(GreengrassClientError):
    code = "InvalidInputException"


class MissingCoreException(GreengrassClientError):
    code = "MissingCoreException"


class ResourceNotFoundException(GreengrassClientError):
    code = "ResourceNotFoundException"
