from moto.core.exceptions import ServiceException


class DmsClientError(ServiceException):
    pass


class ResourceNotFoundFault(DmsClientError):
    code = "ResourceNotFoundFault"


class InvalidResourceStateFault(DmsClientError):
    code = "InvalidResourceStateFault"


class ResourceAlreadyExistsFault(DmsClientError):
    code = "ResourceAlreadyExistsFault"


class ValidationError(DmsClientError):
    code = "ValidationError"


class InvalidParameterCombinationException(DmsClientError):
    code = "InvalidParameterCombinationException"
