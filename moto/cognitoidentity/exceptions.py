from moto.core.exceptions import ServiceException


class CognitoIdentityException(ServiceException):
    pass


class ResourceNotFoundError(CognitoIdentityException):
    code = "ResourceNotFoundException"


class InvalidNameException(CognitoIdentityException):
    code = "ValidationException"
    message = "1 validation error detected: Value '{}' at 'identityPoolName' failed to satisfy constraint: Member must satisfy regular expression pattern: [\\w\\s+=,.@-]+"

    def __init__(self, name: str):
        super().__init__(InvalidNameException.message.format(name))
