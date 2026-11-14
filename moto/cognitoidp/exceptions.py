from moto.core.exceptions import ServiceException


class CognitoIDPException(ServiceException):
    pass


class AliasExistsException(CognitoIDPException):
    code = "AliasExistsException"
    message = "An account with the given email already exists."


class ResourceNotFoundError(CognitoIDPException):
    code = "ResourceNotFoundException"

    def __init__(self, message: str | None):
        super().__init__(message or "")


class UserNotFoundError(CognitoIDPException):
    code = "UserNotFoundException"


class UsernameExistsException(CognitoIDPException):
    code = "UsernameExistsException"


class GroupExistsException(CognitoIDPException):
    code = "GroupExistsException"


class NotAuthorizedError(CognitoIDPException):
    code = "NotAuthorizedException"

    def __init__(self, message: str | None):
        super().__init__(message or "")


class UserNotConfirmedException(CognitoIDPException):
    code = "UserNotConfirmedException"


class ExpiredCodeException(CognitoIDPException):
    code = "ExpiredCodeException"


class InvalidParameterException(CognitoIDPException):
    code = "InvalidParameterException"

    def __init__(self, msg: str | None = None):
        super().__init__(msg or "A parameter is specified incorrectly.")


class InvalidPasswordException(CognitoIDPException):
    code = "InvalidPasswordException"
    message = "The provided password does not confirm to the configured password policy"


class CodeMismatchException(CognitoIDPException):
    code = "CodeMismatchException"
