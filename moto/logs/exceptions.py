from typing import Any

from moto.core.exceptions import ServiceException


class LogsClientError(ServiceException):
    pass


class ResourceNotFoundException(LogsClientError):
    code = "ResourceNotFoundException"

    def __init__(self, msg: str | None = None):
        super().__init__(msg or "The specified log group does not exist.")


class InvalidParameterException(LogsClientError):
    code = "InvalidParameterException"

    def __init__(
        self,
        msg: str | None = None,
        constraint: str | None = None,
        parameter: str | None = None,
        value: Any = None,
    ):
        if constraint:
            msg = f"1 validation error detected: Value '{value}' at '{parameter}' failed to satisfy constraint: {constraint}"
        super().__init__(msg or "A parameter is specified incorrectly.")


class ResourceAlreadyExistsException(LogsClientError):
    code = "ResourceAlreadyExistsException"
    message = "The specified log group already exists"


class LimitExceededException(LogsClientError):
    code = "LimitExceededException"
    message = "Resource limit exceeded."


class ValidationException(LogsClientError):
    code = "ValidationException"


class ConflictException(LogsClientError):
    code = "ConflictException"
