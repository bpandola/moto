from moto.core.exceptions import ServiceException


class AthenaException(ServiceException):
    pass


class AthenaClientError(AthenaException):
    code = "InvalidRequestException"

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.AthenaErrorCode = code


class InvalidArgumentException(AthenaException):
    """The specified input parameter has a value that is not valid."""

    code = "InvalidArgumentException"


class MetadataException(AthenaException):
    code = "MetadataException"


class QueryStillRunning(AthenaException):
    code = "InvalidRequestException"

    def __init__(self, current_status: str | None):
        super().__init__(f"Query has not yet finished. Current state: {current_status}")
