from moto.core.exceptions import ServiceException


class ManagedBlockchainClientError(ServiceException):
    pass


class BadRequestException(ManagedBlockchainClientError):
    code = "BadRequestException"

    def __init__(self, pretty_called_method: str, operation_error: str):
        super().__init__(
            f"An error occurred (BadRequestException) when calling the {pretty_called_method} operation: {operation_error}",
        )


class InvalidRequestException(ManagedBlockchainClientError):
    code = "InvalidRequestException"

    def __init__(self, pretty_called_method: str, operation_error: str):
        super().__init__(
            f"An error occurred (InvalidRequestException) when calling the {pretty_called_method} operation: {operation_error}",
        )


class ResourceNotFoundException(ManagedBlockchainClientError):
    code = "ResourceNotFoundException"

    def __init__(self, pretty_called_method: str, operation_error: str):
        super().__init__(
            f"An error occurred (ResourceNotFoundException) when calling the {pretty_called_method} operation: {operation_error}",
        )


class ResourceAlreadyExistsException(ManagedBlockchainClientError):
    code = "ResourceAlreadyExistsException"

    def __init__(self, pretty_called_method: str, operation_error: str):
        super().__init__(
            f"An error occurred (ResourceAlreadyExistsException) when calling the {pretty_called_method} operation: {operation_error}",
        )


class ResourceLimitExceededException(ManagedBlockchainClientError):
    code = "ResourceLimitExceededException"

    def __init__(self, pretty_called_method: str, operation_error: str):
        super().__init__(
            f"An error occurred (ResourceLimitExceededException) when calling the {pretty_called_method} operation: {operation_error}",
        )
