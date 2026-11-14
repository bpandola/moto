"""Exceptions raised by the bedrock service."""

from moto.core.exceptions import ServiceException


class BedrockClientError(ServiceException):
    pass


class ResourceNotFoundException(BedrockClientError):
    code = "ResourceNotFoundException"


class ResourceInUseException(BedrockClientError):
    code = "ResourceInUseException"


class ValidationException(BedrockClientError):
    code = "ValidationException"

    def __init__(self, msg: str):
        # The detailed msg is not exposed to the client.
        super().__init__(
            "Input validation failed. Check your request parameters and retry the request."
        )


class TooManyTagsException(BedrockClientError):
    code = "TooManyTagsException"
