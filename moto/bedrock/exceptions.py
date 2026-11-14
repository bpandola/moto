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


class TooManyTagsException(BedrockClientError):
    code = "TooManyTagsException"
