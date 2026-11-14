"""Exceptions raised by the bedrockagent service."""

from moto.core.exceptions import ServiceException


class AgentsforBedrockClientError(ServiceException):
    pass


class ResourceNotFoundException(AgentsforBedrockClientError):
    code = "ResourceNotFoundException"


class ConflictException(AgentsforBedrockClientError):
    code = "ConflictException"


class ValidationException(AgentsforBedrockClientError):
    code = "ValidationException"
