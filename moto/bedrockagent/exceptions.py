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
    message = (
        "Input validation failed. Check your request parameters and retry the request."
    )

    def __init__(self, msg: str):
        # The detailed message is not returned to the caller
        super().__init__(self.message)
