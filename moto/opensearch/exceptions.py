"""Exceptions raised by the opensearch service."""

from moto.core.exceptions import ServiceException


class OpenSearchServiceException(ServiceException):
    pass


class ResourceNotFoundException(OpenSearchServiceException):
    code = "ResourceNotFoundException"

    def __init__(self, name: str):
        super().__init__(f"Domain not found: {name}")


class EngineTypeNotFoundException(OpenSearchServiceException):
    code = "EngineTypeNotFoundException"

    def __init__(self, domain_name: str):
        super().__init__(f"Engine Type not found: {domain_name}")
