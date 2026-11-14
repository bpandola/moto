"""Exceptions raised by the ElasticSearch service."""

from moto.core.exceptions import ServiceException


class ElasticSearchError(ServiceException):
    pass


class ResourceNotFound(ElasticSearchError):
    code = "ResourceNotFoundException"

    def __init__(self, resource_type: str, resource_name: str):
        super().__init__(f"{resource_type} not found: {resource_name}")


class InvalidDomainName(ElasticSearchError):
    code = "ValidationException"

    def __init__(self, domain_name: str):
        super().__init__(
            f"1 validation error detected: Value '{domain_name}' at 'domainName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-z][a-z0-9\\-]+"
        )


class DomainNotFound(ResourceNotFound):
    def __init__(self, domain_name: str):
        super().__init__("Domain", domain_name)
