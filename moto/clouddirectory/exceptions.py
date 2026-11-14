"""Exceptions raised by the clouddirectory service."""

from moto.core.exceptions import ServiceException


class CloudDirectoryException(ServiceException):
    pass


class ValidationError(CloudDirectoryException):
    code = "ValidationException"


class InvalidArnException(CloudDirectoryException):
    code = "InvalidArnException"
    message = "Invalid Arn"

    def __init__(self, resource_id: str):
        super().__init__(self.message)
        self.ResourceId = resource_id


class ResourceNotFoundException(CloudDirectoryException):
    code = "ResourceNotFoundException"
    message = "Resource not found"

    def __init__(self, resource_id: str):
        super().__init__(self.message)
        self.ResourceId = resource_id


class SchemaAlreadyPublishedException(CloudDirectoryException):
    code = "SchemaAlreadyPublishedException"
    message = "Schema already published"

    def __init__(self, schema_arn: str):
        super().__init__(self.message)
        self.SchemaArn = schema_arn


class SchemaAlreadyExistsException(CloudDirectoryException):
    code = "SchemaAlreadyExistsException"
    message = "Schema already exists"

    def __init__(self, schema_arn: str):
        super().__init__(self.message)
        self.SchemaArn = schema_arn
