"""Exceptions raised by the s3tables service."""

from moto.core.exceptions import ServiceException


class S3TablesException(ServiceException):
    pass


class BadRequestException(S3TablesException):
    code = "BadRequestException"


class InvalidContinuationToken(BadRequestException):
    message = "The continuation token is not valid."


class InvalidTableBucketName(BadRequestException):
    message = "The specified bucket name is not valid."


class InvalidTableName(BadRequestException):
    template = "1 validation error detected: Value '%s' at 'name' failed to satisfy constraint: Member must satisfy regular expression pattern: [0-9a-z_]*"

    def __init__(self, name: str) -> None:
        super().__init__(self.template.format(name))


class InvalidNamespaceName(BadRequestException):
    message = "The specified namespace name is not valid."


class InvalidMetadataLocation(BadRequestException):
    message = "The specified metadata location is not valid."


class NothingToRename(BadRequestException):
    message = "Neither a new namespace name nor a new table name is specified."


class NotFoundException(S3TablesException):
    code = "NotFoundException"


class NamespaceDoesNotExist(NotFoundException):
    message = "The specified namespace does not exist."


class DestinationNamespaceDoesNotExist(NotFoundException):
    message = "The specified destination namespace does not exist."


class TableDoesNotExist(NotFoundException):
    message = "The specified table does not exist."


class ConflictException(S3TablesException):
    code = "ConflictException"


class VersionTokenMismatch(ConflictException):
    message = "Provided version token does not match the table version token."


class TableAlreadyExists(ConflictException):
    message = "A table with an identical name already exists in the namespace."
