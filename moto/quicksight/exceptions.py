"""Exceptions raised by the quicksight service."""

from moto.core.exceptions import ServiceException


class QuickSightException(ServiceException):
    pass


class ResourceNotFoundException(QuickSightException):
    code = "ResourceNotFoundException"


class InvalidParameterValueException(QuickSightException):
    code = "InvalidParameterValueException"


class ValidationException(QuickSightException):
    code = "ValidationException"


class SchemaException(QuickSightException):
    code = "SchemaException"


class ParamValidationError(QuickSightException):
    code = "ParamValidationError"
