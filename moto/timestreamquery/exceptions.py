"""Exceptions raised by the timestreamquery service."""

from moto.core.exceptions import ServiceException


class TimestreamQueryException(ServiceException):
    pass


class ResourceNotFound(TimestreamQueryException):
    code = "ResourceNotFoundException"

    def __init__(self, arn: str):
        super().__init__(f"The resource with arn {arn} does not exist.")
