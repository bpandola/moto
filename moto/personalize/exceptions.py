"""Exceptions raised by the personalize service."""

from moto.core.exceptions import ServiceException


class PersonalizeException(ServiceException):
    pass


class ResourceNotFoundException(PersonalizeException):
    code = "ResourceNotFoundException"

    def __init__(self, arn: str):
        super().__init__(f"Resource Arn {arn} does not exist.")
