"""Exceptions raised by the resiliencehub service."""

from moto.core.exceptions import ServiceException


class ResilienceHubException(ServiceException):
    pass


class ResourceNotFound(ResilienceHubException):
    code = "ResourceNotFoundException"


class AppNotFound(ResourceNotFound):
    def __init__(self, arn: str):
        super().__init__(f"App not found for appArn {arn}")


class AppVersionNotFound(ResourceNotFound):
    message = "App Version not found"


class ResiliencyPolicyNotFound(ResourceNotFound):
    def __init__(self, arn: str):
        super().__init__(f"ResiliencyPolicy {arn} not found")


class ValidationException(ResilienceHubException):
    code = "ValidationException"
