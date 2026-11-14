"""Exceptions raised by the pipes service."""

from moto.core.exceptions import ServiceException


class PipesException(ServiceException):
    pass


class ValidationException(PipesException):
    code = "ValidationException"

    # TODO: Add field_list parameter in future iterations as it is not being used yet in current codebase


class ConflictException(PipesException):
    code = "ConflictException"

    def __init__(
        self,
        message: str,
        resource_id: str | None = None,
        resource_type: str | None = None,
    ):
        super().__init__(message)
        self.resourceId = resource_id
        self.resourceType = resource_type


class NotFoundException(PipesException):
    code = "NotFoundException"


class InternalException(PipesException):
    code = "InternalException"

    def __init__(self, message: str, retry_after_seconds: int | None = None):
        super().__init__(message)
        self.retryAfterSeconds = retry_after_seconds


class ServiceQuotaExceededException(PipesException):
    code = "ServiceQuotaExceededException"

    def __init__(
        self,
        message: str,
        quota_code: str | None = None,
        resource_id: str | None = None,
        resource_type: str | None = None,
        service_code: str | None = None,
    ):
        super().__init__(message)
        self.quotaCode = quota_code
        self.resourceId = resource_id
        self.resourceType = resource_type
        self.serviceCode = service_code


class ThrottlingException(PipesException):
    code = "ThrottlingException"

    def __init__(
        self,
        message: str,
        quota_code: str | None = None,
        retry_after_seconds: int | None = None,
        service_code: str | None = None,
    ):
        super().__init__(message)
        self.quotaCode = quota_code
        self.retryAfterSeconds = retry_after_seconds
        self.serviceCode = service_code


ResourceNotFoundException = NotFoundException
