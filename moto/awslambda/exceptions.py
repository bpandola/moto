from typing import Any

from moto.core.exceptions import ServiceException


class LambdaClientError(ServiceException):
    pass


class CrossAccountNotAllowed(LambdaClientError):
    code = "AccessDeniedException"
    message = "Cross-account pass role is not allowed."


class FunctionAlreadyExists(LambdaClientError):
    code = "ResourceConflictException"

    def __init__(self, function_name: str) -> None:
        super().__init__(f"Function already exist: {function_name}")


class InvalidParameterValueException(LambdaClientError):
    code = "InvalidParameterValueException"


class InvalidRoleFormat(LambdaClientError):
    code = "ValidationException"
    pattern = r"arn:(aws[a-zA-Z-]*)?:iam::(\d{12}):role/?[a-zA-Z_0-9+=,.@\-_/]+"

    def __init__(self, role: str):
        super().__init__(
            f"1 validation error detected: Value '{role}' at 'role' failed to satisfy constraint: Member must satisfy regular expression pattern: {InvalidRoleFormat.pattern}"
        )


class PreconditionFailedException(LambdaClientError):
    code = "PreconditionFailedException"


class ConflictException(LambdaClientError):
    code = "ConflictException"


class ResourceConflictException(LambdaClientError):
    code = "ResourceConflictException"


class ResourceNotFoundException(LambdaClientError):
    code = "ResourceNotFoundException"


class UnknownAliasException(ResourceNotFoundException):
    def __init__(self, arn: str):
        super().__init__(f"Cannot find alias arn: {arn}")


class UnknownFunctionException(ResourceNotFoundException):
    def __init__(self, arn: str):
        super().__init__(f"Function not found: {arn}")


class GenericResourcNotFound(ResourceNotFoundException):
    message = "The resource you requested does not exist."


class UnknownLayerException(ResourceNotFoundException):
    message = "Cannot find layer"


class UnknownLayerVersionException(ResourceNotFoundException):
    def __init__(self, arns: Any) -> None:
        super().__init__(f"One or more LayerVersion does not exist {arns}")


class UnknownPolicyException(ResourceNotFoundException):
    message = "No policy is associated with the given resource."


class UnknownEventConfig(ResourceNotFoundException):
    def __init__(self, arn: str) -> None:
        super().__init__(f"The function {arn} doesn't have an EventInvokeConfig")


class ValidationException(LambdaClientError):
    code = "ValidationException"

    def __init__(self, value: str, property_name: str, specific_message: str):
        super().__init__(
            f"1 validation error detected: Value '{value}' at '{property_name}' failed to satisfy constraint: {specific_message}"
        )
