from typing import Any

from moto.core.exceptions import ServiceException


class ApiGatewayException(ServiceException):
    pass


class BadRequestException(ApiGatewayException):
    code = "BadRequestException"


class NotFoundException(ApiGatewayException):
    code = "NotFoundException"


class AccessDeniedException(ApiGatewayException):
    code = "AccessDeniedException"


class ConflictException(ApiGatewayException):
    code = "ConflictException"


class AwsProxyNotAllowed(BadRequestException):
    message = "Integrations of type 'AWS_PROXY' currently only supports Lambda function and Firehose stream invocations."


class CrossAccountNotAllowed(AccessDeniedException):
    message = "Cross-account pass role is not allowed."


class RoleNotSpecified(BadRequestException):
    message = "Role ARN must be specified for AWS integrations"


class IntegrationMethodNotDefined(BadRequestException):
    message = "Enumeration value for HttpMethod must be non-empty"


class InvalidOpenAPIDocumentException(BadRequestException):
    def __init__(self, cause: Any):
        super().__init__(
            f"Failed to parse the uploaded OpenAPI document due to: {cause.message}"
        )


class InvalidOpenApiDocVersionException(BadRequestException):
    message = "Only OpenAPI 3.x.x are currently supported"


class InvalidOpenApiModeException(BadRequestException):
    message = 'Enumeration value of OpenAPI import mode must be "overwrite" or "merge"'


class InvalidResourcePathException(BadRequestException):
    message = "Resource's path part only allow a-zA-Z0-9._- and curly braces at the beginning and the end and an optional plus sign before the closing brace."


class InvalidHttpEndpoint(BadRequestException):
    message = "Invalid HTTP endpoint specified for URI"


class InvalidArn(BadRequestException):
    message = "Invalid ARN specified in the request"


class InvalidIntegrationArn(BadRequestException):
    message = "AWS ARN for integration must contain path or action"


class InvalidRequestInput(BadRequestException):
    message = "Invalid request input"


class NoIntegrationDefined(NotFoundException):
    message = "No integration defined for method"


class NoIntegrationResponseDefined(NotFoundException):
    message = "Invalid Response status code specified"


class NoMethodDefined(BadRequestException):
    message = "The REST API doesn't contain any methods"


class AuthorizerNotFoundException(NotFoundException):
    message = "Invalid Authorizer identifier specified"


class StageNotFoundException(NotFoundException):
    message = "Invalid stage identifier specified"


class ApiKeyNotFoundException(NotFoundException):
    message = "Invalid API Key identifier specified"


class UsagePlanNotFoundException(NotFoundException):
    message = "Invalid Usage Plan ID specified"


class ApiKeyAlreadyExists(ApiGatewayException):
    code = "ConflictException"
    message = "API Key already exists"


class InvalidDomainName(BadRequestException):
    message = "No Domain Name specified"


class DomainNameNotFound(NotFoundException):
    message = "Invalid domain name identifier specified"


class InvalidRestApiId(BadRequestException):
    message = "No Rest API Id specified"


class InvalidModelName(BadRequestException):
    message = "No Model Name specified"


class RestAPINotFound(NotFoundException):
    message = "Invalid Rest API Id specified"


class RequestValidatorNotFound(BadRequestException):
    message = "Invalid Request Validator Id specified"


class ModelNotFound(NotFoundException):
    message = "Invalid Model Name specified"


class ApiKeyValueMinLength(BadRequestException):
    message = "API Key value should be at least 20 characters"


class MethodNotFoundException(NotFoundException):
    message = "Invalid Method identifier specified"


class InvalidBasePathException(BadRequestException):
    message = (
        "API Gateway V1 doesn't support the slash character (/) in base path mappings. "
        "To create a multi-level base path mapping, use API Gateway V2."
    )


class DeploymentNotFoundException(NotFoundException):
    message = "Invalid Deployment identifier specified"


class InvalidRestApiIdForBasePathMappingException(BadRequestException):
    message = "Invalid REST API identifier specified"


class InvalidStageException(BadRequestException):
    message = "Invalid stage identifier specified"


class BasePathConflictException(ConflictException):
    message = "Base path already exists for this domain name"


class BasePathNotFoundException(NotFoundException):
    message = "Invalid base path mapping identifier specified"


class ResourceIdNotFoundException(NotFoundException):
    message = "Invalid resource identifier specified"


class VpcLinkNotFound(NotFoundException):
    message = "VPCLink not found"


class ValidationException(ApiGatewayException):
    code = "ValidationException"


class StageStillActive(BadRequestException):
    message = "Active stages pointing to this deployment must be moved or deleted"


class GatewayResponseNotFound(NotFoundException):
    message = "GatewayResponse not found"
