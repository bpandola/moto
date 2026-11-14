from moto.core.exceptions import ServiceException


class ResourceAccessManagerException(ServiceException):
    pass


class InvalidParameterException(ResourceAccessManagerException):
    code = "InvalidParameterException"


class MalformedArnException(ResourceAccessManagerException):
    code = "MalformedArnException"


class OperationNotPermittedException(ResourceAccessManagerException):
    code = "OperationNotPermittedException"
    message = (
        "Unable to enable sharing with AWS Organizations. "
        "Received AccessDeniedException from AWSOrganizations with the following error message: "
        "You don't have permissions to access this resource."
    )


class UnknownResourceException(ResourceAccessManagerException):
    code = "UnknownResourceException"
