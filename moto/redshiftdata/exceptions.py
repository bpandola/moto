from moto.core.exceptions import ServiceException


class RedshiftDataAPIServiceException(ServiceException):
    pass


class ResourceNotFoundException(RedshiftDataAPIServiceException):
    code = "ResourceNotFoundException"
    message = "Query does not exist."


class ValidationException(RedshiftDataAPIServiceException):
    code = "ValidationException"
