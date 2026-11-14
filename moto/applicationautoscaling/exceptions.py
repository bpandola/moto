from moto.core.exceptions import ServiceException


class ApplicationAutoScalingException(ServiceException):
    pass


class AWSValidationException(ApplicationAutoScalingException):
    code = "ValidationException"
