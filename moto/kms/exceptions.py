from moto.core.exceptions import ServiceException


class KMSException(ServiceException):
    pass


class NotFoundException(KMSException):
    code = "NotFoundException"


class ValidationException(KMSException):
    code = "ValidationException"


class AlreadyExistsException(KMSException):
    code = "AlreadyExistsException"


class NotAuthorizedException(KMSException):
    code = "NotAuthorizedException"
    message = ""


class AccessDeniedException(KMSException):
    code = "AccessDeniedException"


class InvalidCiphertextException(KMSException):
    code = "InvalidCiphertextException"
    message = ""


class InvalidKeyUsageException(KMSException):
    code = "InvalidKeyUsageException"
    message = ""


class KMSInvalidMacException(KMSException):
    code = "KMSInvalidMacException"
    message = ""
