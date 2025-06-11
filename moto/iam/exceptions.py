from typing import Any

from moto.core.exceptions import ServiceException


class IAMNotFoundException(ServiceException):
    def __init__(self, message: str):
        super().__init__("NoSuchEntity", message)


class IAMConflictException(ServiceException):
    def __init__(self, code: str = "Conflict", message: str = ""):
        super().__init__(code, message)


class IAMReportNotPresentException(ServiceException):
    def __init__(self, message: str):
        super().__init__("CredentialReportNotPresentException", message)


class IAMLimitExceededException(ServiceException):
    def __init__(self, message: str):
        super().__init__("LimitExceeded", message)


class MalformedCertificate(ServiceException):
    def __init__(self, cert: str):
        super().__init__("MalformedCertificate", f"Certificate {cert} is malformed")


class MalformedPolicyDocument(ServiceException):
    def __init__(self, message: str = ""):
        super().__init__("MalformedPolicyDocument", message)


class DuplicateTags(ServiceException):
    def __init__(self) -> None:
        super().__init__(
            "InvalidInput",
            "Duplicate tag keys found. Please note that Tag keys are case insensitive.",
        )


class TagKeyTooBig(ServiceException):
    def __init__(self, tag: str, param: str = "tags.X.member.key"):
        super().__init__(
            "ValidationError",
            f"1 validation error detected: Value '{tag}' at '{param}' failed to satisfy "
            "constraint: Member must have length less than or equal to 128.",
        )


class TagValueTooBig(ServiceException):
    def __init__(self, tag: str):
        super().__init__(
            "ValidationError",
            f"1 validation error detected: Value '{tag}' at 'tags.X.member.value' failed to satisfy "
            "constraint: Member must have length less than or equal to 256.",
        )


class InvalidTagCharacters(ServiceException):
    def __init__(self, tag: str, param: str = "tags.X.member.key"):
        message = f"1 validation error detected: Value '{tag}' at '{param}' failed to satisfy constraint: Member must satisfy regular expression pattern: [\\p{{L}}\\p{{Z}}\\p{{N}}_.:/=+\\-@]+"

        super().__init__("ValidationError", message)


class TooManyTags(ServiceException):
    def __init__(self, tags: Any, param: str = "tags"):
        super().__init__(
            "ValidationError",
            f"1 validation error detected: Value '{tags}' at '{param}' failed to satisfy "
            "constraint: Member must have length less than or equal to 50.",
        )


class EntityAlreadyExists(ServiceException):
    def __init__(self, message: str):
        super().__init__("EntityAlreadyExistsException", message)


class ValidationError(ServiceException):
    def __init__(self, message: str):
        super().__init__("ValidationError", message)


class InvalidInput(ServiceException):
    def __init__(self, message: str):
        super().__init__("InvalidInput", message)


class NoSuchEntity(ServiceException):
    def __init__(self, message: str):
        super().__init__("NoSuchEntityException", message)
