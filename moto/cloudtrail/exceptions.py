"""Exceptions raised by the cloudtrail service."""

from moto.core.exceptions import ServiceException


class CloudTrailException(ServiceException):
    pass


class InvalidParameterCombinationException(CloudTrailException):
    code = "InvalidParameterCombinationException"


class S3BucketDoesNotExistException(CloudTrailException):
    code = "S3BucketDoesNotExistException"


class InsufficientSnsTopicPolicyException(CloudTrailException):
    code = "InsufficientSnsTopicPolicyException"


class TrailNotFoundException(CloudTrailException):
    code = "TrailNotFoundException"

    def __init__(self, account_id: str, name: str):
        super().__init__(f"Unknown trail: {name} for the user: {account_id}")


class InvalidTrailNameException(CloudTrailException):
    code = "InvalidTrailNameException"


class TrailNameTooShort(InvalidTrailNameException):
    def __init__(self, actual_length: int):
        super().__init__(
            f"Trail name too short. Minimum allowed length: 3 characters. Specified name length: {actual_length} characters."
        )


class TrailNameTooLong(InvalidTrailNameException):
    def __init__(self, actual_length: int):
        super().__init__(
            f"Trail name too long. Maximum allowed length: 128 characters. Specified name length: {actual_length} characters."
        )


class TrailNameNotStartingCorrectly(InvalidTrailNameException):
    message = "Trail name must starts with a letter or number."


class TrailNameNotEndingCorrectly(InvalidTrailNameException):
    message = "Trail name must ends with a letter or number."


class TrailNameInvalidChars(InvalidTrailNameException):
    message = "Trail name or ARN can only contain uppercase letters, lowercase letters, numbers, periods (.), hyphens (-), and underscores (_)."
