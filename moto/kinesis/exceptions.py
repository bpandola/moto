from moto.core.exceptions import ServiceException


class KinesisException(ServiceException):
    pass


class ResourceNotFoundError(KinesisException):
    code = "ResourceNotFoundException"


class ResourceInUseError(KinesisException):
    code = "ResourceInUseException"


class StreamNotFoundError(ResourceNotFoundError):
    def __init__(self, stream_name: str, account_id: str):
        super().__init__(f"Stream {stream_name} under account {account_id} not found.")


class StreamCannotBeUpdatedError(KinesisException):
    code = "ValidationException"

    def __init__(self, stream_name: str, account_id: str):
        super().__init__(
            f"Request is invalid. Stream {stream_name} under account {account_id} is in On-Demand mode."
        )


class ShardNotFoundError(ResourceNotFoundError):
    def __init__(self, shard_id: str, stream: str, account_id: str):
        super().__init__(
            f"Could not find shard {shard_id} in stream {stream} under account {account_id}."
        )


class ConsumerNotFound(ResourceNotFoundError):
    def __init__(self, consumer: str, account_id: str):
        super().__init__(f"Consumer {consumer}, account {account_id} not found.")


class InvalidArgumentError(KinesisException):
    code = "InvalidArgumentException"


class InvalidRetentionPeriod(InvalidArgumentError):
    def __init__(self, hours: int, too_short: bool):
        if too_short:
            msg = f"Minimum allowed retention period is 24 hours. Requested retention period ({hours} hours) is too short."
        else:
            msg = f"Maximum allowed retention period is 8760 hours. Requested retention period ({hours} hours) is too long."
        super().__init__(msg)


class InvalidDecreaseRetention(InvalidArgumentError):
    def __init__(self, name: str | None, requested: int, existing: int):
        msg = f"Requested retention period ({requested} hours) for stream {name} can not be longer than existing retention period ({existing} hours). Use IncreaseRetentionPeriod API."
        super().__init__(msg)


class InvalidIncreaseRetention(InvalidArgumentError):
    def __init__(self, name: str | None, requested: int, existing: int):
        msg = f"Requested retention period ({requested} hours) for stream {name} can not be shorter than existing retention period ({existing} hours). Use DecreaseRetentionPeriod API."
        super().__init__(msg)


class ValidationException(KinesisException):
    code = "ValidationException"

    def __init__(self, value: str, position: str, regex_to_match: str):
        super().__init__(
            f"1 validation error detected: Value '{value}' at '{position}' failed to satisfy constraint: Member must satisfy regular expression pattern: {regex_to_match}"
        )


class RecordSizeExceedsLimit(KinesisException):
    code = "ValidationException"

    def __init__(self, position: int):
        super().__init__(
            f"1 validation error detected: Value at 'records.{position}.member.data' failed to satisfy constraint: Member must have length less than or equal to 1048576"
        )


class TotalRecordsSizeExceedsLimit(KinesisException):
    code = "InvalidArgumentException"
    message = "Records size exceeds 5 MB limit"


class TooManyRecords(KinesisException):
    code = "ValidationException"
    message = "1 validation error detected: Value at 'records' failed to satisfy constraint: Member must have length less than or equal to 500"
