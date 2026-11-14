from moto.core.exceptions import ServiceException


class SecretsManagerClientError(ServiceException):
    pass


class ResourceNotFoundException(SecretsManagerClientError):
    code = "ResourceNotFoundException"


class SecretNotFoundException(SecretsManagerClientError):
    code = "ResourceNotFoundException"
    message = "Secrets Manager can't find the specified secret."


class SecretHasNoValueException(SecretsManagerClientError):
    code = "ResourceNotFoundException"

    def __init__(self, version_stage: str):
        super().__init__(
            "Secrets Manager can't find the specified secret "
            f"value for staging label: {version_stage}"
        )


class SecretStageVersionMismatchException(SecretsManagerClientError):
    code = "InvalidRequestException"
    message = (
        "You provided a VersionStage that is not associated to the provided VersionId."
    )


class ClientError(SecretsManagerClientError):
    code = "InvalidParameterValue"


class InvalidParameterException(SecretsManagerClientError):
    code = "InvalidParameterException"


class ResourceExistsException(SecretsManagerClientError):
    code = "ResourceExistsException"


class InvalidRequestException(SecretsManagerClientError):
    code = "InvalidRequestException"


class ValidationException(SecretsManagerClientError):
    code = "ValidationException"


class OperationNotPermittedOnReplica(InvalidParameterException):
    message = "Operation not permitted on a replica secret. Call must be made in primary secret's region."
