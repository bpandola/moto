from moto.core.exceptions import ServiceException


class IoTClientError(ServiceException):
    pass


class ResourceNotFoundException(IoTClientError):
    code = "ResourceNotFoundException"

    def __init__(self, msg: str | None = None):
        super().__init__(msg or "The specified resource does not exist")


class InvalidRequestException(IoTClientError):
    code = "InvalidRequestException"

    def __init__(self, msg: str | None = None):
        super().__init__(msg or "The request is not valid.")


class InvalidStateTransitionException(IoTClientError):
    code = "InvalidStateTransitionException"

    def __init__(self, msg: str | None = None):
        super().__init__(msg or "An attempt was made to change to an invalid state.")


class VersionConflictException(IoTClientError):
    code = "VersionConflictException"

    def __init__(self, name: str):
        super().__init__(
            f"The version for thing {name} does not match the expected version."
        )


class CertificateStateException(IoTClientError):
    code = "CertificateStateException"

    def __init__(self, msg: str, cert_id: str):
        super().__init__(f"{msg} Id: {cert_id}")


class DeleteConflictException(IoTClientError):
    code = "DeleteConflictException"


class ResourceAlreadyExistsException(IoTClientError):
    code = "ResourceAlreadyExistsException"

    def __init__(self, msg: str, resource_id: str, resource_arn: str):
        super().__init__(msg or "The resource already exists.")
        self.resourceId = resource_id
        self.resourceArn = resource_arn


class VersionsLimitExceededException(IoTClientError):
    code = "VersionsLimitExceededException"

    def __init__(self, name: str):
        super().__init__(
            f"The policy {name} already has the maximum number of versions (5)"
        )


class ThingStillAttached(IoTClientError):
    code = "InvalidRequestException"

    def __init__(self, name: str):
        super().__init__(
            f"Cannot delete. Thing {name} is still attached to one or more principals"
        )


class ConflictException(IoTClientError):
    code = "ConflictException"

    def __init__(self, name: str):
        super().__init__(f"A resource {name} already exists.")
