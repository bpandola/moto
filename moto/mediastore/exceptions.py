from moto.core.exceptions import ServiceException


class MediaStoreClientError(ServiceException):
    pass


class ContainerNotFoundException(MediaStoreClientError):
    code = "ContainerNotFoundException"

    def __init__(self, msg: str | None = None):
        super().__init__(msg or "The specified container does not exist")


class ResourceNotFoundException(MediaStoreClientError):
    code = "ResourceNotFoundException"

    def __init__(self, msg: str | None = None):
        super().__init__(msg or "The specified container does not exist")


class PolicyNotFoundException(MediaStoreClientError):
    code = "PolicyNotFoundException"

    def __init__(self, msg: str | None = None):
        super().__init__(
            msg or "The policy does not exist within the specfied container"
        )
