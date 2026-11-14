from moto.core.exceptions import ServiceException


class MediaPackageClientError(ServiceException):
    pass


# AWS service exceptions are caught with the underlying botocore exception, ClientError
class ClientError(MediaPackageClientError):
    def __init__(self, error: str, message: str):
        super().__init__(error, message)
