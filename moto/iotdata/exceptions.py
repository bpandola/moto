from moto.core.exceptions import ServiceException


class IoTDataPlaneClientError(ServiceException):
    pass


class ResourceNotFoundException(IoTDataPlaneClientError):
    code = "ResourceNotFoundException"
    message = "The specified resource does not exist"


class InvalidRequestException(IoTDataPlaneClientError):
    code = "InvalidRequestException"


class ConflictException(IoTDataPlaneClientError):
    code = "ConflictException"
