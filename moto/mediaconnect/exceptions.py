from moto.core.exceptions import ServiceException


class MediaConnectException(ServiceException):
    pass


class NotFoundException(MediaConnectException):
    code = "NotFoundException"
