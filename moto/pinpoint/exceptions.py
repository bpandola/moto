"""Exceptions raised by the pinpoint service."""

from moto.core.exceptions import ServiceException


class PinpointExceptions(ServiceException):
    pass


class ApplicationNotFound(PinpointExceptions):
    code = "NotFoundException"
    message = "Application not found"


class EventStreamNotFound(PinpointExceptions):
    code = "NotFoundException"
    message = "Resource not found"
