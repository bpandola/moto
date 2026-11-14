from moto.core.exceptions import ServiceException


class EventsException(ServiceException):
    pass


class IllegalStatusException(EventsException):
    code = "IllegalStatusException"


class InvalidEventPatternException(EventsException):
    code = "InvalidEventPatternException"

    def __init__(self, reason: str | None = None):
        msg = "Event pattern is not valid. "
        if reason:
            msg += f"Reason: {reason}"

        super().__init__(msg)


class InvalidParameterValueException(EventsException):
    code = "InvalidParameterValue"


class ResourceNotFoundException(EventsException):
    code = "ResourceNotFoundException"


class ResourceAlreadyExistsException(EventsException):
    code = "ResourceAlreadyExistsException"


class ValidationException(EventsException):
    code = "ValidationException"
