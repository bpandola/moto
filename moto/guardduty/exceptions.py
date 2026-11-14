from moto.core.exceptions import ServiceException


class GuardDutyException(ServiceException):
    pass


class DetectorNotFoundException(GuardDutyException):
    code = "BadRequestException"
    message = "The request is rejected because the input detectorId is not owned by the current account."


class FilterNotFoundException(GuardDutyException):
    code = "BadRequestException"
    message = "The request is rejected since no such resource found."


class IPSetNotFoundException(GuardDutyException):
    code = "BadRequestException"
    message = "The request is rejected since no such resource found."
