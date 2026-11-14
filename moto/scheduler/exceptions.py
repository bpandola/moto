"""Exceptions raised by the scheduler service."""

from moto.core.exceptions import ServiceException


class EventBridgeSchedulerException(ServiceException):
    pass


class ScheduleExists(EventBridgeSchedulerException):
    code = "ConflictException"

    def __init__(self, name: str) -> None:
        super().__init__(f"Schedule {name} already exists.")


class ScheduleNotFound(EventBridgeSchedulerException):
    code = "ResourceNotFoundException"

    def __init__(self, name: str) -> None:
        super().__init__(f"Schedule {name} does not exist.")


class ScheduleGroupNotFound(EventBridgeSchedulerException):
    code = "ResourceNotFoundException"

    def __init__(self, name: str) -> None:
        super().__init__(f"Schedule group {name} does not exist.")


class ValidationException(EventBridgeSchedulerException):
    code = "ValidationException"
