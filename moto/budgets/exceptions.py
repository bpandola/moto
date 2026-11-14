"""Exceptions raised by the budgets service."""

from moto.core.exceptions import ServiceException


class BudgetsException(ServiceException):
    pass


class DuplicateRecordException(BudgetsException):
    code = "DuplicateRecordException"

    def __init__(self, record_type: str, record_name: str):
        super().__init__(
            f"Error creating {record_type}: {record_name} - the {record_type} already exists."
        )


class NotFoundException(BudgetsException):
    code = "NotFoundException"


class BudgetMissingLimit(BudgetsException):
    code = "InvalidParameterException"
    message = "Unable to create/update budget - please provide one of the followings: Budget Limit/ Planned Budget Limit/ Auto Adjust Data"
