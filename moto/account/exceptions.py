"""Exceptions raised by the account service."""

from moto.core.exceptions import ServiceException


class AccountException(ServiceException):
    pass


class UnknownContactType(AccountException):
    code = "AccessDeniedException"

    def __init__(self, user_arn: str, action: str):
        super().__init__(
            f"User: {user_arn} is not authorized to perform: account:{action} (You specified an invalid Alternate Contact type.)"
        )


class UnspecifiedContactType(AccountException):
    code = "ResourceNotFoundException"
    message = "No contact of the inputted alternate contact type found."
