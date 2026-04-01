from moto.core.exceptions import ServiceException as JsonRESTError


class AccountAlreadyRegisteredException(JsonRESTError):
    def __init__(self) -> None:
        super().__init__(
            "AccountAlreadyRegisteredException",
            "The provided account is already a delegated administrator for your organization.",
        )


class AccountAlreadyClosedException(JsonRESTError):
    def __init__(self) -> None:
        super().__init__(
            "AccountAlreadyClosedException",
            "The provided account is already closed.",
        )


class AccountNotRegisteredException(JsonRESTError):
    def __init__(self) -> None:
        super().__init__(
            "AccountNotRegisteredException",
            "The provided account is not a registered delegated administrator for your organization.",
        )


class AccountNotFoundException(JsonRESTError):
    def __init__(self) -> None:
        super().__init__(
            "AccountNotFoundException", "You specified an account that doesn't exist."
        )


class AlreadyInOrganizationException(JsonRESTError):
    def __init__(self) -> None:
        super().__init__(
            "AlreadyInOrganizationException",
            "The provided account is already a member of an organization.",
        )


class AWSOrganizationsNotInUseException(JsonRESTError):
    def __init__(self) -> None:
        super().__init__(
            "AWSOrganizationsNotInUseException",
            "Your account is not a member of an organization.",
        )


class ConstraintViolationException(JsonRESTError):
    def __init__(self, message: str):
        super().__init__("ConstraintViolationException", message)


class InvalidInputException(JsonRESTError):
    def __init__(self, message: str):
        super().__init__("InvalidInputException", message)


class DuplicateOrganizationalUnitException(JsonRESTError):
    def __init__(self) -> None:
        super().__init__(
            "DuplicateOrganizationalUnitException",
            "An OU with the same name already exists.",
        )


class DuplicatePolicyException(JsonRESTError):
    def __init__(self) -> None:
        super().__init__(
            "DuplicatePolicyException", "A policy with the same name already exists."
        )


class OrganizationNotEmptyException(JsonRESTError):
    def __init__(self) -> None:
        super().__init__(
            "OrganizationNotEmptyException",
            "To delete an organization you must first remove all member accounts (except the master).",
        )


class PolicyTypeAlreadyEnabledException(JsonRESTError):
    def __init__(self) -> None:
        super().__init__(
            "PolicyTypeAlreadyEnabledException",
            "The specified policy type is already enabled.",
        )


class PolicyTypeNotEnabledException(JsonRESTError):
    def __init__(self) -> None:
        super().__init__(
            "PolicyTypeNotEnabledException",
            "This operation can be performed only for enabled policy types.",
        )


class RootNotFoundException(JsonRESTError):
    def __init__(self) -> None:
        super().__init__(
            "RootNotFoundException", "You specified a root that doesn't exist."
        )


class TargetNotFoundException(JsonRESTError):
    def __init__(self) -> None:
        super().__init__(
            "TargetNotFoundException", "You specified a target that doesn't exist."
        )


class PolicyNotFoundException(JsonRESTError):
    def __init__(self, message: str) -> None:
        super().__init__("PolicyNotFoundException", message)
