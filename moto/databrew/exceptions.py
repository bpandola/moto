from moto.core.exceptions import ServiceException


class DataBrewClientError(ServiceException):
    pass


class AlreadyExistsException(DataBrewClientError):
    code = "AlreadyExistsException"

    def __init__(self, typ: str):
        super().__init__(f"{typ} already exists.")


class ConflictException(DataBrewClientError):
    code = "ConflictException"


class ValidationException(DataBrewClientError):
    code = "ValidationException"


class RulesetAlreadyExistsException(AlreadyExistsException):
    def __init__(self) -> None:
        super().__init__("Ruleset")


class EntityNotFoundException(DataBrewClientError):
    code = "EntityNotFoundException"


class ResourceNotFoundException(DataBrewClientError):
    code = "ResourceNotFoundException"


class RulesetNotFoundException(EntityNotFoundException):
    def __init__(self, recipe_name: str):
        super().__init__(f"Ruleset {recipe_name} not found.")


class ServiceQuotaExceededException(DataBrewClientError):
    code = "ServiceQuotaExceededException"
    message = "A service quota is exceeded."
