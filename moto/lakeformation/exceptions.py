"""Exceptions raised by the lakeformation service."""

from moto.core.exceptions import ServiceException


class LakeFormationException(ServiceException):
    pass


class EntityNotFound(LakeFormationException):
    code = "EntityNotFoundException"
    message = "Entity not found"


class InvalidInput(LakeFormationException):
    code = "InvalidInputException"


class AlreadyExists(LakeFormationException):
    code = "AlreadyExistsException"
