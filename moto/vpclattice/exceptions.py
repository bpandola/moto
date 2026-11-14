"""Exceptions raised by the vpclattice service."""

from moto.core.exceptions import ServiceException


class VPCLatticeException(ServiceException):
    pass


class ValidationException(VPCLatticeException):
    code = "ValidationException"


class ResourceNotFoundException(VPCLatticeException):
    code = "ResourceNotFoundException"
