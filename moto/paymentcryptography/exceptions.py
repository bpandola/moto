"""Exceptions raised by the paymentcryptography service."""

from moto.core.exceptions import ServiceException


class PaymentCryptographyException(ServiceException):
    pass


class ResourceNotFoundException(PaymentCryptographyException):
    code = "ResourceNotFoundException"


class ConflictException(PaymentCryptographyException):
    code = "ConflictException"
