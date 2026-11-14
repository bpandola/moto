"""Exceptions raised by the kafka service."""

from moto.core.exceptions import ServiceException


class KafkaException(ServiceException):
    pass


class NotFoundException(KafkaException):
    code = "NotFoundException"


class BadRequestException(KafkaException):
    code = "BadRequestException"
