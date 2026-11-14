"""Exceptions raised by the connectcampaigns service."""

from moto.core.exceptions import ServiceException


class ConnectCampaignsException(ServiceException):
    pass


class ResourceNotFoundException(ConnectCampaignsException):
    """When a resource is not found."""

    code = "ResourceNotFoundException"


class ValidationException(ConnectCampaignsException):
    """When validation fails on input parameters."""

    code = "ValidationException"
