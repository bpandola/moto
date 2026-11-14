"""Exceptions raised by the appconfig service."""

from moto.core.exceptions import ServiceException


class AppConfigException(ServiceException):
    pass


class AppNotFoundException(AppConfigException):
    code = "ResourceNotFoundException"
    message = "Application not found"


class ConfigurationProfileNotFound(AppConfigException):
    code = "ResourceNotFoundException"
    message = "ConfigurationProfile not found"


class ConfigurationVersionNotFound(AppConfigException):
    code = "ResourceNotFoundException"
    message = "HostedConfigurationVersion not found"
