"""Exceptions raised by the mediapackagev2 service."""

from moto.core.exceptions import ServiceException


class MediaPackageV2Exception(ServiceException):
    pass


class ChannelGroupNotEmpty(MediaPackageV2Exception):
    code = "ConflictException"
    message = "The channel group you tried to delete has channels attached to it. If you want to delete this channel group, you must first delete the channels attached to it"


class ChannelNotFound(MediaPackageV2Exception):
    code = "ResourceNotFoundException"
    message = "MediaPackage can't process your request because we can't find your channel. Verify your channel name or add a channel and then try again"


class ChannelGroupNotFound(MediaPackageV2Exception):
    code = "ResourceNotFoundException"
    message = "MediaPackage can't process your request because we can't find your channel group. Verify your channel group name or add a channel group and then try again"
