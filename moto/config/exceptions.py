from typing import Any

from moto.core.exceptions import ServiceException


class ConfigException(ServiceException):
    pass


class NameTooLongException(ConfigException):
    code = "ValidationException"

    def __init__(self, name: str, location: str, max_limit: int = 256):
        message = (
            f"1 validation error detected: Value '{name}' at '{location}' "
            f"failed to satisfy constraint: Member must have length less "
            f"than or equal to {max_limit}"
        )
        super().__init__(message)


class InvalidConfigurationRecorderNameException(ConfigException):
    code = "InvalidConfigurationRecorderNameException"

    def __init__(self, name: str | None):
        message = (
            f"The configuration recorder name '{name}' is not valid, blank string."
        )
        super().__init__(message)


class MaxNumberOfConfigurationRecordersExceededException(ConfigException):
    code = "MaxNumberOfConfigurationRecordersExceededException"

    def __init__(self, name: str):
        message = (
            f"Failed to put configuration recorder '{name}' because the maximum number of "
            "configuration recorders: 1 is reached."
        )
        super().__init__(message)


class InvalidRecordingGroupException(ConfigException):
    code = "InvalidRecordingGroupException"

    def __init__(self) -> None:
        message = "The recording group provided is not valid"
        super().__init__(message)


class InvalidResourceTypeException(ConfigException):
    code = "ValidationException"

    def __init__(self, bad_list: list[str], good_list: Any):
        message = (
            f"{len(bad_list)} validation error detected: Value '{bad_list}' at "
            "'configurationRecorder.recordingGroup.resourceTypes' failed to satisfy constraint: "
            f"Member must satisfy constraint: [Member must satisfy enum value set: {good_list}]"
        )

        super().__init__(message)


class NoSuchConfigurationAggregatorException(ConfigException):
    code = "NoSuchConfigurationAggregatorException"

    def __init__(self, number: int = 1):
        if number == 1:
            message = "The configuration aggregator does not exist. Check the configuration aggregator name and try again."
        else:
            message = (
                "At least one of the configuration aggregators does not exist. Check the configuration aggregator"
                " names and try again."
            )
        super().__init__(message)


class NoSuchConfigurationRecorderException(ConfigException):
    code = "NoSuchConfigurationRecorderException"

    def __init__(self, name: str):
        message = (
            f"Cannot find configuration recorder with the specified name '{name}'."
        )
        super().__init__(message)


class InvalidDeliveryChannelNameException(ConfigException):
    code = "InvalidDeliveryChannelNameException"

    def __init__(self, name: str | None):
        message = f"The delivery channel name '{name}' is not valid, blank string."
        super().__init__(message)


class NoSuchBucketException(ConfigException):
    """We are *only* validating that there is value that is not '' here."""

    code = "NoSuchBucketException"

    def __init__(self) -> None:
        message = "Cannot find a S3 bucket with an empty bucket name."
        super().__init__(message)


class InvalidNextTokenException(ConfigException):
    code = "InvalidNextTokenException"

    def __init__(self) -> None:
        message = "The nextToken provided is invalid"
        super().__init__(message)


class InvalidS3KeyPrefixException(ConfigException):
    code = "InvalidS3KeyPrefixException"

    def __init__(self) -> None:
        message = "The s3 key prefix '' is not valid, empty s3 key prefix."
        super().__init__(message)


class InvalidS3KmsKeyArnException(ConfigException):
    code = "InvalidS3KmsKeyArnException"

    def __init__(self) -> None:
        message = "The arn '' is not a valid kms key or alias arn."
        super().__init__(message)


class InvalidSNSTopicARNException(ConfigException):
    """We are *only* validating that there is value that is not '' here."""

    code = "InvalidSNSTopicARNException"

    def __init__(self) -> None:
        message = "The sns topic arn '' is not valid."
        super().__init__(message)


class InvalidDeliveryFrequency(ConfigException):
    code = "InvalidDeliveryFrequency"

    def __init__(self, value: str | None, good_list: Any):
        message = (
            f"1 validation error detected: Value '{value}' at "
            "'deliveryChannel.configSnapshotDeliveryProperties.deliveryFrequency' failed to satisfy "
            f"constraint: Member must satisfy enum value set: {good_list}"
        )
        super().__init__(message)


class MaxNumberOfDeliveryChannelsExceededException(ConfigException):
    code = "MaxNumberOfDeliveryChannelsExceededException"

    def __init__(self, name: str):
        message = f"Failed to put delivery channel '{name}' because the maximum number of delivery channels: 1 is reached."
        super().__init__(message)


class NoSuchDeliveryChannelException(ConfigException):
    code = "NoSuchDeliveryChannelException"

    def __init__(self, name: str):
        message = f"Cannot find delivery channel with specified name '{name}'."
        super().__init__(message)


class NoAvailableConfigurationRecorderException(ConfigException):
    code = "NoAvailableConfigurationRecorderException"

    def __init__(self) -> None:
        message = "Configuration recorder is not available to put delivery channel."
        super().__init__(message)


class NoAvailableDeliveryChannelException(ConfigException):
    code = "NoAvailableDeliveryChannelException"

    def __init__(self) -> None:
        message = "Delivery channel is not available to start configuration recorder."
        super().__init__(message)


class LastDeliveryChannelDeleteFailedException(ConfigException):
    code = "LastDeliveryChannelDeleteFailedException"

    def __init__(self, name: str):
        message = (
            f"Failed to delete last specified delivery channel with name '{name}', because there, "
            "because there is a running configuration recorder."
        )
        super().__init__(message)


class TooManyAccountSources(ConfigException):
    code = "ValidationException"

    def __init__(self, length: int):
        locations = ["com.amazonaws.xyz"] * length

        locs = ", ".join(locations)
        message = (
            f"Value '[{locs}]' at 'accountAggregationSources' failed to satisfy constraint: "
            "Member must have length less than or equal to 1"
        )
        super().__init__(message)


class DuplicateTags(ConfigException):
    code = "InvalidInput"

    def __init__(self) -> None:
        super().__init__(
            "Duplicate tag keys found. Please note that Tag keys are case insensitive.",
        )


class TagKeyTooBig(ConfigException):
    code = "ValidationException"

    def __init__(self, tag: str, param: str = "tags.X.member.key"):
        super().__init__(
            f"1 validation error detected: Value '{tag}' at '{param}' failed to satisfy "
            "constraint: Member must have length less than or equal to 128",
        )


class TagValueTooBig(ConfigException):
    code = "ValidationException"

    def __init__(self, tag: str, param: str = "tags.X.member.value"):
        super().__init__(
            f"1 validation error detected: Value '{tag}' at '{param}' failed to satisfy "
            "constraint: Member must have length less than or equal to 256",
        )


class InvalidParameterValueException(ConfigException):
    code = "InvalidParameterValueException"

    def __init__(self, message: str):
        super().__init__(message)


class InvalidTagCharacters(ConfigException):
    code = "ValidationException"

    def __init__(self, tag: str, param: str = "tags.X.member.key"):
        message = f"1 validation error detected: Value '{tag}' at '{param}' failed to satisfy "
        message += "constraint: Member must satisfy regular expression pattern: [\\\\p{L}\\\\p{Z}\\\\p{N}_.:/=+\\\\-@]+"

        super().__init__(message)


class TooManyTags(ConfigException):
    code = "ValidationException"

    def __init__(self, tags: Any, param: str = "tags"):
        super().__init__(
            f"1 validation error detected: Value '{tags}' at '{param}' failed to satisfy "
            "constraint: Member must have length less than or equal to 50.",
        )


class InvalidResourceParameters(ConfigException):
    code = "ValidationException"

    def __init__(self) -> None:
        super().__init__(
            "Both Resource ID and Resource Name cannot be specified in the request",
        )


class InvalidLimitException(ConfigException):
    code = "InvalidLimitException"

    def __init__(self, value: int):
        super().__init__(
            f"Value '{value}' at 'limit' failed to satisfy constraint: Member"
            " must have value less than or equal to 100",
        )


class TooManyResourceIds(ConfigException):
    code = "ValidationException"

    def __init__(self) -> None:
        super().__init__(
            "The specified list had more than 20 resource ID's. "
            "It must have '20' or less items",
        )


class ResourceNotDiscoveredException(ConfigException):
    code = "ResourceNotDiscoveredException"

    def __init__(self, resource_type: str, resource: str):
        super().__init__(
            f"Resource {resource} of resourceType:{resource_type} is unknown or has not been discovered",
        )


class ResourceNotFoundException(ConfigException):
    code = "ResourceNotFoundException"

    def __init__(self, resource_arn: str):
        super().__init__(f"ResourceArn '{resource_arn}' does not exist")


class TooManyResourceKeys(ConfigException):
    code = "ValidationException"

    def __init__(self, bad_list: list[str]):
        message = (
            f"1 validation error detected: Value '{bad_list}' at "
            "'resourceKeys' failed to satisfy constraint: "
            "Member must have length less than or equal to 100"
        )
        super().__init__(message)


class InvalidResultTokenException(ConfigException):
    code = "InvalidResultTokenException"

    def __init__(self) -> None:
        message = "The resultToken provided is invalid"
        super().__init__(message)


class ValidationException(ConfigException):
    code = "ValidationException"

    def __init__(self, message: str):
        super().__init__(message)


class NoSuchOrganizationConformancePackException(ConfigException):
    code = "NoSuchOrganizationConformancePackException"

    def __init__(self, message: str):
        super().__init__(message)


class MaxNumberOfConfigRulesExceededException(ConfigException):
    code = "MaxNumberOfConfigRulesExceededException"

    def __init__(self, name: str, max_limit: int):
        message = f"Failed to put config rule '{name}' because the maximum number of config rules: {max_limit} is reached."
        super().__init__(message)


class ResourceInUseException(ConfigException):
    code = "ResourceInUseException"

    def __init__(self, message: str):
        super().__init__(message)


class InsufficientPermissionsException(ConfigException):
    code = "InsufficientPermissionsException"

    def __init__(self, message: str):
        super().__init__(message)


class NoSuchConfigRuleException(ConfigException):
    code = "NoSuchConfigRuleException"

    def __init__(self, rule_name: str):
        message = f"The ConfigRule '{rule_name}' provided in the request is invalid. Please check the configRule name"
        super().__init__(message)


class MissingRequiredConfigRuleParameterException(ConfigException):
    code = "ParamValidationError"

    def __init__(self, message: str):
        super().__init__(message)


class NoSuchRetentionConfigurationException(ConfigException):
    code = "NoSuchRetentionConfigurationException"

    def __init__(self, name: str):
        message = (
            f"Cannot find retention configuration with the specified name '{name}'."
        )
        super().__init__(message)


class InvalidResourceType(ConfigException):
    code = "InvalidResourceType"

    def __init__(self, resource_type: str, message: str):
        super().__init__(message)
