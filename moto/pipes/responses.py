"""Handles incoming pipes requests, invokes methods, returns responses."""

from typing import Any

from moto.core.responses import ActionResult, BaseResponse, EmptyResult

from .exceptions import ValidationException
from .models import EventBridgePipesBackend, pipes_backends


class EventBridgePipesResponse(BaseResponse):
    """Handler for EventBridgePipes requests and responses."""

    def __init__(self) -> None:
        super().__init__(service_name="pipes")
        self.automated_parameter_parsing = True

    @property
    def pipes_backend(self) -> EventBridgePipesBackend:
        return pipes_backends[self.current_account][self.region]

    def create_pipe(self) -> ActionResult:
        name = self._get_param("Name")
        description = self._get_param("Description")
        desired_state = self._get_param("DesiredState")
        source = self._get_param("Source")
        source_parameters = self._get_param("SourceParameters")
        enrichment = self._get_param("Enrichment")
        enrichment_parameters = self._get_param("EnrichmentParameters")
        target = self._get_param("Target")
        target_parameters = self._get_param("TargetParameters")
        role_arn = self._get_param("RoleArn")
        tags = self._get_param("Tags")
        log_configuration = self._get_param("LogConfiguration")
        kms_key_identifier = self._get_param("KmsKeyIdentifier")

        if not source:
            raise ValidationException("Source is a required parameter")
        if not target:
            raise ValidationException("Target is a required parameter")
        if not role_arn:
            raise ValidationException("RoleArn is a required parameter")

        pipe = self.pipes_backend.create_pipe(
            name=name,
            description=description,
            desired_state=desired_state,
            source=source,
            source_parameters=source_parameters,
            enrichment=enrichment,
            enrichment_parameters=enrichment_parameters,
            target=target,
            target_parameters=target_parameters,
            role_arn=role_arn,
            tags=tags,
            log_configuration=log_configuration,
            kms_key_identifier=kms_key_identifier,
        )
        return ActionResult(
            {
                "Arn": pipe.arn,
                "Name": pipe.name,
                "DesiredState": pipe.desired_state,
                "CurrentState": pipe.current_state,
                "CreationTime": pipe.creation_time,
                "LastModifiedTime": pipe.last_modified_time,
            }
        )

    def describe_pipe(self) -> ActionResult:
        name = self._get_param("Name")
        pipe = self.pipes_backend.describe_pipe(
            name=name,
        )
        response_dict: dict[str, Any] = {
            "Arn": pipe.arn,
            "Name": pipe.name,
            "DesiredState": pipe.desired_state,
            "CurrentState": pipe.current_state,
            "CreationTime": pipe.creation_time,
            "LastModifiedTime": pipe.last_modified_time,
        }
        if pipe.description is not None:
            response_dict["Description"] = pipe.description
        if pipe.state_reason is not None:
            response_dict["StateReason"] = pipe.state_reason
        if pipe.source is not None:
            response_dict["Source"] = pipe.source
        if pipe.source_parameters is not None:
            response_dict["SourceParameters"] = pipe.source_parameters
        if pipe.enrichment is not None:
            response_dict["Enrichment"] = pipe.enrichment
        if pipe.enrichment_parameters is not None:
            response_dict["EnrichmentParameters"] = pipe.enrichment_parameters
        if pipe.target is not None:
            response_dict["Target"] = pipe.target
        if pipe.target_parameters is not None:
            response_dict["TargetParameters"] = pipe.target_parameters
        if pipe.role_arn is not None:
            response_dict["RoleArn"] = pipe.role_arn
        if pipe.tags is not None:
            response_dict["Tags"] = pipe.tags
        if pipe.log_configuration is not None:
            response_dict["LogConfiguration"] = pipe.log_configuration
        if pipe.kms_key_identifier is not None:
            response_dict["KmsKeyIdentifier"] = pipe.kms_key_identifier
        return ActionResult(response_dict)

    def delete_pipe(self) -> ActionResult:
        name = self._get_param("Name")

        pipe = self.pipes_backend.delete_pipe(name=name)

        return ActionResult(
            {
                "Arn": pipe.arn,
                "Name": pipe.name,
                "DesiredState": pipe.desired_state,
                "CurrentState": pipe.current_state,
                "CreationTime": pipe.creation_time,
                "LastModifiedTime": pipe.last_modified_time,
            }
        )

    def tag_resource(self) -> ActionResult:
        resource_arn = self._get_param("resourceArn")
        tags = self._get_param("tags")
        if not tags:
            raise ValidationException("Tags is a required parameter")

        self.pipes_backend.tag_resource(
            resource_arn=resource_arn,
            tags=tags,
        )
        return EmptyResult()

    def untag_resource(self) -> ActionResult:
        resource_arn = self._get_param("resourceArn")
        tag_keys = self._get_param("tagKeys")

        self.pipes_backend.untag_resource(
            resource_arn=resource_arn,
            tag_keys=tag_keys,
        )
        return EmptyResult()

    def list_tags_for_resource(self) -> ActionResult:
        resource_arn = self._get_param("resourceArn")
        tags = self.pipes_backend.list_tags_for_resource(resource_arn)
        return ActionResult({"tags": tags})

    def list_pipes(self) -> ActionResult:
        name_prefix = self._get_param("NamePrefix")
        desired_state = self._get_param("DesiredState")
        current_state = self._get_param("CurrentState")
        source_prefix = self._get_param("SourcePrefix")
        target_prefix = self._get_param("TargetPrefix")
        next_token = self._get_param("NextToken")
        limit = self._get_param("Limit")

        pipes, next_token = self.pipes_backend.list_pipes(
            name_prefix=name_prefix,
            desired_state=desired_state,
            current_state=current_state,
            source_prefix=source_prefix,
            target_prefix=target_prefix,
            next_token=next_token,
            limit=limit,
        )

        response_dict: dict[str, Any] = {"Pipes": pipes}
        if next_token:
            response_dict["NextToken"] = next_token

        return ActionResult(response_dict)

    def start_pipe(self) -> ActionResult:
        name = self._get_param("Name")
        pipe = self.pipes_backend.start_pipe(name=name)
        response_dict: dict[str, Any] = {
            "Arn": pipe.arn,
            "Name": pipe.name,
            "DesiredState": pipe.desired_state,
            "CurrentState": pipe.current_state,
            "CreationTime": pipe.creation_time,
            "LastModifiedTime": pipe.last_modified_time,
        }
        return ActionResult(response_dict)

    def stop_pipe(self) -> ActionResult:
        name = self._get_param("Name")
        pipe = self.pipes_backend.stop_pipe(name=name)
        response_dict: dict[str, Any] = {
            "Arn": pipe.arn,
            "Name": pipe.name,
            "DesiredState": pipe.desired_state,
            "CurrentState": pipe.current_state,
            "CreationTime": pipe.creation_time,
            "LastModifiedTime": pipe.last_modified_time,
        }
        return ActionResult(response_dict)
