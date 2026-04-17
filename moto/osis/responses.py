"""Handles incoming osis requests, invokes methods, returns responses."""

import json
from urllib.parse import unquote

from moto.core.responses import BaseResponse

from .models import OpenSearchIngestionBackend, osis_backends


class OpenSearchIngestionResponse(BaseResponse):
    """Handler for OpenSearchIngestion requests and responses."""

    def __init__(self) -> None:
        super().__init__(service_name="osis")
        self.automated_parameter_parsing = True

    @property
    def osis_backend(self) -> OpenSearchIngestionBackend:
        """Return backend instance specific for this region."""
        return osis_backends[self.current_account][self.region]

    def create_pipeline(self) -> str:
        pipeline_name = self._get_param("PipelineName")
        min_units = self._get_param("MinUnits")
        max_units = self._get_param("MaxUnits")
        pipeline_configuration_body = self._get_param("PipelineConfigurationBody")
        log_publishing_options = self._get_param("LogPublishingOptions")
        vpc_options = self._get_param("VpcOptions")
        buffer_options = self._get_param("BufferOptions")
        encryption_at_rest_options = self._get_param("EncryptionAtRestOptions")
        tags = self._get_param("Tags")
        pipeline = self.osis_backend.create_pipeline(
            pipeline_name=pipeline_name,
            min_units=min_units,
            max_units=max_units,
            pipeline_configuration_body=pipeline_configuration_body,
            log_publishing_options=log_publishing_options,
            vpc_options=vpc_options,
            buffer_options=buffer_options,
            encryption_at_rest_options=encryption_at_rest_options,
            tags=tags,
        )
        return json.dumps({"Pipeline": pipeline.to_dict()})

    def delete_pipeline(self) -> str:
        pipeline_name = self._get_param("PipelineName")
        self.osis_backend.delete_pipeline(
            pipeline_name=pipeline_name,
        )
        return json.dumps({})

    def get_pipeline(self) -> str:
        pipeline_name = self._get_param("PipelineName")
        pipeline = self.osis_backend.get_pipeline(
            pipeline_name=pipeline_name,
        )
        return json.dumps({"Pipeline": pipeline.to_dict()})

    def list_pipelines(self) -> str:
        max_results = self._get_int_param("MaxResults")
        next_token = self._get_param("NextToken")
        pipelines, next_token = self.osis_backend.list_pipelines(
            max_results=max_results,
            next_token=next_token,
        )
        return json.dumps(
            {
                "nextToken": next_token,
                "Pipelines": [p.to_short_dict() for p in pipelines],
            }
        )

    def list_tags_for_resource(self) -> str:
        arn = self._get_param("Arn")
        tags = self.osis_backend.list_tags_for_resource(arn=arn)
        return json.dumps(dict(tags))

    def update_pipeline(self) -> str:
        pipeline_name = self._get_param("PipelineName")
        min_units = self._get_param("MinUnits")
        max_units = self._get_param("MaxUnits")
        pipeline_configuration_body = self._get_param("PipelineConfigurationBody")
        log_publishing_options = self._get_param("LogPublishingOptions")
        buffer_options = self._get_param("BufferOptions")
        encryption_at_rest_options = self._get_param("EncryptionAtRestOptions")
        pipeline = self.osis_backend.update_pipeline(
            pipeline_name=pipeline_name,
            min_units=min_units,
            max_units=max_units,
            pipeline_configuration_body=pipeline_configuration_body,
            log_publishing_options=log_publishing_options,
            buffer_options=buffer_options,
            encryption_at_rest_options=encryption_at_rest_options,
        )
        # TODO: adjust response
        return json.dumps({"Pipeline": pipeline.to_dict()})

    def tag_resource(self) -> str:
        arn = self._get_param("Arn")
        tags = self._get_param("Tags")
        self.osis_backend.tag_resource(
            arn=arn,
            tags=tags,
        )
        return json.dumps({})

    def untag_resource(self) -> str:
        arn = self._get_param("Arn")
        tag_keys = self._get_param("TagKeys")
        self.osis_backend.untag_resource(
            arn=arn,
            tag_keys=tag_keys,
        )
        return json.dumps({})

    def start_pipeline(self) -> str:
        pipeline_name = self._get_param("PipelineName")
        pipeline = self.osis_backend.start_pipeline(
            pipeline_name=pipeline_name,
        )
        return json.dumps({"Pipeline": pipeline.to_dict()})

    def stop_pipeline(self) -> str:
        pipeline_name = self._get_param("PipelineName")
        pipeline = self.osis_backend.stop_pipeline(
            pipeline_name=pipeline_name,
        )
        return json.dumps({"Pipeline": pipeline.to_dict()})

    def get_resource_policy(self) -> str:
        resource_arn = unquote(self._get_param("ResourceArn"))
        policy = self.osis_backend.get_resource_policy(
            resource_arn=resource_arn,
        )
        return json.dumps({"ResourceArn": resource_arn, "Policy": policy})

    def put_resource_policy(self) -> str:
        resource_arn = unquote(self._get_param("ResourceArn"))
        policy = self._get_param("Policy")
        self.osis_backend.put_resource_policy(
            resource_arn=resource_arn,
            policy=policy,
        )
        return json.dumps({"ResourceArn": resource_arn, "Policy": policy})

    def delete_resource_policy(self) -> str:
        resource_arn = unquote(self._get_param("ResourceArn"))
        self.osis_backend.delete_resource_policy(
            resource_arn=resource_arn,
        )
        return json.dumps({})
