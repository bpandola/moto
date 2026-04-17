"""Handles incoming cloudhsmv2 requests, invokes methods, returns responses."""

import json
from datetime import datetime
from typing import Any

from moto.core.responses import BaseResponse

from .models import CloudHSMV2Backend, cloudhsmv2_backends


class DateTimeEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, datetime):
            return o.isoformat()

        return super().default(o)


class CloudHSMV2Response(BaseResponse):
    """Handler for CloudHSMV2 requests and responses."""

    def __init__(self) -> None:
        super().__init__(service_name="cloudhsmv2")
        self.automated_parameter_parsing = True

    @property
    def cloudhsmv2_backend(self) -> CloudHSMV2Backend:
        """Return backend instance specific for this region."""
        return cloudhsmv2_backends[self.current_account][self.region]

    def list_tags(self) -> str:
        resource_id = self._get_param("ResourceId")
        next_token = self._get_param("NextToken")
        max_results = self._get_param("MaxResults")

        tag_list, next_token = self.cloudhsmv2_backend.list_tags(
            resource_id=resource_id,
            next_token=next_token,
            max_results=max_results,
        )

        return json.dumps({"TagList": tag_list, "NextToken": next_token})

    def tag_resource(self) -> str:
        resource_id = self._get_param("ResourceId")
        tag_list = self._get_param("TagList")

        self.cloudhsmv2_backend.tag_resource(
            resource_id=resource_id,
            tag_list=tag_list,
        )
        return json.dumps({})

    def untag_resource(self) -> str:
        resource_id = self._get_param("ResourceId")
        tag_key_list = self._get_param("TagKeyList")
        self.cloudhsmv2_backend.untag_resource(
            resource_id=resource_id,
            tag_key_list=tag_key_list,
        )
        return json.dumps({})

    def create_cluster(self) -> str:
        backup_retention_policy = self._get_param("BackupRetentionPolicy", {})
        hsm_type = self._get_param("HsmType")
        source_backup_id = self._get_param("SourceBackupId")
        subnet_ids = self._get_param("SubnetIds", [])
        network_type = self._get_param("NetworkType")
        tag_list = self._get_param("TagList")
        mode = self._get_param("Mode")

        cluster = self.cloudhsmv2_backend.create_cluster(
            backup_retention_policy=backup_retention_policy,
            hsm_type=hsm_type,
            source_backup_id=source_backup_id,
            subnet_ids=subnet_ids,
            network_type=network_type,
            tag_list=tag_list,
            mode=mode,
        )
        return json.dumps({"Cluster": cluster}, cls=DateTimeEncoder)

    def delete_cluster(self) -> str:
        cluster_id = self._get_param("ClusterId")

        cluster = self.cloudhsmv2_backend.delete_cluster(cluster_id=cluster_id)
        return json.dumps({"Cluster": cluster}, cls=DateTimeEncoder)

    def describe_clusters(self) -> str:
        filters = self._get_param("Filters", {})
        next_token = self._get_param("NextToken")
        max_results = self._get_param("MaxResults")

        clusters, next_token = self.cloudhsmv2_backend.describe_clusters(
            filters=filters,
            next_token=next_token,
            max_results=max_results,
        )

        response = {"Clusters": clusters, "NextToken": next_token}

        return json.dumps(response, cls=DateTimeEncoder)

    def get_resource_policy(self) -> str:
        resource_arn = self._get_param("ResourceArn")
        policy = self.cloudhsmv2_backend.get_resource_policy(
            resource_arn=resource_arn,
        )
        return json.dumps({"Policy": policy})

    def describe_backups(self) -> str:
        next_token = self._get_param("NextToken")
        max_results = self._get_param("MaxResults")
        filters = self._get_param("Filters", {})
        shared = self._get_param("Shared")
        sort_ascending = self._get_param("SortAscending")

        backups, next_token = self.cloudhsmv2_backend.describe_backups(
            next_token=next_token,
            max_results=max_results,
            filters=filters,
            shared=shared,
            sort_ascending=sort_ascending,
        )

        response = {"Backups": [b.to_dict() for b in backups], "NextToken": next_token}

        return json.dumps(response, cls=DateTimeEncoder)

    def put_resource_policy(self) -> str:
        resource_arn = self._get_param("ResourceArn")
        policy = self._get_param("Policy")

        result = self.cloudhsmv2_backend.put_resource_policy(
            resource_arn=resource_arn,
            policy=policy,
        )
        return json.dumps(result)
