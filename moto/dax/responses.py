import json
import re

from moto.core.responses import BaseResponse

from .exceptions import InvalidParameterValueException
from .models import DAXBackend, dax_backends


class DAXResponse(BaseResponse):
    def __init__(self) -> None:
        super().__init__(service_name="dax")
        self.automated_parameter_parsing = True

    @property
    def dax_backend(self) -> DAXBackend:
        return dax_backends[self.current_account][self.region]

    def create_cluster(self) -> str:
        cluster_name = self._get_param("ClusterName")
        node_type = self._get_param("NodeType")
        description = self._get_param("Description")
        replication_factor = self._get_param("ReplicationFactor")
        iam_role_arn = self._get_param("IamRoleArn")
        tags = self._get_param("Tags", [])
        sse_specification = self._get_param("SSESpecification", {})
        encryption_type = self._get_param("ClusterEndpointEncryptionType", "NONE")

        self._validate_arn(iam_role_arn)
        self._validate_name(cluster_name)

        cluster = self.dax_backend.create_cluster(
            cluster_name=cluster_name,
            node_type=node_type,
            description=description,
            replication_factor=replication_factor,
            iam_role_arn=iam_role_arn,
            tags=tags,
            sse_specification=sse_specification,
            encryption_type=encryption_type,
        )
        return json.dumps({"Cluster": cluster.to_json()})

    def delete_cluster(self) -> str:
        cluster_name = self._get_param("ClusterName")
        cluster = self.dax_backend.delete_cluster(cluster_name)
        return json.dumps({"Cluster": cluster.to_json()})

    def describe_clusters(self) -> str:
        cluster_names = self._get_param("ClusterNames", [])
        max_results = self._get_param("MaxResults")
        next_token = self._get_param("NextToken")

        for name in cluster_names:
            self._validate_name(name)

        clusters, next_token = self.dax_backend.describe_clusters(
            cluster_names=cluster_names, max_results=max_results, next_token=next_token
        )
        return json.dumps(
            {"Clusters": [c.to_json() for c in clusters], "NextToken": next_token}
        )

    def _validate_arn(self, arn: str) -> None:
        if not arn.startswith("arn:"):
            raise InvalidParameterValueException(f"ARNs must start with 'arn:': {arn}")
        sections = arn.split(":")
        if len(sections) < 3:
            raise InvalidParameterValueException(
                f"Second colon partition not found: {arn}"
            )
        if len(sections) < 4:
            raise InvalidParameterValueException(f"Third colon vendor not found: {arn}")
        if len(sections) < 5:
            raise InvalidParameterValueException(
                f"Fourth colon (region/namespace delimiter) not found: {arn}"
            )
        if len(sections) < 6:
            raise InvalidParameterValueException(
                f"Fifth colon (namespace/relative-id delimiter) not found: {arn}"
            )

    def _validate_name(self, name: str) -> None:
        msg = "Cluster ID specified is not a valid identifier. Identifiers must begin with a letter; must contain only ASCII letters, digits, and hyphens; and must not end with a hyphen or contain two consecutive hyphens."
        if not re.match("^[a-z][a-z0-9-]+[a-z0-9]$", name):
            raise InvalidParameterValueException(msg)
        if "--" in name:
            raise InvalidParameterValueException(msg)

    def list_tags(self) -> str:
        resource_name = self._get_param("ResourceName")
        tags = self.dax_backend.list_tags(resource_name=resource_name)
        return json.dumps(tags)

    def increase_replication_factor(self) -> str:
        cluster_name = self._get_param("ClusterName")
        new_replication_factor = self._get_param("NewReplicationFactor")
        cluster = self.dax_backend.increase_replication_factor(
            cluster_name=cluster_name, new_replication_factor=new_replication_factor
        )
        return json.dumps({"Cluster": cluster.to_json()})

    def decrease_replication_factor(self) -> str:
        cluster_name = self._get_param("ClusterName")
        new_replication_factor = self._get_param("NewReplicationFactor")
        node_ids_to_remove = self._get_param("NodeIdsToRemove")
        cluster = self.dax_backend.decrease_replication_factor(
            cluster_name=cluster_name,
            new_replication_factor=new_replication_factor,
            node_ids_to_remove=node_ids_to_remove,
        )
        return json.dumps({"Cluster": cluster.to_json()})

    def tag_resource(self) -> str:
        resource_name = self._get_param("ResourceName")
        tags = self._get_param("Tags")
        self.dax_backend.tag_resource(
            resource_name=resource_name,
            tags=tags,
        )
        return "{}"

    def untag_resource(self) -> str:
        resource_name = self._get_param("ResourceName")
        tag_keys = self._get_param("TagKeys")
        self.dax_backend.untag_resource(
            resource_name=resource_name,
            tag_keys=tag_keys,
        )
        return "{}"
