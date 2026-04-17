"""Handles incoming kafka requests, invokes methods, returns responses."""

import json
from typing import Any

from moto.core.responses import BaseResponse

from .models import KafkaBackend, kafka_backends


def _to_camel_case(name: str) -> str:
    """Convert PascalCase to camelCase."""
    if not name:
        return name
    return name[0].lower() + name[1:]


def _convert_keys(obj: Any) -> Any:
    """Recursively convert PascalCase dict keys to camelCase."""
    if isinstance(obj, dict):
        return {_to_camel_case(k): _convert_keys(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_convert_keys(item) for item in obj]
    return obj


class KafkaResponse(BaseResponse):
    """Handler for Kafka requests and responses."""

    def __init__(self) -> None:
        super().__init__(service_name="kafka")
        self.automated_parameter_parsing = True

    @property
    def kafka_backend(self) -> KafkaBackend:
        """Return backend instance specific for this region."""
        return kafka_backends[self.current_account][self.region]

    def create_cluster_v2(self) -> str:
        cluster_name = self._get_param("ClusterName")
        tags = self._get_param("Tags")
        provisioned = _convert_keys(self._get_param("Provisioned"))
        serverless = _convert_keys(self._get_param("Serverless"))
        cluster_arn, cluster_name, state, cluster_type = (
            self.kafka_backend.create_cluster_v2(
                cluster_name=cluster_name,
                tags=tags,
                provisioned=provisioned,
                serverless=serverless,
            )
        )
        return json.dumps(
            {
                "clusterArn": cluster_arn,
                "clusterName": cluster_name,
                "state": state,
                "clusterType": cluster_type,
            }
        )

    def describe_cluster_v2(self) -> str:
        cluster_arn = self._get_param("ClusterArn")
        cluster_info = self.kafka_backend.describe_cluster_v2(
            cluster_arn=cluster_arn,
        )
        return json.dumps({"clusterInfo": cluster_info})

    def list_clusters_v2(self) -> str:
        cluster_name_filter = self._get_param("ClusterNameFilter")
        cluster_type_filter = self._get_param("ClusterTypeFilter")
        max_results = self._get_param("MaxResults")
        next_token = self._get_param("NextToken")
        cluster_info_list, next_token = self.kafka_backend.list_clusters_v2(
            cluster_name_filter=cluster_name_filter,
            cluster_type_filter=cluster_type_filter,
            max_results=max_results,
            next_token=next_token,
        )
        return json.dumps(
            {"clusterInfoList": cluster_info_list, "nextToken": next_token}
        )

    def list_tags_for_resource(self) -> str:
        resource_arn = self._get_param("ResourceArn")
        tags = self.kafka_backend.list_tags_for_resource(
            resource_arn=resource_arn,
        )
        return json.dumps({"tags": tags})

    def tag_resource(self) -> str:
        resource_arn = self._get_param("ResourceArn")
        tags = self._get_param("Tags")
        self.kafka_backend.tag_resource(
            resource_arn=resource_arn,
            tags=tags,
        )
        return json.dumps({})

    def untag_resource(self) -> str:
        resource_arn = self._get_param("ResourceArn")
        tag_keys = self._get_param("TagKeys")
        self.kafka_backend.untag_resource(
            resource_arn=resource_arn,
            tag_keys=tag_keys,
        )
        return json.dumps({})

    def create_cluster(self) -> str:
        broker_node_group_info = _convert_keys(self._get_param("BrokerNodeGroupInfo"))
        client_authentication = _convert_keys(self._get_param("ClientAuthentication"))
        cluster_name = self._get_param("ClusterName")
        configuration_info = _convert_keys(self._get_param("ConfigurationInfo"))
        encryption_info = _convert_keys(self._get_param("EncryptionInfo"))
        enhanced_monitoring = self._get_param("EnhancedMonitoring")
        open_monitoring = _convert_keys(self._get_param("OpenMonitoring"))
        kafka_version = self._get_param("KafkaVersion")
        logging_info = _convert_keys(self._get_param("LoggingInfo"))
        number_of_broker_nodes = self._get_param("NumberOfBrokerNodes")
        tags = self._get_param("Tags")
        storage_mode = self._get_param("StorageMode")
        cluster_arn, cluster_name, state = self.kafka_backend.create_cluster(
            broker_node_group_info=broker_node_group_info,
            client_authentication=client_authentication,
            cluster_name=cluster_name,
            configuration_info=configuration_info,
            encryption_info=encryption_info,
            enhanced_monitoring=enhanced_monitoring,
            open_monitoring=open_monitoring,
            kafka_version=kafka_version,
            logging_info=logging_info,
            number_of_broker_nodes=number_of_broker_nodes,
            tags=tags,
            storage_mode=storage_mode,
        )
        return json.dumps(
            {"clusterArn": cluster_arn, "clusterName": cluster_name, "state": state}
        )

    def describe_cluster(self) -> str:
        cluster_arn = self._get_param("ClusterArn")
        cluster_info = self.kafka_backend.describe_cluster(
            cluster_arn=cluster_arn,
        )
        return json.dumps({"clusterInfo": cluster_info})

    def delete_cluster(self) -> str:
        cluster_arn = self._get_param("ClusterArn")
        current_version = self._get_param("CurrentVersion")
        cluster_arn, state = self.kafka_backend.delete_cluster(
            cluster_arn=cluster_arn,
            current_version=current_version,
        )
        return json.dumps({"clusterArn": cluster_arn, "state": state})

    def put_cluster_policy(self) -> str:
        cluster_arn = self._get_param("ClusterArn")
        current_version = self._get_param("CurrentVersion")
        policy = self._get_param("Policy")

        new_version = self.kafka_backend.put_cluster_policy(
            cluster_arn=cluster_arn,
            current_version=current_version,
            policy=policy,
        )
        return json.dumps({"currentVersion": new_version})

    def get_cluster_policy(self) -> str:
        cluster_arn = self._get_param("ClusterArn")
        policy, current_version = self.kafka_backend.get_cluster_policy(
            cluster_arn=cluster_arn,
        )

        return json.dumps(
            {
                "currentVersion": current_version,
                "policy": policy,
            }
        )

    def delete_cluster_policy(self) -> str:
        cluster_arn = self._get_param("ClusterArn")
        self.kafka_backend.delete_cluster_policy(cluster_arn=cluster_arn)
        return json.dumps({})

    def list_clusters(self) -> str:
        cluster_name_filter = self._get_param("ClusterNameFilter")
        max_results = self._get_param("MaxResults")
        next_token = self._get_param("NextToken")

        cluster_info_list = self.kafka_backend.list_clusters(
            cluster_name_filter=cluster_name_filter,
            max_results=max_results,
            next_token=next_token,
        )

        return json.dumps(
            {"clusterInfoList": cluster_info_list, "nextToken": next_token}
        )
