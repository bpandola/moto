"""Handles incoming kafka requests, invokes methods, returns responses."""

from typing import Any

from moto.core.responses import ActionResult, BaseResponse, EmptyResult

from .models import FakeKafkaCluster, KafkaBackend, kafka_backends


class KafkaResponse(BaseResponse):
    """Handler for Kafka requests and responses."""

    def __init__(self) -> None:
        super().__init__(service_name="kafka")
        self.automated_parameter_parsing = True

    @property
    def kafka_backend(self) -> KafkaBackend:
        """Return backend instance specific for this region."""
        return kafka_backends[self.current_account][self.region]

    def create_cluster_v2(self) -> ActionResult:
        cluster_name = self._get_param("ClusterName")
        tags = self._get_param("Tags")
        provisioned = self._get_param("Provisioned")
        serverless = self._get_param("Serverless")
        cluster = self.kafka_backend.create_cluster_v2(
            cluster_name=cluster_name,
            tags=tags,
            provisioned=provisioned,
            serverless=serverless,
        )
        result = {
            "ClusterArn": cluster.arn,
            "ClusterName": cluster.cluster_name,
            "State": cluster.state,
            "ClusterType": cluster.cluster_type,
        }
        return ActionResult(result)

    def describe_cluster_v2(self) -> ActionResult:
        cluster_arn = self._get_param("ClusterArn")
        cluster = self.kafka_backend.describe_cluster_v2(
            cluster_arn=cluster_arn,
        )
        return ActionResult({"ClusterInfo": self._cluster_v2_info(cluster)})

    def list_clusters_v2(self) -> ActionResult:
        cluster_name_filter = self._get_param("ClusterNameFilter")
        cluster_type_filter = self._get_param("ClusterTypeFilter")
        clusters = self.kafka_backend.list_clusters_v2(
            cluster_name_filter=cluster_name_filter,
            cluster_type_filter=cluster_type_filter,
        )
        cluster_info_list = [self._cluster_v2_info(cluster) for cluster in clusters]
        return ActionResult({"ClusterInfoList": cluster_info_list})

    def list_tags_for_resource(self) -> ActionResult:
        resource_arn = self._get_param("ResourceArn")
        tags = self.kafka_backend.list_tags_for_resource(
            resource_arn=resource_arn,
        )
        return ActionResult({"Tags": tags})

    def tag_resource(self) -> EmptyResult:
        resource_arn = self._get_param("ResourceArn")
        tags = self._get_param("Tags")
        self.kafka_backend.tag_resource(
            resource_arn=resource_arn,
            tags=tags,
        )
        return EmptyResult()

    def untag_resource(self) -> EmptyResult:
        resource_arn = self._get_param("ResourceArn")
        tag_keys = self._get_param("TagKeys")
        self.kafka_backend.untag_resource(
            resource_arn=resource_arn,
            tag_keys=tag_keys,
        )
        return EmptyResult()

    def create_cluster(self) -> ActionResult:
        broker_node_group_info = self._get_param("BrokerNodeGroupInfo")
        client_authentication = self._get_param("ClientAuthentication")
        cluster_name = self._get_param("ClusterName")
        configuration_info = self._get_param("ConfigurationInfo")
        encryption_info = self._get_param("EncryptionInfo")
        enhanced_monitoring = self._get_param("EnhancedMonitoring")
        open_monitoring = self._get_param("OpenMonitoring")
        kafka_version = self._get_param("KafkaVersion")
        logging_info = self._get_param("LoggingInfo")
        number_of_broker_nodes = self._get_param("NumberOfBrokerNodes")
        tags = self._get_param("Tags")
        storage_mode = self._get_param("StorageMode")
        cluster = self.kafka_backend.create_cluster(
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
        result = {
            "ClusterArn": cluster.arn,
            "ClusterName": cluster.cluster_name,
            "State": cluster.state,
        }
        return ActionResult(result)

    def describe_cluster(self) -> ActionResult:
        cluster_arn = self._get_param("ClusterArn")
        cluster = self.kafka_backend.describe_cluster(
            cluster_arn=cluster_arn,
        )
        configuration_info = cluster.configuration_info or {}
        cluster_info = {
            "ActiveOperationArn": "arn:aws:kafka:region:account-id:operation/active-operation",
            "BrokerNodeGroupInfo": cluster.broker_node_group_info or {},
            "ClientAuthentication": cluster.client_authentication or {},
            "ClusterArn": cluster.arn,
            "ClusterName": cluster.cluster_name,
            "CreationTime": cluster.creation_time,
            "CurrentBrokerSoftwareInfo": {
                "ConfigurationArn": configuration_info.get("Arn", "string"),
                "ConfigurationRevision": configuration_info.get("Revision", 1),
                "KafkaVersion": cluster.kafka_version,
            },
            "CurrentVersion": cluster.current_version,
            "EncryptionInfo": cluster.encryption_info or {},
            "EnhancedMonitoring": cluster.enhanced_monitoring,
            "OpenMonitoring": cluster.open_monitoring or {},
            "LoggingInfo": cluster.logging_info or {},
            "NumberOfBrokerNodes": cluster.number_of_broker_nodes or 0,
            "State": cluster.state,
            "StateInfo": {
                "Code": "string",
                "Message": "Cluster state details.",
            },
            "Tags": self.kafka_backend.list_tags_for_resource(cluster.arn),
            "ZookeeperConnectString": cluster.zookeeper_connect_string
            or "zookeeper.example.com:2181",
            "ZookeeperConnectStringTls": cluster.zookeeper_connect_string_tls
            or "zookeeper.example.com:2181",
            "StorageMode": cluster.storage_mode,
            "CustomerActionStatus": "NONE",
        }
        return ActionResult({"ClusterInfo": cluster_info})

    def delete_cluster(self) -> ActionResult:
        cluster_arn = self._get_param("ClusterArn")
        current_version = self._get_param("CurrentVersion")
        cluster = self.kafka_backend.delete_cluster(
            cluster_arn=cluster_arn,
            current_version=current_version,
        )
        return ActionResult({"ClusterArn": cluster.arn, "State": cluster.state})

    def put_cluster_policy(self) -> ActionResult:
        cluster_arn = self._get_param("ClusterArn")
        current_version = self._get_param("CurrentVersion")
        policy = self._get_param("Policy")

        new_version = self.kafka_backend.put_cluster_policy(
            cluster_arn=cluster_arn,
            current_version=current_version,
            policy=policy,
        )
        return ActionResult({"CurrentVersion": new_version})

    def get_cluster_policy(self) -> ActionResult:
        cluster_arn = self._get_param("ClusterArn")
        policy, current_version = self.kafka_backend.get_cluster_policy(
            cluster_arn=cluster_arn,
        )
        result = {
            "CurrentVersion": current_version,
            "Policy": policy,
        }
        return ActionResult(result)

    def delete_cluster_policy(self) -> EmptyResult:
        cluster_arn = self._get_param("ClusterArn")
        self.kafka_backend.delete_cluster_policy(cluster_arn=cluster_arn)
        return EmptyResult()

    def list_clusters(self) -> ActionResult:
        cluster_name_filter = self._get_param("ClusterNameFilter")
        clusters = self.kafka_backend.list_clusters(
            cluster_name_filter=cluster_name_filter,
        )
        cluster_info_list = [
            {
                "ClusterArn": cluster.arn,
                "ClusterName": cluster.cluster_name,
                "State": cluster.state,
                "CreationTime": cluster.creation_time,
            }
            for cluster in clusters
        ]
        return ActionResult({"ClusterInfoList": cluster_info_list})

    def _cluster_v2_info(self, cluster: FakeKafkaCluster) -> dict[str, Any]:
        cluster_info: dict[str, Any] = {
            "ActiveOperationArn": "arn:aws:kafka:region:account-id:operation/active-operation",
            "ClusterArn": cluster.arn,
            "ClusterName": cluster.cluster_name,
            "ClusterType": cluster.cluster_type,
            "CreationTime": cluster.creation_time,
            "CurrentVersion": cluster.current_version,
            "State": cluster.state,
            "StateInfo": {
                "Code": "string",
                "Message": "Cluster state details.",
            },
            "Tags": self.kafka_backend.list_tags_for_resource(cluster.arn),
        }

        if cluster.cluster_type == "PROVISIONED":
            configuration_info = cluster.configuration_info or {}
            cluster_info["Provisioned"] = {
                "BrokerNodeGroupInfo": cluster.broker_node_group_info or {},
                "ClientAuthentication": cluster.client_authentication or {},
                "CurrentBrokerSoftwareInfo": {
                    "ConfigurationArn": configuration_info.get("Arn", "string"),
                    "ConfigurationRevision": configuration_info.get("Revision", 1),
                    "KafkaVersion": cluster.kafka_version,
                },
                "EncryptionInfo": cluster.encryption_info or {},
                "EnhancedMonitoring": cluster.enhanced_monitoring,
                "OpenMonitoring": cluster.open_monitoring or {},
                "LoggingInfo": cluster.logging_info or {},
                "NumberOfBrokerNodes": cluster.number_of_broker_nodes or 0,
                "ZookeeperConnectString": cluster.zookeeper_connect_string
                or "zookeeper.example.com:2181",
                "ZookeeperConnectStringTls": cluster.zookeeper_connect_string_tls
                or "zookeeper.example.com:2181",
                "StorageMode": cluster.storage_mode,
                "CustomerActionStatus": "NONE",
            }
        elif cluster.cluster_type == "SERVERLESS":
            serverless_config = cluster.serverless_config or {}
            cluster_info["Serverless"] = {
                "VpcConfigs": serverless_config.get("VpcConfigs", []),
                "ClientAuthentication": serverless_config.get(
                    "ClientAuthentication", {}
                ),
            }

        return cluster_info
