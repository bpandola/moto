from typing import Any, List

from .models import (
    DBCluster,
    DBClusterSnapshot,
    DBInstance,
    DBProxy,
    DBSecurityGroup,
    DBSnapshot,
    DBSubnetGroup,
    GlobalCluster,
)

# TOTAL HACK
# This is mainly used to alias the top-level parameters passed from the
# RDS responses class to the Jinja template engine into something that
# can be picked up by the Serializer using the Service Model.
# For example: the create_db_instance method returns a `cluster` key
# while the serializer is looking for `DBCluster` or `db_cluster`
# These aliases are only necessary because we are still hacking the
# template render method.  Once we fix up the response methods to use
# the serializer, we can format the result dict as needed
SERIALIZATION_ALIASES = {
    "DBCluster": "cluster",
    "DBClusterParameterGroups": "db_parameter_groups",
    "DBClusters": "clusters",
    "DBClusterSnapshot": "snapshot",
    "DBClusterSnapshotArn": "snapshot_arn",
    "DBClusterSnapshotIdentifier": "snapshot_id",
    "DBClusterSnapshots": "snapshots",
    "DBInstance": "database",
    "DBInstances": "databases",
    "DBProxies": "dbproxies",
    "DBProxy": "dbproxy",
    "DBSecurityGroup": "security_group",
    "DBSecurityGroupName": "group_name",
    "DBSecurityGroups": "security_groups",
    "DBSnapshot": "snapshot",
    "DBSnapshotArn": "snapshot_arn",
    "DBSnapshotIdentifier": "snapshot_id",
    "DBSnapshots": "snapshots",
    "DBSubnetGroup": "subnet_group",
    "DBSubnetGroupName": "subnet_name",
    "DBSubnetGroups": "subnet_groups",
    "EventCategoriesList": "event_categories",
    "EventSubscription": "subscription",
    "EventSubscriptionsList": "subscriptions",
    "ExportTask": "task",
    "ExportTasks": "tasks",
    "GlobalCluster": "cluster",
    "GlobalClusters": "clusters",
    "OptionGroupsList": "option_groups",
    "Parameters": "db_parameter_group",  # Super fail in describe_db_cluster_parameters call
    "ReadReplicaSourceDBInstanceIdentifier": "source_db_identifier",
    "SourceIdsList": "source_ids",
    "TagList": "tags",
    # Also works to alias AWS model attributes to Moto RDS model attributes
    # "DBSecurityGroupDescription": "description",
    # "EventSubscriptionArn": "es_arn",
    # "DBSubnetGroupDescription": "description",
    # "DBParameterGroupName": "name",
    "DBParameterGroupFamily": "family",
    "CustSubscriptionId": "subscription_name",
    "S3Bucket": "s3_bucket_name",
    "EC2SecurityGroupId": "id",
    "EC2SecurityGroupName": "name",
    "EC2SubnetGroupOwnerId": "owner_id",
    "IAMDatabaseAuthenticationEnabled": [
        "enable_iam_database_authentication",
        "iam_auth",
    ],
    # "DBInstanceStatus": "status",
    "DbInstancePort": "port",
    "MultiAZ": "is_multi_az",
    "HttpEndpointEnabled": "enable_http_endpoint",
    "DatabaseName": "db_name",
    "DbClusterResourceId": "resource_id",
    # second one is for neptune
    "DBClusterParameterGroup": ["parameter_group", "db_cluster_parameter_group_name"],
    # "DBProxyArn": "arn",
    # Alias our DTO classes so DBInstanceDTO can still be DBInstance..,
    "DBSubnetGroupDTO": "DBSubnetGroup",
    "DBParameterGroupDTO": "DBParameterGroup",
    "DBInstanceDTO": "DBInstance",
    "DBSecurityGroupDTO": "DBSecurityGroup",
    "DBProxyDTO": "DBProxy",
    # neptune
    "OrderableDBInstanceOptions": "options",
}


class DBProxyDTO:
    def __init__(self, proxy: DBProxy):
        self.proxy = proxy

    @property
    def vpc_security_group_ids(self) -> List[str]:
        # TODO: this is not initialized properly in the DBProxy class...
        ids = self.proxy.vpc_security_group_ids
        if ids is None:
            ids = []
        return ids

    def __getattribute__(self, name: str) -> Any:
        try:
            return super().__getattribute__(name)
        except AttributeError:
            pass
        return self.proxy.__getattribute__(name)


class DBSubnetGroupDTO:
    def __init__(self, subnet_group: DBSubnetGroup):
        self.subnet_group = subnet_group

    @property
    def subnets(self) -> list[dict]:
        subnets = [
            {
                "SubnetStatus": "Active",
                "SubnetIdentifier": subnet.id,
                "SubnetAvailabilityZone": {
                    "Name": subnet.availability_zone,
                    "ProvisionedIopsCapable": False,
                },
            }
            for subnet in self.subnet_group.subnets
        ]
        return subnets

    def __getattribute__(self, name: str) -> Any:
        try:
            return super().__getattribute__(name)
        except AttributeError:
            pass
        return self.subnet_group.__getattribute__(name)


class DBSecurityGroupDTO:
    def __init__(self, security_group: DBSecurityGroup):
        self.security_group = security_group

    @property
    def ip_ranges(self) -> List[dict]:
        ranges = [
            {
                "CIDRIP": ip_range,
                "Status": "authorized",
            }
            for ip_range in self.security_group.ip_ranges
        ]
        return ranges

    def __getattribute__(self, name: str) -> Any:
        try:
            return super().__getattribute__(name)
        except AttributeError:
            pass
        return self.security_group.__getattribute__(name)


class DBInstanceDTO:
    def __init__(self, instance: DBInstance):
        self.instance = instance

    @property
    def vpc_security_groups(self) -> list[dict]:
        groups = [
            {
                "Status": "active",
                "VpcSecurityGroupId": id_,
            }
            for id_ in self.vpc_security_group_ids
        ]
        return groups

    @property
    def db_parameter_groups(self) -> Any:
        # this is hideous
        groups = self.instance.db_parameter_groups()
        for group in groups:
            setattr(group, "ParameterApplyStatus", "in-sync")
        return groups

    @property
    def db_security_groups(self) -> list[dict]:
        groups = [
            {
                "Status": "active",
                "DBSecurityGroupName": group,
            }
            for group in self.security_groups
        ]
        return groups

    @property
    def endpoint(self) -> dict:
        return {
            "Address": self.address,
            "Port": self.port,
        }

    @property
    def option_group_memberships(self) -> List[dict]:
        groups = [
            {
                "OptionGroupName": self.option_group_name,
                "Status": "in-sync",
            }
        ]
        return groups

    @property
    def read_replica_db_instance_identifiers(self) -> list[str]:
        return [replica for replica in self.replicas]

    def __getattribute__(self, name: str) -> Any:
        try:
            return super().__getattribute__(name)
        except AttributeError:
            pass
        return self.instance.__getattribute__(name)


class DBSnapshotDTO:
    def __init__(self, snapshot: DBSnapshot):
        self.snapshot = snapshot
        self.instance = DBInstanceDTO(snapshot.database)

    def __getattribute__(self, name: str) -> Any:
        try:
            return super().__getattribute__(name)
        except AttributeError:
            pass
        try:
            return self.snapshot.__getattribute__(name)
        except AttributeError:
            pass
        return self.instance.__getattribute__(name)


class GlobalClusterDTO:
    def __init__(self, cluster: GlobalCluster):
        self.cluster = cluster

    @property
    def status(self) -> str:
        return "available"  # this is hardcoded in GlobalCluster.to_xml

    @property
    def global_cluster_members(self):
        readers = [
            reader.db_cluster_arn
            for reader in self.cluster.members
            if not reader.is_writer
        ]
        members = [
            {
                "DBClusterArn": member.db_cluster_arn,
                "IsWriter": True if member.is_writer else False,
                "DBClusterParameterGroupStatus": "in-sync",
                "PromotionTier": 1,
                # I don't think this is correct, but current test assert on it being empty for non writers
                "Readers": [],
            }
            for member in self.members
        ]
        for member in members:
            if member["IsWriter"]:
                member["Readers"] = readers
            else:
                member["GlobalWriteForwardingStatus"] = "disabled"
        return members

    def __getattribute__(self, name: str) -> Any:
        try:
            return super().__getattribute__(name)
        except AttributeError:
            return self.cluster.__getattribute__(name)


class DBClusterDTO:
    def __init__(self, cluster: DBCluster, creating: bool = False):
        self.cluster = cluster
        self.creating = creating

    @property
    def status(self) -> str:
        return "creating" if self.creating else self.cluster.status

    @property
    def associated_roles(self) -> List[dict]:
        return []

    @property
    def scaling_configuration_info(self) -> dict:
        info = {
            "MinCapacity": self.cluster.scaling_configuration.get("min_capacity"),
            "MaxCapacity": self.cluster.scaling_configuration.get("max_capacity"),
            "AutoPause": self.cluster.scaling_configuration.get("auto_pause"),
            "SecondsUntilAutoPause": self.cluster.scaling_configuration.get(
                "seconds_until_auto_pause"
            ),
            "TimeoutAction": self.cluster.scaling_configuration.get("timeout_action"),
            "SecondsBeforeTimeout": self.cluster.scaling_configuration.get(
                "seconds_before_timeout"
            ),
        }
        return info

    @property
    def vpc_security_groups(self) -> List[dict]:
        groups = [
            {"VpcSecurityGroupId": sg_id, "Status": "active"}
            for sg_id in self.cluster.vpc_security_group_ids
        ]
        return groups

    @property
    def domain_memberships(self) -> list:
        return []

    @property
    def cross_account_clone(self) -> bool:
        return False

    @property
    def global_write_forwarding_requested(self):
        # This does not appear to be in the standard response for any clusters
        # Docs say it's only for a secondary cluster in aurora global database...
        return True if self.cluster.global_write_forwarding_requested else False

    @property
    def db_cluster_members(self):
        members = [
            {
                "DBInstanceIdentifier": member,
                "IsClusterWriter": True,
                "DBClusterParameterGroupStatus": "in-sync",
                "PromotionTier": 1,
            }
            for member in self.cluster_members
        ]
        return members

    def __getattribute__(self, name: str) -> Any:
        try:
            return super().__getattribute__(name)
        except AttributeError:
            return self.cluster.__getattribute__(name)


class DBClusterSnapshotDTO:
    def __init__(self, snapshot: DBClusterSnapshot):
        self.snapshot = snapshot
        self.cluster = DBClusterDTO(snapshot.cluster)

    def __getattribute__(self, name: str) -> Any:
        try:
            return super().__getattribute__(name)
        except AttributeError:
            pass
        try:
            return self.snapshot.__getattribute__(name)
        except AttributeError:
            pass
        return self.cluster.__getattribute__(name)


MODEL_TO_VIEW = {
    DBInstance: DBInstanceDTO,
    DBSnapshot: DBSnapshotDTO,
    DBCluster: DBClusterDTO,
    DBClusterSnapshot: DBClusterSnapshotDTO,
    GlobalCluster: GlobalClusterDTO,
    DBSecurityGroup: DBSecurityGroupDTO,
    DBSubnetGroup: DBSubnetGroupDTO,
    DBProxy: DBProxyDTO,
}


def transform_view_args(operation, **kwargs):
    transformed = {}
    extra_args = {}
    # HACKY BULLSHIT TO FIX MOTO SHORTCOMING WITHOUT TOUCHING MOTO CODE
    if operation == "CreateDBCluster":
        extra_args["creating"] = True
    elif operation == "ModifyDBParameterGroup":
        # Hack for this one-off...
        group = kwargs.pop("db_parameter_group")
        kwargs["DBParameterGroupName"] = group.name
    elif operation == "DescribeDBParameters":
        group = kwargs.pop("db_parameter_group")
        kwargs["Parameters"] = group.parameters.values()
    elif operation in ["StartExportTask", "CancelExportTask"]:
        # SUPER HACK! Response expects object without kwargs dict wrapper
        task = kwargs.pop("task")
        return task
    elif operation in ["DescribeDBSnapshotAttributes", "ModifyDBSnapshotAttribute"]:
        new_kwargs = dict(
            db_snapshot_attributes_result=dict(
                db_snapshot_identifier=kwargs.pop("db_snapshot_identifier"),
                db_snapshot_attributes=kwargs.pop("db_snapshot_attributes_result"),
            )
        )
        kwargs.update(new_kwargs)
    elif operation in [
        "DescribeDBClusterSnapshotAttributes",
        "ModifyDBClusterSnapshotAttribute",
    ]:
        new_kwargs = dict(
            db_cluster_snapshot_attributes_result=dict(
                db_cluster_snapshot_identifier=kwargs.pop(
                    "db_cluster_snapshot_identifier"
                ),
                db_cluster_snapshot_attributes=kwargs.pop(
                    "db_cluster_snapshot_attributes_result"
                ),
            )
        )
        kwargs.update(new_kwargs)
    for k, v in kwargs.items():
        is_list = isinstance(v, list)
        if is_list:
            if len(v):
                entity = v[0].__class__
            else:
                entity = None
        else:
            entity = v.__class__
        if entity in MODEL_TO_VIEW:
            _class = MODEL_TO_VIEW[entity]
            if is_list:
                transformed[k] = [_class(r, **extra_args) for r in v]
            else:
                transformed[k] = _class(v, **extra_args)
        else:
            transformed[k] = v
    return transformed
