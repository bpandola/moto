import copy
import os

from collections import OrderedDict
from .base import BaseRDSModel
from .tag import TaggableRDSResource
from ..exceptions import (
    DBClusterSnapshotAlreadyExists,
    DBClusterSnapshotNotFound,
    InvalidParameterValue,
    InvalidParameterCombination,
    SharedSnapshotQuotaExceeded,
    SnapshotQuotaExceeded,
    InvalidDBClusterSnapshotStateFault,
    KMSKeyNotAccessibleFault,
)


class DBClusterSnapshot(TaggableRDSResource, BaseRDSModel):

    resource_type = "cluster-snapshot"

    def __init__(
        self,
        backend,
        identifier,
        db_cluster,
        snapshot_type="manual",
        tags=None,
        kms_key_id=None,
    ):
        super().__init__(backend)
        self.db_cluster_snapshot_identifier = identifier
        self.snapshot_type = snapshot_type
        self.percent_progress = 100
        self.status = "available"
        # If tags are provided at creation, AWS does *not* copy tags from the
        # db_cluster (even if copy_tags_to_snapshot is True).
        if tags:
            self.add_tags(tags)
        elif db_cluster.copy_tags_to_snapshot:
            self.add_tags(db_cluster.tags)
        self.cluster = copy.copy(db_cluster)
        self.allocated_storage = self.cluster.allocated_storage
        self.cluster_create_time = self.cluster.created
        self.db_cluster_identifier = self.cluster.resource_id
        if kms_key_id is not None:
            self.kms_key_id = self.cluster.kms_key_id = kms_key_id
            self.encrypted = self.cluster.storage_encrypted = True
        else:
            self.kms_key_id = self.cluster.kms_key_id
            self.encrypted = self.cluster.storage_encrypted
        self.encrypted = self.cluster.storage_encrypted
        self.engine = self.cluster.engine
        self.engine_version = self.cluster.engine_version
        self.master_username = self.cluster.master_username
        self.port = self.cluster.port
        self.storage_encrypted = self.cluster.storage_encrypted
        self.snapshot_attributes = DBClusterSnapshotAttributeResult(
            db_cluster_snapshot_identifier=self.db_cluster_snapshot_identifier
        )

    @property
    def resource_id(self):
        return self.db_cluster_snapshot_identifier

    @property
    def db_cluster_snapshot_arn(self):
        return self.arn

    @property
    def snapshot_create_time(self):
        return self.created


class DBClusterSnapshotAttribute:
    ALLOWED_ATTRIBUTE_NAMES = ["restore"]

    def __init__(self, attribute_name, attribute_values):
        self.attribute_name = attribute_name
        self.attribute_values = attribute_values

    @staticmethod
    def build_from_values(
        attribute_name, attribute_values, values_to_add, values_to_remove
    ):
        if not values_to_add:
            values_to_add = []
        if not values_to_remove:
            values_to_remove = []
        if attribute_name not in DBClusterSnapshotAttribute.ALLOWED_ATTRIBUTE_NAMES:
            raise InvalidParameterValue(
                f"Invalid cluster snapshot attribute {attribute_name}"
            )
        common_values = set(values_to_add).intersection(values_to_remove)
        if common_values:
            raise InvalidParameterCombination(
                "A customer Id may not appear in both the add list and remove list. "
                + f"{common_values}"
            )
        add = attribute_values + values_to_add
        attribute_values = [value for value in add if value not in values_to_remove]
        if len(attribute_values) > os.getenv("MAX_SHARED_ACCOUNTS", 20):
            raise SharedSnapshotQuotaExceeded()
        return DBClusterSnapshotAttribute(attribute_name, attribute_values)


class DBClusterSnapshotAttributeResult:
    def __init__(self, db_cluster_snapshot_identifier):
        self.db_cluster_snapshot_identifier = db_cluster_snapshot_identifier
        self.db_cluster_snapshot_attributes = [
            DBClusterSnapshotAttribute(attribute_name="restore", attribute_values=[])
        ]


class DBClusterSnapshotBackend:
    def __init__(self):
        self.db_cluster_snapshots = OrderedDict()

    def get_db_cluster_snapshot(self, db_cluster_snapshot_identifier):
        if db_cluster_snapshot_identifier in self.db_cluster_snapshots:
            return self.db_cluster_snapshots[db_cluster_snapshot_identifier]
        raise DBClusterSnapshotNotFound(db_cluster_snapshot_identifier)

    def create_db_cluster_snapshot(
        self,
        db_cluster_identifier,
        db_cluster_snapshot_identifier,
        tags=None,
        snapshot_type="manual",
    ):
        if db_cluster_snapshot_identifier in self.db_cluster_snapshots:
            raise DBClusterSnapshotAlreadyExists()
        db_cluster = self.get_db_cluster(db_cluster_identifier)
        snapshot = DBClusterSnapshot(
            self, db_cluster_snapshot_identifier, db_cluster, snapshot_type, tags
        )
        self.db_cluster_snapshots[db_cluster_snapshot_identifier] = snapshot
        return snapshot

    def copy_db_cluster_snapshot(
        self,
        source_db_cluster_snapshot_identifier,
        target_db_cluster_snapshot_identifier,
        kms_key_id=None,
        tags=None,
        copy_tags=False,
    ):
        if source_db_cluster_snapshot_identifier not in self.db_cluster_snapshots:
            raise DBClusterSnapshotNotFound(source_db_cluster_snapshot_identifier)
        if target_db_cluster_snapshot_identifier in self.db_cluster_snapshots:
            raise DBClusterSnapshotAlreadyExists(target_db_cluster_snapshot_identifier)

        if len(self.db_cluster_snapshots) >= int(
            os.environ.get("MOTO_RDS_SNAPSHOT_LIMIT", "100")
        ):
            raise SnapshotQuotaExceeded()
        try:
            if kms_key_id is not None:
                key = self.kms.describe_key(str(kms_key_id))
                # We do this in case an alias was passed in.
                kms_key_id = key.id
        except Exception:
            raise KMSKeyNotAccessibleFault(str(kms_key_id))
        source_db_cluster_snapshot = self.get_db_cluster_snapshot(
            source_db_cluster_snapshot_identifier
        )
        # If tags are present, copy_tags is ignored
        if copy_tags and not tags:
            tags = source_db_cluster_snapshot.tags
        target_db_cluster_snapshot = DBClusterSnapshot(
            self,
            target_db_cluster_snapshot_identifier,
            source_db_cluster_snapshot.cluster,
            tags=tags,
            kms_key_id=kms_key_id,
        )
        self.db_cluster_snapshots[
            target_db_cluster_snapshot_identifier
        ] = target_db_cluster_snapshot
        return target_db_cluster_snapshot

    def delete_db_cluster_snapshot(self, db_cluster_snapshot_identifier):
        snapshot = self.get_db_cluster_snapshot(db_cluster_snapshot_identifier)
        return self.db_cluster_snapshots.pop(snapshot.resource_id)

    def describe_db_cluster_snapshots(
        self,
        db_cluster_identifier=None,
        db_cluster_snapshot_identifier=None,
        snapshot_type=None,
        **_,
    ):
        if db_cluster_snapshot_identifier:
            return [self.get_db_cluster_snapshot(db_cluster_snapshot_identifier)]
        snapshot_types = (
            ["automated", "manual"] if snapshot_type is None else [snapshot_type]
        )
        if db_cluster_identifier:
            db_cluster_snapshots = []
            for snapshot in self.db_cluster_snapshots.values():
                if snapshot.db_cluster_identifier == db_cluster_identifier:
                    if snapshot.snapshot_type in snapshot_types:
                        db_cluster_snapshots.append(snapshot)
            return db_cluster_snapshots
        return self.db_cluster_snapshots.values()

    def describe_db_cluster_snapshot_attributes(
        self,
        db_cluster_snapshot_identifier,
    ):
        cluster_snapshot = self.get_db_cluster_snapshot(db_cluster_snapshot_identifier)
        return cluster_snapshot.snapshot_attributes

    def modify_db_cluster_snapshot_attribute(
        self,
        db_cluster_snapshot_identifier,
        attribute_name,
        values_to_add=None,
        values_to_remove=None,
    ):
        cluster_snapshot = self.get_db_cluster_snapshot(db_cluster_snapshot_identifier)
        if cluster_snapshot.snapshot_type != "manual":
            raise InvalidDBClusterSnapshotStateFault()
        attribute_values = (
            cluster_snapshot.snapshot_attributes.db_cluster_snapshot_attributes[
                0
            ].attribute_values
        )
        updated_attribute_values = DBClusterSnapshotAttribute.build_from_values(
            attribute_name=attribute_name,
            attribute_values=attribute_values,
            values_to_add=values_to_add,
            values_to_remove=values_to_remove,
        )
        cluster_snapshot.snapshot_attributes.db_cluster_snapshot_attributes[
            0
        ] = updated_attribute_values
        return cluster_snapshot.snapshot_attributes