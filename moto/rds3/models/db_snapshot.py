import copy
import datetime
import string
import os

from collections import OrderedDict, defaultdict
from moto.core.utils import iso_8601_datetime_with_milliseconds
from .base import BaseRDSModel
from .event import EventMixin
from .tag import TaggableRDSResource
from ..exceptions import (
    DBSnapshotAlreadyExists,
    DBSnapshotNotFound,
    InvalidDBSnapshotIdentifierValue,
    SnapshotQuotaExceeded,
    InvalidParameterValue,
    InvalidParameterCombination,
    SharedSnapshotQuotaExceeded,
    KMSKeyNotAccessibleFault,
)


class DBSnapshot(TaggableRDSResource, EventMixin, BaseRDSModel):

    resource_type = "snapshot"
    event_source_type = "db-snapshot"

    @staticmethod
    def _is_identifier_valid(db_snapshot_identifier):
        """
        :param db_snapshot_identifier:

        Constraints:

        Cannot be null, empty, or blank
        Must contain from 1 to 255 letters, numbers, or hyphens
        First character must be a letter
        Cannot end with a hyphen or contain two consecutive hyphens
        Example: my-snapshot-id

        :return:
        """
        is_valid = True
        if db_snapshot_identifier is None or db_snapshot_identifier == "":
            is_valid = False
        if len(db_snapshot_identifier) < 1 or len(db_snapshot_identifier) > 255:
            is_valid = False
        if not db_snapshot_identifier[0].isalpha():
            is_valid = False
        if db_snapshot_identifier[-1] == "-":
            is_valid = False
        if db_snapshot_identifier.find("--") != -1:
            is_valid = False
        valid_chars = "".join([string.digits, string.ascii_letters, "-"])
        if not all(char in valid_chars for char in db_snapshot_identifier):
            is_valid = False
        return is_valid

    def __init__(
        self,
        backend,
        identifier,
        db_instance,
        snapshot_type="manual",
        tags=None,
        kms_key_id=None,
    ):
        super().__init__(backend)
        if snapshot_type == "manual":
            if not self._is_identifier_valid(identifier):
                raise InvalidDBSnapshotIdentifierValue(identifier)
        self.db_snapshot_identifier = identifier
        self.snapshot_type = snapshot_type
        self.status = "available"
        self.created_at = iso_8601_datetime_with_milliseconds(datetime.datetime.now())
        # If tags are provided at creation, AWS does *not* copy tags from the
        # db_instance (even if copy_tags_to_snapshot is True).
        # https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_Tagging.html
        if tags:
            self.add_tags(tags)
        elif db_instance.copy_tags_to_snapshot:
            self.add_tags(db_instance.tags)

        self.db_instance = copy.copy(db_instance)
        self.db_instance_identifier = self.db_instance.resource_id
        self.engine = self.db_instance.engine
        self.allocated_storage = self.db_instance.allocated_storage
        if kms_key_id is not None:
            self.kms_key_id = self.db_instance.kms_key_id = kms_key_id
            self.encrypted = self.db_instance.storage_encrypted = True
        else:
            self.kms_key_id = self.db_instance.kms_key_id
            self.encrypted = self.db_instance.storage_encrypted
        self.port = self.db_instance.port
        self.availability_zone = self.db_instance.availability_zone
        self.engine_version = self.db_instance.engine_version
        self.master_username = self.db_instance.master_username
        self.storage_type = self.db_instance.storage_type
        self.iops = self.db_instance.iops
        self.snapshot_attributes = DBSnapshotAttributeResult(
            db_snapshot_identifier=self.db_snapshot_identifier
        )

    @property
    def resource_id(self):
        return self.db_snapshot_identifier

    @property
    def db_snapshot_arn(self):
        return self.arn

    @property
    def snapshot_create_time(self):
        return self.created_at


class DBInstanceAutomatedBackup:
    def __init__(self, backend, db_instance_identifier, automated_snapshots):
        self.backend = backend
        self.db_instance_identifier = db_instance_identifier
        self.automated_snapshots = automated_snapshots

    @property
    def resource_id(self):
        return self.db_instance_identifier

    @property
    def status(self):
        status = "active"
        if self.db_instance_identifier not in self.backend.db_instances:
            status = "retained"
        return status


class DBSnapshotAttribute:
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
        if attribute_name not in DBSnapshotAttribute.ALLOWED_ATTRIBUTE_NAMES:
            raise InvalidParameterValue(f"Invalid snapshot attribute {attribute_name}")
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
        return DBSnapshotAttribute(attribute_name, attribute_values)


class DBSnapshotAttributeResult:
    def __init__(self, db_snapshot_identifier):
        self.db_snapshot_identifier = db_snapshot_identifier
        self.db_snapshot_attributes = [
            DBSnapshotAttribute(attribute_name="restore", attribute_values=[])
        ]


class DBSnapshotBackend:
    def __init__(self):
        self.db_snapshots = OrderedDict()

    def get_db_snapshot(self, db_snapshot_identifier):
        if db_snapshot_identifier not in self.db_snapshots:
            raise DBSnapshotNotFound(db_snapshot_identifier)
        return self.db_snapshots[db_snapshot_identifier]

    def copy_db_snapshot(
        self,
        source_db_snapshot_identifier,
        target_db_snapshot_identifier,
        kms_key_id=None,
        tags=None,
        copy_tags=False,
    ):
        if target_db_snapshot_identifier in self.db_snapshots:
            raise DBSnapshotAlreadyExists(target_db_snapshot_identifier)
        if len(self.db_snapshots) >= int(
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
        source_snapshot = self.get_db_snapshot(source_db_snapshot_identifier)
        # If tags are present, copy_tags is ignored
        if copy_tags and not tags:
            tags = source_snapshot.tags
        target_snapshot = DBSnapshot(
            self,
            target_db_snapshot_identifier,
            source_snapshot.db_instance,
            tags=tags,
            kms_key_id=kms_key_id,
        )
        self.db_snapshots[target_db_snapshot_identifier] = target_snapshot
        return target_snapshot

    def create_db_snapshot(
        self,
        db_instance_identifier,
        db_snapshot_identifier,
        tags=None,
        snapshot_type="manual",
    ):
        if db_snapshot_identifier in self.db_snapshots:
            raise DBSnapshotAlreadyExists(db_snapshot_identifier)
        if len(self.db_snapshots) >= int(
            os.environ.get("MOTO_RDS_SNAPSHOT_LIMIT", "100")
        ):
            raise SnapshotQuotaExceeded()
        db_instance = self.get_db_instance(db_instance_identifier)
        snapshot = DBSnapshot(
            self, db_snapshot_identifier, db_instance, snapshot_type, tags
        )
        snapshot.add_event(f"DB_SNAPSHOT_CREATE_{snapshot_type}_START".upper())
        snapshot.add_event(f"DB_SNAPSHOT_CREATE_{snapshot_type}_FINISH".upper())
        self.db_snapshots[db_snapshot_identifier] = snapshot
        return snapshot

    def delete_db_snapshot(self, db_snapshot_identifier):
        snapshot = self.get_db_snapshot(db_snapshot_identifier)
        snapshot.delete_events()
        return self.db_snapshots.pop(db_snapshot_identifier)

    def describe_db_snapshots(
        self,
        db_instance_identifier=None,
        db_snapshot_identifier=None,
        snapshot_type=None,
        **_,
    ):
        if db_snapshot_identifier:
            return [self.get_db_snapshot(db_snapshot_identifier)]
        snapshot_types = (
            ["automated", "manual"] if snapshot_type is None else [snapshot_type]
        )
        if db_instance_identifier:
            db_instance_snapshots = []
            for snapshot in self.db_snapshots.values():
                if snapshot.db_instance_identifier == db_instance_identifier:
                    if snapshot.snapshot_type in snapshot_types:
                        db_instance_snapshots.append(snapshot)
            return db_instance_snapshots
        return self.db_snapshots.values()

    def describe_db_instance_automated_backups(
        self,
        db_instance_identifier=None,
        **_,
    ):
        snapshots = self.db_snapshots.values()
        if db_instance_identifier is not None:
            snapshots = [
                snap
                for snap in self.db_snapshots.values()
                if snap.db_instance_identifier == db_instance_identifier
            ]
        snapshots_grouped = defaultdict(list)
        for snapshot in snapshots:
            if snapshot.snapshot_type == "automated":
                snapshots_grouped[snapshot.db_instance_identifier].append(snapshot)
        return [
            DBInstanceAutomatedBackup(self, k, v) for k, v in snapshots_grouped.items()
        ]

    def describe_db_snapshot_attributes(
        self,
        db_snapshot_identifier,
    ):
        snapshot = self.get_db_snapshot(db_snapshot_identifier)
        return snapshot.snapshot_attributes

    def modify_db_snapshot_attribute(
        self,
        db_snapshot_identifier,
        attribute_name,
        values_to_add=None,
        values_to_remove=None,
    ):
        snapshot = self.get_db_snapshot(db_snapshot_identifier)
        if snapshot.snapshot_type != "manual":
            raise InvalidDBSnapshotIdentifierValue(db_snapshot_identifier)
        attribute_values = snapshot.snapshot_attributes.db_snapshot_attributes[
            0
        ].attribute_values
        updated_attribute_values = DBSnapshotAttribute.build_from_values(
            attribute_name=attribute_name,
            attribute_values=attribute_values,
            values_to_add=values_to_add,
            values_to_remove=values_to_remove,
        )
        snapshot.snapshot_attributes.db_snapshot_attributes[
            0
        ] = updated_attribute_values
        return snapshot.snapshot_attributes