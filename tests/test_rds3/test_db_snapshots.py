from botocore.exceptions import ClientError
import boto3
import pytest
from sure import this  # noqa
from . import mock_rds


test_tags = [
    {
        "Key": "foo",
        "Value": "bar",
    },
    {
        "Key": "foo1",
        "Value": "bar1",
    },
]


@mock_rds
def test_create_db_snapshot():
    conn = boto3.client("rds", region_name="us-west-2")
    conn.create_db_snapshot.when.called_with(
        DBInstanceIdentifier="db-primary-1", DBSnapshotIdentifier="snapshot-1"
    ).should.throw(ClientError)

    conn.create_db_instance(
        DBInstanceIdentifier="db-primary-1",
        AllocatedStorage=10,
        Engine="postgres",
        DBName="staging-postgres",
        DBInstanceClass="db.m1.small",
        MasterUsername="root",
        MasterUserPassword="hunter2",
        Port=1234,
        DBSecurityGroups=["my_sg"],
        StorageEncrypted=True,
    )

    snapshot = conn.create_db_snapshot(
        DBInstanceIdentifier="db-primary-1", DBSnapshotIdentifier="g-1"
    ).get("DBSnapshot")

    snapshot.get("Engine").should.equal("postgres")
    snapshot.get("DBInstanceIdentifier").should.equal("db-primary-1")
    snapshot.get("DBSnapshotIdentifier").should.equal("g-1")


@mock_rds
def test_describe_db_snapshots_paginated():
    client = boto3.client("rds", region_name="us-west-2")
    client.create_db_instance(
        DBInstanceIdentifier="instance-1",
        DBName="db_name",
        DBInstanceClass="db.m1.small",
        Engine="postgres",
        MasterUsername="root",
        MasterUserPassword="password",
        Port=1234,
        Tags=test_tags,
    )
    auto_snapshots = client.describe_db_snapshots(MaxRecords=20).get("DBSnapshots")
    custom_snap_start = len(auto_snapshots)
    for i in range(custom_snap_start, 21):
        client.create_db_snapshot(
            DBSnapshotIdentifier=f"instance-snap-{i}",
            DBInstanceIdentifier="instance-1",
        )

    resp = client.describe_db_snapshots(MaxRecords=20)
    snaps = resp.get("DBSnapshots")
    snaps.should.have.length_of(20)
    snaps[custom_snap_start]["DBSnapshotIdentifier"].should.equal(
        f"instance-snap-{custom_snap_start}"
    )

    resp2 = client.describe_db_snapshots(Marker=resp["Marker"])
    resp2["DBSnapshots"].should.have.length_of(1)
    resp2["DBSnapshots"][0]["DBSnapshotIdentifier"].should.equal("instance-snap-20")

    resp3 = client.describe_db_snapshots()
    resp3["DBSnapshots"].should.have.length_of(21)


@mock_rds
def test_copy_unencrypted_db_snapshot_to_encrypted_db_snapshot():
    client = boto3.client("rds", region_name="us-west-2")
    client.create_db_instance(
        DBInstanceIdentifier="unencrypted-db-instance",
        AllocatedStorage=10,
        Engine="postgres",
        DBName="staging-postgres",
        DBInstanceClass="db.m1.small",
        MasterUsername="root",
        MasterUserPassword="pass",
        StorageEncrypted=False,
    )
    snapshot = client.create_db_snapshot(
        DBInstanceIdentifier="unencrypted-db-instance",
        DBSnapshotIdentifier="unencrypted-db-snapshot",
    ).get("DBSnapshot")
    snapshot["Encrypted"].should.equal(False)

    client.copy_db_snapshot(
        SourceDBSnapshotIdentifier="unencrypted-db-snapshot",
        TargetDBSnapshotIdentifier="encrypted-db-snapshot",
        KmsKeyId="alias/aws/rds",
    )
    snapshot = client.describe_db_snapshots(
        DBSnapshotIdentifier="encrypted-db-snapshot"
    ).get("DBSnapshots")[0]
    snapshot["DBSnapshotIdentifier"].should.equal("encrypted-db-snapshot")
    snapshot["Encrypted"].should.equal(True)


@mock_rds
def test_db_snapshot_events():
    client = boto3.client("rds", region_name="us-west-2")
    client.create_db_instance(
        DBInstanceIdentifier="test-instance",
        AllocatedStorage=10,
        Engine="postgres",
        DBName="staging-postgres",
        DBInstanceClass="db.m1.small",
        MasterUsername="root",
        MasterUserPassword="pass",
        StorageEncrypted=False,
    )
    # Automated snapshots
    snapshot = client.describe_db_snapshots(DBInstanceIdentifier="test-instance").get(
        "DBSnapshots"
    )[0]
    events = client.describe_events(
        SourceIdentifier=snapshot["DBSnapshotIdentifier"], SourceType="db-snapshot"
    ).get("Events")
    this(len(events)).should.be.greater_than(0)
    # Manual snapshot events
    client.create_db_snapshot(
        DBInstanceIdentifier="test-instance", DBSnapshotIdentifier="test-snapshot"
    )
    events = client.describe_events(
        SourceIdentifier="test-snapshot", SourceType="db-snapshot"
    ).get("Events")
    this(len(events)).should.be.greater_than(0)


@mock_rds
def test_create_db_snapshot_with_invalid_identifier_fails():
    client = boto3.client("rds", region_name="us-west-2")
    client.create_db_instance(
        DBInstanceIdentifier="db-primary-1",
        AllocatedStorage=10,
        Engine="postgres",
        DBName="staging-postgres",
        DBInstanceClass="db.m1.small",
        MasterUsername="root",
        MasterUserPassword="pass",
    )
    client.create_db_snapshot.when.called_with(
        DBInstanceIdentifier="db-primary-1", DBSnapshotIdentifier="rds:snapshot-1"
    ).should.throw(ClientError, "not a valid identifier")


@mock_rds
def test_describe_db_instance_automated_backups_lifecycle():
    instance_id = "test-instance"
    client = boto3.client("rds", region_name="us-west-2")
    client.create_db_instance(
        DBInstanceIdentifier=instance_id,
        AllocatedStorage=10,
        Engine="postgres",
        DBName="staging-postgres",
        DBInstanceClass="db.m1.small",
        MasterUsername="root",
        MasterUserPassword="pass",
        StorageEncrypted=False,
    )
    resp = client.describe_db_instance_automated_backups(
        DBInstanceIdentifier=instance_id,
    )
    automated_backups = resp["DBInstanceAutomatedBackups"]
    assert len(automated_backups) == 1
    automated_backup = automated_backups[0]
    assert automated_backup["DBInstanceIdentifier"] == instance_id
    assert automated_backup["Status"] == "active"

    client.delete_db_instance(
        DBInstanceIdentifier=instance_id,
        DeleteAutomatedBackups=False,
    )

    resp = client.describe_db_instance_automated_backups(
        DBInstanceIdentifier=instance_id,
    )
    automated_backups = resp["DBInstanceAutomatedBackups"]
    assert len(automated_backups) == 1
    automated_backup = automated_backups[0]
    assert automated_backup["DBInstanceIdentifier"] == instance_id
    assert automated_backup["Status"] == "retained"


@mock_rds
def test_delete_automated_backups_by_default():
    instance_id = "test-instance"
    client = boto3.client("rds", region_name="us-west-2")
    client.create_db_instance(
        DBInstanceIdentifier=instance_id,
        AllocatedStorage=10,
        Engine="postgres",
        DBName="staging-postgres",
        DBInstanceClass="db.m1.small",
        MasterUsername="root",
        MasterUserPassword="pass",
        StorageEncrypted=False,
    )
    resp = client.describe_db_instance_automated_backups(
        DBInstanceIdentifier=instance_id,
    )
    automated_backups = resp["DBInstanceAutomatedBackups"]
    assert len(automated_backups) == 1
    automated_backup = automated_backups[0]
    assert automated_backup["DBInstanceIdentifier"] == instance_id
    assert automated_backup["Status"] == "active"

    client.delete_db_instance(DBInstanceIdentifier=instance_id)

    resp = client.describe_db_instance_automated_backups(
        DBInstanceIdentifier=instance_id,
    )
    automated_backups = resp["DBInstanceAutomatedBackups"]
    assert len(automated_backups) == 0


@mock_rds
def test_describe_and_modify_snapshot_attributes():
    instance_id = "test-instance"
    client = boto3.client("rds", region_name="us-west-2")
    client.create_db_instance(
        DBInstanceIdentifier=instance_id,
        AllocatedStorage=10,
        Engine="postgres",
        DBName="staging-postgres",
        DBInstanceClass="db.m1.small",
        MasterUsername="root",
        MasterUserPassword="pass",
        StorageEncrypted=False,
    )
    snapshot = client.create_db_snapshot(
        DBInstanceIdentifier=instance_id, DBSnapshotIdentifier="snapshot-1"
    ).get("DBSnapshot")
    snapshot["DBSnapshotIdentifier"].should.equal("snapshot-1")

    # Describe snapshot that was created
    snapshot_attribute_result = client.describe_db_snapshot_attributes(
        DBSnapshotIdentifier=snapshot["DBSnapshotIdentifier"],
    )
    snapshot_attribute_result["DBSnapshotAttributesResult"][
        "DBSnapshotIdentifier"
    ].should.equal("snapshot-1")
    len(
        snapshot_attribute_result["DBSnapshotAttributesResult"]["DBSnapshotAttributes"]
    ).should.equal(1)
    snapshot_attribute_result["DBSnapshotAttributesResult"]["DBSnapshotAttributes"][0][
        "AttributeName"
    ].should.equal("restore")
    len(
        snapshot_attribute_result["DBSnapshotAttributesResult"]["DBSnapshotAttributes"][
            0
        ]["AttributeValues"]
    ).should.equal(0)

    # Modify the snapshot attribute (Add)
    customer_accounts_add = ["123", "456"]
    snapshot_attribute_result = client.modify_db_snapshot_attribute(
        DBSnapshotIdentifier=snapshot["DBSnapshotIdentifier"],
        AttributeName="restore",
        ValuesToAdd=customer_accounts_add,
    )
    snapshot_attribute_result["DBSnapshotAttributesResult"][
        "DBSnapshotIdentifier"
    ].should.equal("snapshot-1")
    len(
        snapshot_attribute_result["DBSnapshotAttributesResult"]["DBSnapshotAttributes"]
    ).should.equal(1)
    snapshot_attribute_result["DBSnapshotAttributesResult"]["DBSnapshotAttributes"][0][
        "AttributeName"
    ].should.equal("restore")
    len(
        snapshot_attribute_result["DBSnapshotAttributesResult"]["DBSnapshotAttributes"][
            0
        ]["AttributeValues"]
    ).should.equal(2)
    snapshot_attribute_result["DBSnapshotAttributesResult"]["DBSnapshotAttributes"][0][
        "AttributeValues"
    ].should.equal(customer_accounts_add)

    # Modify the snapshot attribute (Add + Remove)
    customer_accounts_add = ["789"]
    customer_accounts_remove = ["123", "456"]
    snapshot_attribute_result = client.modify_db_snapshot_attribute(
        DBSnapshotIdentifier=snapshot["DBSnapshotIdentifier"],
        AttributeName="restore",
        ValuesToAdd=customer_accounts_add,
        ValuesToRemove=customer_accounts_remove,
    )
    snapshot_attribute_result["DBSnapshotAttributesResult"][
        "DBSnapshotIdentifier"
    ].should.equal("snapshot-1")
    len(
        snapshot_attribute_result["DBSnapshotAttributesResult"]["DBSnapshotAttributes"]
    ).should.equal(1)
    snapshot_attribute_result["DBSnapshotAttributesResult"]["DBSnapshotAttributes"][0][
        "AttributeName"
    ].should.equal("restore")
    len(
        snapshot_attribute_result["DBSnapshotAttributesResult"]["DBSnapshotAttributes"][
            0
        ]["AttributeValues"]
    ).should.equal(1)
    snapshot_attribute_result["DBSnapshotAttributesResult"]["DBSnapshotAttributes"][0][
        "AttributeValues"
    ].should.equal(customer_accounts_add)

    # Modify the snapshot attribute (Remove)
    customer_accounts_remove = ["789"]
    snapshot_attribute_result = client.modify_db_snapshot_attribute(
        DBSnapshotIdentifier=snapshot["DBSnapshotIdentifier"],
        AttributeName="restore",
        ValuesToRemove=customer_accounts_remove,
    )
    snapshot_attribute_result["DBSnapshotAttributesResult"][
        "DBSnapshotIdentifier"
    ].should.equal("snapshot-1")
    len(
        snapshot_attribute_result["DBSnapshotAttributesResult"]["DBSnapshotAttributes"]
    ).should.equal(1)
    snapshot_attribute_result["DBSnapshotAttributesResult"]["DBSnapshotAttributes"][0][
        "AttributeName"
    ].should.equal("restore")
    len(
        snapshot_attribute_result["DBSnapshotAttributesResult"]["DBSnapshotAttributes"][
            0
        ]["AttributeValues"]
    ).should.equal(0)


@mock_rds
def test_describe_snapshot_attributes_fails_with_invalid_snapshot_identifier():
    client = boto3.client("rds", region_name="us-west-2")
    with pytest.raises(ClientError) as ex:
        client.describe_db_snapshot_attributes(
            DBSnapshotIdentifier="invalid_snapshot_id",
        )
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(404)
    ex.value.response["Error"]["Code"].should.equal("DBSnapshotNotFound")


@mock_rds
def test_modify_snapshot_attributes_fails_with_invalid_snapshot_id():
    client = boto3.client("rds", region_name="us-west-2")
    with pytest.raises(ClientError) as ex:
        client.modify_db_snapshot_attribute(
            DBSnapshotIdentifier="invalid_snapshot_id",
            AttributeName="restore",
            ValuesToRemove=["123"],
        )
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(404)
    ex.value.response["Error"]["Code"].should.equal("DBSnapshotNotFound")


@mock_rds
def test_modify_snapshot_attributes_fails_with_invalid_attribute_name():
    instance_id = "test-instance"
    client = boto3.client("rds", region_name="us-west-2")
    client.create_db_instance(
        DBInstanceIdentifier=instance_id,
        AllocatedStorage=10,
        Engine="postgres",
        DBName="staging-postgres",
        DBInstanceClass="db.m1.small",
        MasterUsername="root",
        MasterUserPassword="pass",
        StorageEncrypted=False,
    )
    snapshot = client.create_db_snapshot(
        DBInstanceIdentifier=instance_id, DBSnapshotIdentifier="snapshot-1"
    ).get("DBSnapshot")
    snapshot["DBSnapshotIdentifier"].should.equal("snapshot-1")

    with pytest.raises(ClientError) as ex:
        client.modify_db_snapshot_attribute(
            DBSnapshotIdentifier=snapshot["DBSnapshotIdentifier"],
            AttributeName="invalid_name",
            ValuesToAdd=["123"],
        )
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    ex.value.response["Error"]["Code"].should.equal("InvalidParameterValue")


@mock_rds
def test_modify_snapshot_attributes_fails_with_invalid_parameter_combination():
    instance_id = "test-instance"
    client = boto3.client("rds", region_name="us-west-2")
    client.create_db_instance(
        DBInstanceIdentifier=instance_id,
        AllocatedStorage=10,
        Engine="postgres",
        DBName="staging-postgres",
        DBInstanceClass="db.m1.small",
        MasterUsername="root",
        MasterUserPassword="pass",
        StorageEncrypted=False,
    )
    snapshot = client.create_db_snapshot(
        DBInstanceIdentifier=instance_id, DBSnapshotIdentifier="snapshot-1"
    ).get("DBSnapshot")
    snapshot["DBSnapshotIdentifier"].should.equal("snapshot-1")

    with pytest.raises(ClientError) as ex:
        client.modify_db_snapshot_attribute(
            DBSnapshotIdentifier=snapshot["DBSnapshotIdentifier"],
            AttributeName="restore",
            ValuesToAdd=["123", "456"],
            ValuesToRemove=["456"],
        )
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    ex.value.response["Error"]["Code"].should.equal("InvalidParameterCombination")


@mock_rds
def test_modify_snapshot_attributes_fails_when_exceeding_number_of_shared_accounts():
    instance_id = "test-instance"
    client = boto3.client("rds", region_name="us-west-2")
    client.create_db_instance(
        DBInstanceIdentifier=instance_id,
        AllocatedStorage=10,
        Engine="postgres",
        DBName="staging-postgres",
        DBInstanceClass="db.m1.small",
        MasterUsername="root",
        MasterUserPassword="pass",
        StorageEncrypted=False,
    )
    snapshot = client.create_db_snapshot(
        DBInstanceIdentifier=instance_id, DBSnapshotIdentifier="snapshot-1"
    ).get("DBSnapshot")
    snapshot["DBSnapshotIdentifier"].should.equal("snapshot-1")

    customer_accounts_add = [str(x) for x in range(30)]
    with pytest.raises(ClientError) as ex:
        client.modify_db_snapshot_attribute(
            DBSnapshotIdentifier=snapshot["DBSnapshotIdentifier"],
            AttributeName="restore",
            ValuesToAdd=customer_accounts_add,
        )
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    ex.value.response["Error"]["Code"].should.equal("SharedSnapshotQuotaExceeded")


@mock_rds
def test_modify_snapshot_attributes_fails_for_automated_snapshot():
    client = boto3.client("rds", region_name="us-west-2")
    client.create_db_instance(
        DBInstanceIdentifier="test-instance",
        AllocatedStorage=10,
        Engine="postgres",
        DBName="staging-postgres",
        DBInstanceClass="db.m1.small",
        MasterUsername="root",
        MasterUserPassword="pass",
        StorageEncrypted=False,
    )
    # Automated snapshots
    auto_snapshot = client.describe_db_snapshots(MaxRecords=20).get("DBSnapshots")[0]
    with pytest.raises(ClientError) as ex:
        client.modify_db_snapshot_attribute(
            DBSnapshotIdentifier=auto_snapshot["DBSnapshotIdentifier"],
            AttributeName="restore",
        )
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    ex.value.response["Error"]["Code"].should.equal("InvalidParameterValue")


@mock_rds
def test_copy_db_snapshot_fails_for_inaccessible_kms_key_arn():
    client = boto3.client("rds", region_name="us-west-2")
    client.create_db_instance(
        DBInstanceIdentifier="test-instance",
        AllocatedStorage=10,
        Engine="postgres",
        DBName="staging-postgres",
        DBInstanceClass="db.m1.small",
        MasterUsername="root",
        MasterUserPassword="pass",
        StorageEncrypted=True,
    )
    snapshot = client.create_db_snapshot(
        DBInstanceIdentifier="test-instance", DBSnapshotIdentifier="snapshot-1"
    ).get("DBSnapshot")
    snapshot["DBSnapshotIdentifier"].should.equal("snapshot-1")

    kms_key_id = (
        "arn:aws:kms:us-east-1:123456789012:key/6e551f00-8a97-4e3b-b620-1a59080bd1be"
    )
    with pytest.raises(ClientError) as ex:
        client.copy_db_snapshot(
            SourceDBSnapshotIdentifier="snapshot-1",
            TargetDBSnapshotIdentifier="snapshot-1-copy",
            KmsKeyId=kms_key_id,
        )
    message = f"Specified KMS key [{kms_key_id}] does not exist, is not enabled or you do not have permissions to access it."
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    ex.value.response["Error"]["Code"].should.equal("KMSKeyNotAccessibleFault")
    ex.value.response["Error"]["Message"].should.contain(message)


@mock_rds
def test_copy_db_snapshot_copy_tags_from_source_snapshot():
    client = boto3.client("rds", region_name="us-west-2")
    client.create_db_instance(
        DBInstanceIdentifier="instance-1",
        AllocatedStorage=10,
        Engine="postgres",
        DBName="staging-postgres",
        DBInstanceClass="db.m1.small",
        MasterUsername="root",
        MasterUserPassword="pass",
    )
    snapshot = client.create_db_snapshot(
        DBSnapshotIdentifier="snap-1",
        DBInstanceIdentifier="instance-1",
        Tags=test_tags,
    ).get("DBSnapshot")
    tag_list = client.list_tags_for_resource(
        ResourceName=snapshot["DBSnapshotArn"]
    ).get("TagList")
    tag_list.should.equal(test_tags)
    copied_snapshot = client.copy_db_snapshot(
        SourceDBSnapshotIdentifier="snap-1",
        TargetDBSnapshotIdentifier="snap-1-copy",
        CopyTags=True,
    ).get("DBSnapshot")
    tag_list = client.list_tags_for_resource(
        ResourceName=copied_snapshot["DBSnapshotArn"]
    ).get("TagList")
    tag_list.should.equal(test_tags)


@mock_rds
def test_copy_db_snapshot_tags_in_request():
    client = boto3.client("rds", region_name="us-west-2")
    client.create_db_instance(
        DBInstanceIdentifier="instance-1",
        AllocatedStorage=10,
        Engine="postgres",
        DBName="staging-postgres",
        DBInstanceClass="db.m1.small",
        MasterUsername="root",
        MasterUserPassword="pass",
    )
    snapshot = client.create_db_snapshot(
        DBSnapshotIdentifier="snap-1",
        DBInstanceIdentifier="instance-1",
        Tags=test_tags,
    ).get("DBSnapshot")
    tag_list = client.list_tags_for_resource(
        ResourceName=snapshot["DBSnapshotArn"]
    ).get("TagList")
    tag_list.should.equal(test_tags)
    new_snapshot_tags = [
        {
            "Key": "foo",
            "Value": "baz",
        },
    ]
    copied_snapshot = client.copy_db_snapshot(
        SourceDBSnapshotIdentifier="snap-1",
        TargetDBSnapshotIdentifier="snap-1-copy",
        Tags=new_snapshot_tags,
        CopyTags=True,
    ).get("DBSnapshot")
    tag_list = client.list_tags_for_resource(
        ResourceName=copied_snapshot["DBSnapshotArn"]
    ).get("TagList")
    tag_list.should.equal(new_snapshot_tags)
