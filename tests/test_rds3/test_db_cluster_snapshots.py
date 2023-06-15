import boto3
from botocore.exceptions import ClientError

from . import mock_rds
from sure import this
import pytest


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
def test_create_db_cluster_snapshot():
    client = boto3.client("rds", region_name="us-west-2")
    client.create_db_cluster(
        DBClusterIdentifier="cluster-1",
        DatabaseName="db_name",
        Engine="aurora-postgresql",
        MasterUsername="root",
        MasterUserPassword="password",
        Port=1234,
        Tags=test_tags,
    )
    snapshot = client.create_db_cluster_snapshot(
        DBClusterSnapshotIdentifier="cluster-snap", DBClusterIdentifier="cluster-1"
    ).get("DBClusterSnapshot")
    this(snapshot["DBClusterIdentifier"]).should.equal("cluster-1")


@mock_rds
def test_describe_db_cluster_snapshots_paginated():
    client = boto3.client("rds", region_name="us-west-2")
    client.create_db_cluster(
        DBClusterIdentifier="cluster-1",
        DatabaseName="db_name",
        Engine="aurora-postgresql",
        MasterUsername="root",
        MasterUserPassword="password",
        Port=1234,
        Tags=test_tags,
    )
    auto_snapshots = client.describe_db_cluster_snapshots(MaxRecords=20).get(
        "DBClusterSnapshots"
    )
    custom_snap_start = len(auto_snapshots)
    for i in range(custom_snap_start, 21):
        client.create_db_cluster_snapshot(
            DBClusterSnapshotIdentifier=f"cluster-snap-{i}",
            DBClusterIdentifier="cluster-1",
        )

    resp = client.describe_db_cluster_snapshots(MaxRecords=20)
    snaps = resp.get("DBClusterSnapshots")
    snaps.should.have.length_of(20)
    snaps[custom_snap_start]["DBClusterSnapshotIdentifier"].should.equal(
        f"cluster-snap-{custom_snap_start}"
    )

    resp2 = client.describe_db_cluster_snapshots(Marker=resp["Marker"])
    resp2["DBClusterSnapshots"].should.have.length_of(1)
    resp2["DBClusterSnapshots"][0]["DBClusterSnapshotIdentifier"].should.equal(
        "cluster-snap-20"
    )

    resp3 = client.describe_db_cluster_snapshots()
    resp3["DBClusterSnapshots"].should.have.length_of(21)


@mock_rds
def test_delete_db_cluster_snapshot():
    client = boto3.client("rds", region_name="us-west-2")
    client.create_db_cluster(
        DBClusterIdentifier="cluster-1",
        DatabaseName="db_name",
        Engine="aurora-postgresql",
        MasterUsername="root",
        MasterUserPassword="password",
        Port=1234,
    )
    client.create_db_cluster_snapshot(
        DBClusterSnapshotIdentifier="cluster-snap", DBClusterIdentifier="cluster-1"
    )
    snapshots = client.describe_db_cluster_snapshots(
        DBClusterIdentifier="cluster-1", SnapshotType="manual"
    ).get("DBClusterSnapshots")
    snapshots.should.have.length_of(1)
    snapshot = client.delete_db_cluster_snapshot(
        DBClusterSnapshotIdentifier="cluster-snap"
    ).get("DBClusterSnapshot")
    snapshot["DBClusterSnapshotIdentifier"].should.equal("cluster-snap")
    snapshots = client.describe_db_cluster_snapshots(
        DBClusterIdentifier="cluster-1", SnapshotType="manual"
    ).get("DBClusterSnapshots")
    snapshots.should.have.length_of(0)


@mock_rds
def test_delete_non_existent_db_cluster_snapshot_fails():
    client = boto3.client("rds", region_name="us-west-2")
    client.delete_db_cluster_snapshot.when.called_with(
        DBClusterSnapshotIdentifier="non-existent"
    ).should.throw(ClientError, "not found")


@mock_rds
def test_describe_and_modify_cluster_snapshot_attributes():
    client = boto3.client("rds", region_name="us-west-2")
    client.create_db_cluster(
        DBClusterIdentifier="cluster-1",
        DatabaseName="db_name",
        Engine="aurora-postgresql",
        MasterUsername="root",
        MasterUserPassword="password",
        Port=1234,
    )
    cluster_snapshot = client.create_db_cluster_snapshot(
        DBClusterSnapshotIdentifier="cluster-snap", DBClusterIdentifier="cluster-1"
    ).get("DBClusterSnapshot")
    cluster_snapshot["DBClusterSnapshotIdentifier"].should.equal("cluster-snap")
    cluster_snapshot_attribute_results = client.describe_db_cluster_snapshot_attributes(
        DBClusterSnapshotIdentifier=cluster_snapshot["DBClusterSnapshotIdentifier"]
    )
    cluster_snapshot_attribute_results["DBClusterSnapshotAttributesResult"][
        "DBClusterSnapshotIdentifier"
    ].should.equal("cluster-snap")
    len(
        cluster_snapshot_attribute_results["DBClusterSnapshotAttributesResult"][
            "DBClusterSnapshotAttributes"
        ]
    ).should.equal(1)
    cluster_snapshot_attribute_results["DBClusterSnapshotAttributesResult"][
        "DBClusterSnapshotAttributes"
    ][0]["AttributeName"].should.equal("restore")
    len(
        cluster_snapshot_attribute_results["DBClusterSnapshotAttributesResult"][
            "DBClusterSnapshotAttributes"
        ][0]["AttributeValues"]
    ).should.equal(0)

    # Modify the snapshot attribute (Add)
    customer_accounts_add = ["123", "456"]
    cluster_snapshot_attribute_result = client.modify_db_cluster_snapshot_attribute(
        DBClusterSnapshotIdentifier=cluster_snapshot["DBClusterSnapshotIdentifier"],
        AttributeName="restore",
        ValuesToAdd=customer_accounts_add,
    )
    cluster_snapshot_attribute_result["DBClusterSnapshotAttributesResult"][
        "DBClusterSnapshotIdentifier"
    ].should.equal("cluster-snap")
    len(
        cluster_snapshot_attribute_result["DBClusterSnapshotAttributesResult"][
            "DBClusterSnapshotAttributes"
        ]
    ).should.equal(1)
    cluster_snapshot_attribute_result["DBClusterSnapshotAttributesResult"][
        "DBClusterSnapshotAttributes"
    ][0]["AttributeName"].should.equal("restore")
    len(
        cluster_snapshot_attribute_result["DBClusterSnapshotAttributesResult"][
            "DBClusterSnapshotAttributes"
        ][0]["AttributeValues"]
    ).should.equal(2)
    cluster_snapshot_attribute_result["DBClusterSnapshotAttributesResult"][
        "DBClusterSnapshotAttributes"
    ][0]["AttributeValues"].should.equal(customer_accounts_add)

    # Modify the snapshot attribute (Add + Remove)
    customer_accounts_add = ["789"]
    customer_accounts_remove = ["123", "456"]
    cluster_snapshot_attribute_result = client.modify_db_cluster_snapshot_attribute(
        DBClusterSnapshotIdentifier=cluster_snapshot["DBClusterSnapshotIdentifier"],
        AttributeName="restore",
        ValuesToAdd=customer_accounts_add,
        ValuesToRemove=customer_accounts_remove,
    )
    cluster_snapshot_attribute_result["DBClusterSnapshotAttributesResult"][
        "DBClusterSnapshotIdentifier"
    ].should.equal("cluster-snap")
    len(
        cluster_snapshot_attribute_result["DBClusterSnapshotAttributesResult"][
            "DBClusterSnapshotAttributes"
        ]
    ).should.equal(1)
    cluster_snapshot_attribute_result["DBClusterSnapshotAttributesResult"][
        "DBClusterSnapshotAttributes"
    ][0]["AttributeName"].should.equal("restore")
    len(
        cluster_snapshot_attribute_result["DBClusterSnapshotAttributesResult"][
            "DBClusterSnapshotAttributes"
        ][0]["AttributeValues"]
    ).should.equal(1)
    cluster_snapshot_attribute_result["DBClusterSnapshotAttributesResult"][
        "DBClusterSnapshotAttributes"
    ][0]["AttributeValues"].should.equal(customer_accounts_add)

    # Modify the snapshot attribute (Add + Remove)
    customer_accounts_remove = ["789"]
    cluster_snapshot_attribute_result = client.modify_db_cluster_snapshot_attribute(
        DBClusterSnapshotIdentifier=cluster_snapshot["DBClusterSnapshotIdentifier"],
        AttributeName="restore",
        ValuesToRemove=customer_accounts_remove,
    )
    cluster_snapshot_attribute_result["DBClusterSnapshotAttributesResult"][
        "DBClusterSnapshotIdentifier"
    ].should.equal("cluster-snap")
    len(
        cluster_snapshot_attribute_result["DBClusterSnapshotAttributesResult"][
            "DBClusterSnapshotAttributes"
        ]
    ).should.equal(1)
    cluster_snapshot_attribute_result["DBClusterSnapshotAttributesResult"][
        "DBClusterSnapshotAttributes"
    ][0]["AttributeName"].should.equal("restore")
    len(
        cluster_snapshot_attribute_result["DBClusterSnapshotAttributesResult"][
            "DBClusterSnapshotAttributes"
        ][0]["AttributeValues"]
    ).should.equal(0)


@mock_rds
def test_describe_snapshot_attributes_with_invalid_cluster_snapshot_id():
    client = boto3.client("rds", region_name="us-west-2")
    with pytest.raises(ClientError) as ex:
        client.describe_db_cluster_snapshot_attributes(
            DBClusterSnapshotIdentifier="invalid_snapshot_id",
        )
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(404)
    ex.value.response["Error"]["Code"].should.equal("DBClusterSnapshotNotFoundFault")


@mock_rds
def test_modify_snapshot_attributes_with_invalid_cluster_snapshot_id():
    client = boto3.client("rds", region_name="us-west-2")
    with pytest.raises(ClientError) as ex:
        client.modify_db_cluster_snapshot_attribute(
            DBClusterSnapshotIdentifier="invalid_snapshot_id",
            AttributeName="restore",
            ValuesToRemove=["123"],
        )
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(404)
    ex.value.response["Error"]["Code"].should.equal("DBClusterSnapshotNotFoundFault")


@mock_rds
def test_modify_snapshot_attributes_with_invalid_attribute_name():
    client = boto3.client("rds", region_name="us-west-2")
    client.create_db_cluster(
        DBClusterIdentifier="cluster-1",
        DatabaseName="db_name",
        Engine="aurora-postgresql",
        MasterUsername="root",
        MasterUserPassword="password",
        Port=1234,
    )
    cluster_snapshot = client.create_db_cluster_snapshot(
        DBClusterSnapshotIdentifier="cluster-snap", DBClusterIdentifier="cluster-1"
    ).get("DBClusterSnapshot")
    cluster_snapshot["DBClusterSnapshotIdentifier"].should.equal("cluster-snap")

    with pytest.raises(ClientError) as ex:
        client.modify_db_cluster_snapshot_attribute(
            DBClusterSnapshotIdentifier=cluster_snapshot["DBClusterSnapshotIdentifier"],
            AttributeName="invalid_name",
            ValuesToAdd=["123"],
        )
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    ex.value.response["Error"]["Code"].should.equal("InvalidParameterValue")


@mock_rds
def test_modify_snapshot_attributes_with_invalid_parameter_combination():
    client = boto3.client("rds", region_name="us-west-2")
    client.create_db_cluster(
        DBClusterIdentifier="cluster-1",
        DatabaseName="db_name",
        Engine="aurora-postgresql",
        MasterUsername="root",
        MasterUserPassword="password",
        Port=1234,
    )
    cluster_snapshot = client.create_db_cluster_snapshot(
        DBClusterSnapshotIdentifier="cluster-snap", DBClusterIdentifier="cluster-1"
    ).get("DBClusterSnapshot")
    cluster_snapshot["DBClusterSnapshotIdentifier"].should.equal("cluster-snap")

    with pytest.raises(ClientError) as ex:
        client.modify_db_cluster_snapshot_attribute(
            DBClusterSnapshotIdentifier=cluster_snapshot["DBClusterSnapshotIdentifier"],
            AttributeName="restore",
            ValuesToAdd=["123", "456"],
            ValuesToRemove=["456"],
        )
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    ex.value.response["Error"]["Code"].should.equal("InvalidParameterCombination")


@mock_rds
def test_modify_snapshot_attributes_exceeding_number_of_shared_accounts():
    client = boto3.client("rds", region_name="us-west-2")
    client.create_db_cluster(
        DBClusterIdentifier="cluster-1",
        DatabaseName="db_name",
        Engine="aurora-postgresql",
        MasterUsername="root",
        MasterUserPassword="password",
        Port=1234,
    )
    cluster_snapshot = client.create_db_cluster_snapshot(
        DBClusterSnapshotIdentifier="cluster-snap", DBClusterIdentifier="cluster-1"
    ).get("DBClusterSnapshot")
    cluster_snapshot["DBClusterSnapshotIdentifier"].should.equal("cluster-snap")

    customer_accounts_add = [str(x) for x in range(30)]
    with pytest.raises(ClientError) as ex:
        client.modify_db_cluster_snapshot_attribute(
            DBClusterSnapshotIdentifier=cluster_snapshot["DBClusterSnapshotIdentifier"],
            AttributeName="restore",
            ValuesToAdd=customer_accounts_add,
        )
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    ex.value.response["Error"]["Code"].should.equal("SharedSnapshotQuotaExceeded")


@mock_rds
def test_modify_snapshot_attributes_for_automated_snapshot():
    client = boto3.client("rds", region_name="us-west-2")
    client.create_db_cluster(
        DBClusterIdentifier="cluster-1",
        DatabaseName="db_name",
        Engine="aurora-postgresql",
        MasterUsername="root",
        MasterUserPassword="password",
        Port=1234,
    )
    # Automated snapshots
    auto_snapshot = client.describe_db_cluster_snapshots(MaxRecords=20).get(
        "DBClusterSnapshots"
    )[0]
    with pytest.raises(ClientError) as ex:
        client.modify_db_cluster_snapshot_attribute(
            DBClusterSnapshotIdentifier=auto_snapshot["DBClusterSnapshotIdentifier"],
            AttributeName="restore",
        )
    ex.value.response["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)
    ex.value.response["Error"]["Code"].should.equal(
        "InvalidDBClusterSnapshotStateFault"
    )
