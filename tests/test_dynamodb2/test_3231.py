from __future__ import unicode_literals, print_function

import boto3
import sure  # noqa
from botocore.exceptions import ClientError
from nose.tools import assert_raises

from moto import mock_dynamodb2
import unittest


@unittest.skip("temporarily disabled")
@mock_dynamodb2
def test_put_item_with_invalid_numeric_value_fails():
    conn = boto3.client("dynamodb", region_name="us-west-2")
    table_name = "test-table"
    conn.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "a_number", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "a_number", "AttributeType": "N"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    # Invalid key value.
    with assert_raises(ClientError) as ex:
        conn.put_item(
            TableName=table_name, Item={"a_number": {"N": "abc"}},
        )
    ex.exception.response["Error"]["Code"].should.equal("ValidationException")
    ex.exception.response["Error"]["Message"].should.equal(
        "The parameter cannot be converted to a numeric value: abc"
    )
    # Invalid non-key value.
    with assert_raises(ClientError) as ex:
        conn.put_item(
            TableName=table_name,
            Item={"a_number": {"N": "1"}, "another_invalid_number": {"N": "xyz"}},
        )
    ex.exception.response["Error"]["Code"].should.equal("ValidationException")
    ex.exception.response["Error"]["Message"].should.equal(
        "The parameter cannot be converted to a numeric value: xyz"
    )
