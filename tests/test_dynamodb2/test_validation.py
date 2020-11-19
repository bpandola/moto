import unittest
import boto3
import sure  # noqa
from botocore.exceptions import ClientError
from nose.tools import assert_raises
from parameterized import parameterized

from moto import mock_dynamodb2


def assert_error_response(resp, code=None, message=None):
    error = resp.exception.response["Error"]
    if code:
        error["Code"].should.equal(code)
    if message:
        error["Message"].should.contain(message)


@parameterized.expand(
    [
        (
            "Hash Key Missing",
            {"song": {"S": "all eyez on me"}},
            "One or more parameter values were invalid: Missing the key artist in the item",
        ),
        (
            "Range Key Missing",
            {"artist": {"S": "tupac"}},
            "One or more parameter values were invalid: Missing the key song in the item",
        ),
        (
            "Hash Key Empty",
            {"artist": {"S": ""}, "song": {"S": "all eyez on me"}},
            "One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty string value. Key: artist",
        ),
        (
            "Range Key Empty",
            {"artist": {"S": "tupac"}, "song": {"S": ""}},
            "One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty string value. Key: song",
        ),
        (
            "Hash Key Type Mismatch",
            {"artist": {"N": "0"}, "song": {"S": "all eyez on me"}},
            "One or more parameter values were invalid: Type mismatch for key artist expected: S actual: N",
        ),
        (
            "Range Key Type Mismatch",
            {"artist": {"S": "tupac"}, "song": {"SS": [""]}},
            "One or more parameter values were invalid: Type mismatch for key song expected: S actual: SS",
        ),
    ]
)
@unittest.skip("temporarily disabled")
@mock_dynamodb2
def test_put_item_key_validation(_, item, expected_error_message):
    client = boto3.client("dynamodb", region_name="us-west-2")
    table_name = "test-table"
    key_schema = [
        {"AttributeName": "artist", "KeyType": "HASH",},
        {"AttributeName": "song", "KeyType": "RANGE",},
    ]
    attribute_definitions = [
        {"AttributeName": "artist", "AttributeType": "S",},
        {"AttributeName": "song", "AttributeType": "S",},
    ]
    client.create_table(
        TableName=table_name,
        KeySchema=key_schema,
        AttributeDefinitions=attribute_definitions,
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    with assert_raises(ClientError) as ex:
        client.put_item(TableName=table_name, Item=item)
    assert_error_response(
        ex, "ValidationException", expected_error_message,
    )


@mock_dynamodb2
def test_attribute_path():
    client = boto3.client("dynamodb", region_name="us-west-2")
    table_name = "test-table"
    key_schema = [
        {"AttributeName": "id", "KeyType": "HASH",},
    ]
    attribute_definitions = [
        {"AttributeName": "id", "AttributeType": "S",},
    ]
    client.create_table(
        TableName=table_name,
        KeySchema=key_schema,
        AttributeDefinitions=attribute_definitions,
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    client.put_item(
        TableName=table_name,
        Item={
            "id": {"S": "foo2"},
            "itemmap": {
                "M": {
                    "itemlist": {
                        "L": [
                            {"M": {"foo00": {"S": "bar1"}, "foo01": {"S": "bar2"}}},
                            {"M": {"foo10": {"S": "bar1"}, "foo11": {"S": "bar2"}}},
                        ]
                    }
                }
            },
        },
    )
    # "Remove itemmap.itemlist[1]"
