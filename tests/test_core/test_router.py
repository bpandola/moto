from werkzeug.routing import Map, MapAdapter

from moto.core.request import Request
from moto.core.routing import GreedyPathConverter, ServiceOperationRouter
from moto.core.utils import get_service_model


def test_op_router() -> None:
    model = get_service_model("mq")
    router = ServiceOperationRouter(model)
    req = Request.from_values(
        method="POST", path="/v1/brokers/broker-id-test/users/username-test"
    )
    op, args = router.match(req)

    assert op.name == "CreateUser"
    assert args["broker-id"] == "broker-id-test"
    assert args["username"] == "username-test"


def test_same_path_different_query_args() -> None:
    model = get_service_model("s3")
    router = ServiceOperationRouter(model)
    req = Request.from_values(method="GET", path="/my-bucket-name")
    op, args = router.match(req)
    assert op.name == "ListObjects"
    assert args["Bucket"] == "my-bucket-name"
    req = Request.from_values(method="GET", path="/my-bucket-name?list-type=2")
    op, args = router.match(req)
    assert op.name == "ListObjectsV2"
    assert args["Bucket"] == "my-bucket-name"


def test_s3_router() -> None:
    model = get_service_model("s3")
    router = ServiceOperationRouter(model)
    req = Request.from_values(method="GET", path="/my-bucket-name?list-type=2")
    # Alternative url
    # req = Request.from_values(method="GET", base_url="https://my-bucket-name.localhost", path="/?list-type=2")
    op, args = router.match(req)

    assert op.name == "ListObjectsV2"
    assert args["Bucket"] == "my-bucket-name"


def test_s3_full_url() -> None:
    model = get_service_model("s3")
    router = ServiceOperationRouter(model)
    req = Request.from_values(
        method="GET",
        base_url="https://b7525d4a-4973-4207-9f07-a73b4ec3ff65.s3.amazonaws.com",
        path="/",
        query_string="tagging",
    )
    op, args = router.match(req)

    assert op.name == "GetBucketTagging"
    assert args["Bucket"] == "b7525d4a-4973-4207-9f07-a73b4ec3ff65"
    "https://123456789012.s3-control.us-east-1.amazonaws.com/v20180820/tags/arn%3Aaws%3As3%3A%3A%3Abd054ad3-6778-4f25-91a5-c7c84db350e2"


def test_s3_control_full_url() -> None:
    model = get_service_model("s3control")
    router = ServiceOperationRouter(model)
    req = Request.from_values(
        method="GET",
        base_url="https://123456789012.s3-control.us-east-1.amazonaws.com",
        path="/v20180820/tags/arn%3Aaws%3As3%3A%3A%3Abd054ad3-6778-4f25-91a5-c7c84db350e2",
    )
    op, args = router.match(req)

    assert op.name == "ListTagsForResource"
    assert args["AccountId"] == "123456789012"


def test_op_args() -> None:
    model = get_service_model("route53")
    router = ServiceOperationRouter(model)
    # Get the one rule we want...
    rules = router._map["rest-xml"]._rules_by_endpoint[
        model.operation_model("ActivateKeySigningKey")
    ]
    rules = [rule.empty() for rule in rules]
    rule_map = Map(
        rules=rules,
        strict_slashes=False,
        merge_slashes=False,
        converters={"path": GreedyPathConverter},
    )
    matcher: MapAdapter = rule_map.bind(
        "route53.us-east-1.amazon.com",
    )
    op, args = matcher.match(
        "/2013-04-01/keysigningkey/HostedZoneId/Name/activate",
        method="POST",
    )
    assert args["HostedZoneId"] == "HostedZoneId"
    assert args["Name"] == "Name"
    assert op.name == "ActivateKeySigningKey"
