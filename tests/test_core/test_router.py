import pytest

from moto.core.request import Request
from moto.core.routing import ServiceOperationRouter
from moto.core.utils import get_service_model
from moto.s3.responses import S3Response
from moto.s3control.responses import S3ControlResponse

s3_response = S3Response()


def test_op_router() -> None:
    model = get_service_model("mq")
    router = ServiceOperationRouter(model)
    req = Request.from_primitives(
        "POST", "/v1/brokers/broker-id-test/users/username-test", {}
    )
    op, args = router.match(req)

    assert op.name == "CreateUser"
    assert args["broker-id"] == "broker-id-test"
    assert args["username"] == "username-test"


def test_same_path_different_query_args() -> None:
    model = get_service_model("s3")
    router = ServiceOperationRouter(model)
    req = Request.from_primitives("GET", "/my-bucket-name", {})
    op, args = router.match(req)
    assert op.name == "ListObjects"
    assert args["Bucket"] == "my-bucket-name"
    req = Request.from_primitives("GET", "/my-bucket-name?list-type=2", {})
    op, args = router.match(req)
    assert op.name == "ListObjectsV2"
    assert args["Bucket"] == "my-bucket-name"


def test_s3_router() -> None:
    model = get_service_model("s3")
    router = ServiceOperationRouter(model)
    req = Request.from_primitives("GET", "/my-bucket-name?list-type=2", {})
    # Alternative url
    # req = Request.from_primitives(method="GET", base_url="https://my-bucket-name.localhost", path="/?list-type=2")
    op, args = router.match(req)

    assert op.name == "ListObjectsV2"
    assert args["Bucket"] == "my-bucket-name"


def test_s3_localhost_router() -> None:
    model = get_service_model("s3")
    router = ServiceOperationRouter(model)
    req = Request.from_primitives("GET", "https://foobaz.localhost:5000/", {})
    op, args = router.match(req, s3_response)

    assert op.name == "ListObjects"
    assert args["Bucket"] == "foobaz"


def test_s3_full_url() -> None:
    model = get_service_model("s3")
    router = ServiceOperationRouter(model)
    req = Request.from_primitives(
        "GET",
        "https://b7525d4a-4973-4207-9f07-a73b4ec3ff65.s3.amazonaws.com/?tagging",
        {},
    )
    op, args = router.match(req, s3_response)

    assert op.name == "GetBucketTagging"
    assert args["Bucket"] == "b7525d4a-4973-4207-9f07-a73b4ec3ff65"


def test_s3_control_full_url() -> None:
    model = get_service_model("s3control")
    router = ServiceOperationRouter(model)
    req = Request.from_primitives(
        "GET",
        "https://123456789012.s3-control.us-east-1.amazonaws.com/v20180820/tags/arn%3Aaws%3As3%3A%3A%3Abd054ad3-6778-4f25-91a5-c7c84db350e2",
        {"x-amz-account-id": "0123456789012"},
    )
    op, args = router.match(req)

    assert op.name == "ListTagsForResource"
    # AccountId comes from the host prefix, when the host has it.
    assert args == {
        "resourceArn": "arn:aws:s3:::bd054ad3-6778-4f25-91a5-c7c84db350e2",
        "AccountId": "123456789012",
    }


def test_s3_control_server_mode_url() -> None:
    router = ServiceOperationRouter(get_service_model("s3control"))
    req = Request.from_primitives(
        "GET",
        "http://localhost:5000/v20180820/accesspoint/my-access-point",
        {"x-amz-account-id": "123456789012"},
    )
    op, args = router.match(req)

    assert op.name == "GetAccessPoint"
    # No host prefix, so no AccountId (the parser falls back to the header).
    assert args == {"name": "my-access-point"}


@pytest.mark.parametrize(
    "url,expected_account_id",
    [
        pytest.param(
            "https://111111111111.s3-control.us-east-1.amazonaws.com",
            "111111111111",
            id="host prefix",
        ),
        pytest.param("http://localhost:5000", "222222222222", id="header fallback"),
    ],
)
def test_s3_control_account_id_host_label(url: str, expected_account_id: str) -> None:
    response = S3ControlResponse()
    req = Request.from_primitives(
        "GET",
        f"{url}/v20180820/accesspoint/my-access-point",
        {"x-amz-account-id": "222222222222"},
    )
    response.setup_class(req, req.url)
    assert response._get_param("AccountId") == expected_account_id
    assert response._get_param("Name") == "my-access-point"


def test_op_args() -> None:
    model = get_service_model("route53")
    router = ServiceOperationRouter(model)
    req = Request.from_primitives(
        "POST",
        "https://route53.us-east-1.amazon.com/2013-04-01/keysigningkey/HostedZoneId/Name/activate",
        {},
    )
    op, args = router.match(req)
    assert op.name == "ActivateKeySigningKey"
    assert args["HostedZoneId"] == "HostedZoneId"
    assert args["Name"] == "Name"


def test_s3_key_trailing_slash_is_preserved() -> None:
    router = ServiceOperationRouter(get_service_model("s3"))
    req = Request.from_primitives("PUT", "https://s3.amazonaws.com/bucket/dir/", {})
    op, args = router.match(req)
    assert op.name == "PutObject"
    assert args["Key"] == "dir/"


def test_s3_bucket_trailing_slash_is_optional() -> None:
    router = ServiceOperationRouter(get_service_model("s3"))
    for url in ["https://s3.amazonaws.com/bucket", "https://s3.amazonaws.com/bucket/"]:
        op, args = router.match(Request.from_primitives("PUT", url, {}))
        assert op.name == "CreateBucket"
        assert args == {"Bucket": "bucket"}


def test_uri_params_are_decoded_once() -> None:
    router = ServiceOperationRouter(get_service_model("s3"))
    req = Request.from_primitives("PUT", "https://s3.amazonaws.com/bucket/a%2525b", {})
    _, args = router.match(req)
    assert args["Key"] == "a%25b"


def test_last_label_is_implicitly_greedy() -> None:
    router = ServiceOperationRouter(get_service_model("bedrock-agentcore-control"))
    arn = "arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime/abc"
    req = Request.from_primitives("GET", f"https://localhost/tags/{arn}", {})
    op, args = router.match(req)
    assert op.name == "ListTagsForResource"
    assert args["resourceArn"] == arn


def test_implicitly_greedy_label_excludes_trailing_slash() -> None:
    router = ServiceOperationRouter(get_service_model("s3"))
    req = Request.from_primitives("GET", "https://s3.amazonaws.com/bucket/?tagging", {})
    op, args = router.match(req)
    assert op.name == "GetBucketTagging"
    assert args == {"Bucket": "bucket"}


def test_s3_virtual_host_key_with_leading_slash() -> None:
    router = ServiceOperationRouter(get_service_model("s3"))
    req = Request.from_primitives("PUT", "https://bucket.s3.amazonaws.com//key", {})
    op, args = router.match(req, s3_response)
    assert op.name == "PutObject"
    assert args == {"Bucket": "bucket", "Key": "/key"}


def test_rest_query_arg_constraints_ignore_form_body() -> None:
    router = ServiceOperationRouter(get_service_model("s3"))
    body = b"delete="
    req = Request.from_primitives(
        "POST",
        "https://s3.amazonaws.com/bucket",
        {"Content-Type": "application/x-www-form-urlencoded"},
        body,
    )
    op, _ = router.match(req)
    # The form body doesn't satisfy DeleteObjects' `?delete` query literal...
    assert op.name == "PostObject"
    # ...and matching didn't consume the request body.
    assert req.get_data() == body
