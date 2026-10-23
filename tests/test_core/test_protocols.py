# mypy: ignore-errors
import base64
import copy
import json
import math
import os
import re
from calendar import timegm
from enum import Enum
from urllib.parse import parse_qs, urlparse

import pytest
from botocore.parsers import RpcV2CBORParser as BotocoreRpcV2CBORParser
from botocore.utils import parse_timestamp
from dateutil.tz import tzutc
from werkzeug.datastructures import Headers, MultiDict

from moto.core.model import OperationModel, ServiceModel
from moto.core.parse import PROTOCOL_PARSERS
from moto.core.responses import BaseResponse
from moto.core.serialize import SERIALIZERS

CBOR_PROTOCOL = "smithy-rpc-v2-cbor"
TEST_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "protocols")
PROTOCOL_TEST_BLACKLIST = [
    "REST XML Event Stream",
    "RPC JSON Event Stream",
]


class TestType(Enum):
    __test__ = False  # Tell test runner to ignore this class

    INPUT = "input"
    OUTPUT = "output"


def _compliance_tests(test_type=None):
    inp = test_type is None or test_type is TestType.INPUT
    out = test_type is None or test_type is TestType.OUTPUT

    for full_path in _walk_files():
        if full_path.endswith(".json"):
            for model, case, protocol in _load_cases(full_path):
                if model.get("description") in PROTOCOL_TEST_BLACKLIST:
                    continue
                description = case["description"]
                test_name = f"{protocol}-protocol-{description}"
                if "params" in case and inp:
                    yield pytest.param(model, case, protocol, id=test_name)
                elif "response" in case and out:
                    yield pytest.param(model, case, protocol, id=test_name)


def _walk_files():
    for root, _, filenames in os.walk(TEST_DIR):
        for filename in filenames:
            yield os.path.join(root, filename)


def _load_cases(full_path):
    all_test_data = json.load(open(full_path))
    protocol = os.path.basename(full_path).split(".")[0]
    # e.g. smithy-rpc-v2-cbor-query-compatible.json
    protocol = re.sub(r"(-non)?-query-compatible$", "", protocol)
    for test_data in all_test_data:
        cases = test_data.pop("cases")
        description = test_data["description"]
        for index, case in enumerate(cases):
            case["description"] = description
            case["id"] = index
            yield test_data, case, protocol


def _compliance_blob_parser(value):
    # By default, Blobs are returned as bytes type because it's possible
    # that the blob contains binary data that actually can't be decoded.
    # For the compliance tests, we decode to a string for easier asserts.
    return base64.b64decode(value).decode("utf-8")


def _compliance_blob_parser_for(protocol):
    if protocol == CBOR_PROTOCOL:
        # Blobs are raw bytes in CBOR, not base64-encoded.
        return lambda value: value.decode("utf-8")
    return _compliance_blob_parser


def _special_floats_to_str(value):
    if isinstance(value, dict):
        return {k: _special_floats_to_str(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_special_floats_to_str(v) for v in value]
    if isinstance(value, float) and math.isnan(value):
        return "NaN"
    if isinstance(value, float) and math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    return value


def _bytes_to_str(value):
    if isinstance(value, dict):
        return {k: _bytes_to_str(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_bytes_to_str(v) for v in value]
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _error_shape_for(operation_model, error_code):
    for error_shape in operation_model.error_shapes:
        if error_shape.error_code == error_code:
            return error_shape
    raise AssertionError(f"No error shape found for {error_code}")


def _remove_none_values(value):
    if isinstance(value, dict):
        return {k: _remove_none_values(v) for k, v in value.items() if v is not None}
    return value


def _compliance_timestamp_parser(value):
    datetime = parse_timestamp(value)
    datetime = datetime.astimezone(tzutc())
    epoch_time = int(timegm(datetime.timetuple()))
    return epoch_time


def _build_query_params(query_string, body, headers):
    def multidict_from_querystring(qs):
        params = parse_qs(qs, keep_blank_values=True)
        return MultiDict(params)

    query_params = multidict_from_querystring(query_string)
    if headers.get("Content-Type", "").startswith("application/x-www-form-urlencoded"):
        body_params = multidict_from_querystring(body)
        query_params.update(body_params)
    return query_params


def _create_request_dict(given, serialized):
    method = serialized.get("method", given.get("http", {}).get("method", "POST"))
    # We need the headers to be case-insensitive
    headers = Headers(serialized.get("headers", {}))
    uri = serialized.get("uri", "/")
    parsed_uri = urlparse(uri)
    query_string = parsed_uri.query
    url_path = parsed_uri.path
    body = serialized["body"]
    values = _build_query_params(query_string, serialized["body"], headers)
    uri_template = given.get("http", {}).get("requestUri", "/")
    uri_regex = BaseResponse.uri_to_regexp(uri_template)
    uri_match = re.match(uri_regex, url_path)
    uri_params = uri_match.groupdict() if uri_match else {}
    request_dict = {
        "method": method,
        "body": body,
        "headers": headers,
        "values": values,
        "url_path": url_path,
        "url_params": uri_params,
    }
    return request_dict


@pytest.mark.parametrize(
    "json_description, case, protocol", list(_compliance_tests(TestType.INPUT))
)
def test_input_compliance(json_description: dict, case: dict, protocol: str):
    service_description = copy.deepcopy(json_description)
    model = ServiceModel(service_description)
    operation_model = OperationModel(case["given"], model)
    protocol_parser = PROTOCOL_PARSERS[protocol]
    parser = protocol_parser(
        operation_model,
        blob_parser=_compliance_blob_parser_for(protocol),
        timestamp_parser=_compliance_timestamp_parser,
    )
    serialized = case["serialized"]
    if protocol == CBOR_PROTOCOL:
        serialized = {**serialized, "body": base64.b64decode(serialized["body"])}
    request_dict = _create_request_dict(case["given"], serialized)
    parsed = parser.parse(request_dict)
    expected = case.get("params", {})
    if protocol == CBOR_PROTOCOL:
        # The CBOR test cases are written from the client's perspective, so null
        # parameters are never sent, and NaN/Infinity are given as strings.
        parsed = _special_floats_to_str(parsed)
        expected = _remove_none_values(expected)
    assert parsed == expected


@pytest.mark.parametrize(
    "json_description, case, protocol", list(_compliance_tests(TestType.OUTPUT))
)
def test_output_compliance(json_description: dict, case: dict, protocol):
    service_description = copy.deepcopy(json_description)
    model = ServiceModel(service_description)
    operation_model = OperationModel(case["given"], model)
    protocol_serializer = SERIALIZERS[protocol]
    serializer = protocol_serializer(operation_model)
    result = case["result"] if "error" not in case else _create_exception(case)
    resp = serializer.serialize(result)  # _to_response(result)
    if protocol == CBOR_PROTOCOL:
        _assert_cbor_output_compliance(operation_model, case, resp)
        return
    assert resp["body"] == case["response"]["body"]
    protocol_to_content_type = {
        "ec2": "text/xml",
        "json": "application/x-amz-json-1.0",
        "query": "text/xml",
        "query-json": "application/json",
        "rest-xml": "application/xml",
        "rest-json": "application/json",
    }
    if resp["body"]:
        content_type = resp["headers"]["Content-Type"]
        assert content_type == protocol_to_content_type[protocol]
    headers_expected = case["response"]["headers"]
    # TODO: Get rid of this if once we get the headers sorted for all responses
    if headers_expected:
        resp["headers"].pop("Content-Type", None)
        assert resp["headers"] == headers_expected
    assert resp["status_code"] == case["response"]["status_code"]


def _assert_cbor_output_compliance(operation_model, case, resp):
    # The CBOR test cases are written from the client's perspective: they describe
    # how a client should parse the given response, including responses that Moto
    # would never send (e.g. half-precision floats, extra fields).  Instead of
    # comparing bytes, we verify that botocore parses our response into the
    # expected result.
    expected_headers = case["response"]["headers"]
    assert resp["headers"]["smithy-protocol"] == "rpc-v2-cbor"
    assert "X-Amzn-Errortype" not in resp["headers"]
    if "x-amzn-query-error" in expected_headers:
        assert (
            resp["headers"]["x-amzn-query-error"]
            == expected_headers["x-amzn-query-error"]
        )
    if "error" in case:
        # The fixtures use a 400 for all errors, but botocore models carry the
        # status code of the awsQueryError trait (e.g. 402), which Moto honors.
        assert 400 <= resp["status_code"] < 600
    else:
        assert resp["status_code"] == case["response"]["status_code"]
    if operation_model.output_shape is None and "error" not in case:
        assert resp["body"] == b""
        assert "Content-Type" not in resp["headers"]
    else:
        assert resp["headers"]["Content-Type"] == "application/cbor"

    parser = BotocoreRpcV2CBORParser(timestamp_parser=lambda value: value)
    response_dict = {
        "status_code": resp["status_code"],
        "headers": resp["headers"],
        "body": resp["body"],
    }
    if "error" in case:
        # Like botocore's client, parse the error code first, then the modeled fields.
        parsed_error = parser.parse(response_dict, operation_model.output_shape)
        assert parsed_error["Error"]["Code"] == case["errorCode"]
        if "errorMessage" in case:
            assert parsed_error["Error"]["Message"] == case["errorMessage"]
        error_shape = _error_shape_for(operation_model, case["errorCode"])
        parsed = parser.parse(response_dict, error_shape)
        expected = {k: v for k, v in case["error"].items() if k.lower() != "message"}
        parsed = {k: v for k, v in parsed.items() if k.lower() != "message"}
        assert parsed == expected
    else:
        parsed = parser.parse(response_dict, operation_model.output_shape)
        parsed.pop("ResponseMetadata", None)
        parsed = _bytes_to_str(_special_floats_to_str(parsed))
        assert parsed == _remove_none_values(case["result"])


def _create_exception(case):
    exc = type(case["errorCode"], (Exception,), {})()
    exc.code = case["errorCode"]
    if "errorMessage" in case:
        exc.message = case["errorMessage"]
        exc.Message = case["errorMessage"]
    for key, value in case["error"].items():
        setattr(exc, key, value)
    return exc
