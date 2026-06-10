from __future__ import annotations

from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from botocore.awsrequest import AWSPreparedRequest
from botocore.httpchecksum import AwsChunkedWrapper
from werkzeug.wrappers import Request as WerkzeugRequest

from moto.settings import MAX_FORM_MEMORY_SIZE
from moto.utilities.constants import APPLICATION_JSON, JSON_TYPES

if TYPE_CHECKING:
    from moto.core.model import ServiceModel


class Request(WerkzeugRequest):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.max_form_memory_size = MAX_FORM_MEMORY_SIZE

    @classmethod
    def from_primitives(
        cls, method: str, url: str, headers: Any, body: str | bytes | None = None
    ) -> Request:
        # TODO: do a from values here instead of using AWS intermediary
        wrapper = AWSPreparedRequest(method, url, headers, body, stream_output=False)
        return normalize_request(wrapper)

    @classmethod
    def from_values(cls, *args: Any, **kwargs: Any) -> Request:
        req = super().from_values(*args, **kwargs)
        return Request(req.environ.copy())

    @property
    def raw_path(self) -> str:
        raw_uri: str = self.environ.get("RAW_URI", "")
        # If RAW_URI starts with a double slash, werkzeug will fail to parse it correctly and
        # Request.path will be invalid.  This can occur with Amazon S3 Virtual-Hosted requests,
        # where the bucket name is part of the domain name, combined with an object key that
        # begins with a slash (e.g., bucket-name.s3.amazonaws.com//object-key).
        revert_quote = False
        if raw_uri.startswith("//"):
            raw_uri = "/%2F" + raw_uri[2:]
            revert_quote = True
        # We have to parse because RAW_URI can contain a full URL.
        to_parse = raw_uri or self.path
        raw_path = urlparse(to_parse).path
        if revert_quote:
            raw_path = raw_path.replace("%2F", "/", 1)
        if raw_path and not raw_path.startswith("/"):
            raw_path = f"/{raw_path}"
        if not raw_path:
            raw_path = "/"
        return raw_path

    @property
    def raw_url(self) -> str:
        url_root = self.url_root.rstrip("/")
        raw_url = f"{url_root}{self.raw_path}"
        if self.query_string:
            qs = f"{self.query_string.decode()}"
            # qs = qs.replace("%2F", "/")
            raw_url += f"?{qs}"
        return raw_url


def normalize_request(
    request: AWSPreparedRequest | WerkzeugRequest | Request,
) -> Request:
    if isinstance(request, Request):
        return request
    if isinstance(request, WerkzeugRequest):
        return Request(request.environ.copy())
    if isinstance(request.body, AwsChunkedWrapper):
        body = request.body.read()
    else:
        body = request.body if request.body is not None else b""
    for header, value in request.headers.items():
        if isinstance(value, bytes):
            request.headers[header] = value.decode("utf-8")

    headers_to_strip: list[str] = ["Transfer-Encoding"]
    parsed_url = urlparse(request.url)
    # If path starts with a double slash, werkzeug will fail to parse it correctly and
    # Request.path will be invalid.  This can occur with Amazon S3 Virtual-Hosted requests,
    # where the bucket name is part of the domain name, combined with an object key that
    # begins with a slash (e.g., bucket-name.s3.amazonaws.com//object-key).
    # path = (
    #     "/%2F" + parsed_url.path[2:]
    #     if parsed_url.path.startswith("//")
    #     else parsed_url.path
    # )
    path = parsed_url.path
    normalized_request = Request.from_values(
        method=request.method,
        base_url=f"{parsed_url.scheme}://{parsed_url.netloc}",
        path=path,
        query_string=parsed_url.query,
        data=body,
        headers=[
            (k, v) for k, v in request.headers.items() if k not in headers_to_strip
        ],
    )
    # There are some S3 checks that fail when CONTENT_LENGTH not set.
    if "CONTENT_LENGTH" not in normalized_request.environ:
        normalized_request.environ["CONTENT_LENGTH"] = "0"
    return normalized_request


def determine_request_protocol(
    service_model: ServiceModel, content_type: str | None = None
) -> str:
    protocol = str(service_model.protocol)
    # Short circuit protocol detection for S3 because the ContentType header
    # is often set based on the MIME type of the object data being uploaded.
    if service_model.service_name == "s3":
        return protocol
    supported_protocols = service_model.metadata.get("protocols", [protocol])
    content_type = content_type if content_type is not None else ""
    if content_type in JSON_TYPES:
        protocol = "rest-json" if content_type == APPLICATION_JSON else "json"
    elif content_type.startswith("application/x-www-form-urlencoded"):
        protocol = "ec2" if "ec2" in supported_protocols else "query"
    if protocol not in supported_protocols:
        raise NotImplementedError(
            f"Unsupported protocol [{protocol}] for service {service_model.service_name}"
        )
    return protocol
