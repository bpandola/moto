from __future__ import annotations

from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from botocore.httpchecksum import AwsChunkedWrapper
from werkzeug.wrappers import Request as _Request

from moto.settings import MAX_FORM_MEMORY_SIZE
from moto.utilities.constants import APPLICATION_JSON, JSON_TYPES

if TYPE_CHECKING:
    from botocore.awsrequest import AWSPreparedRequest

    from moto.core.model import ServiceModel


class Request(_Request):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.max_form_memory_size = MAX_FORM_MEMORY_SIZE

    @classmethod
    def from_values(cls, *args: Any, **kwargs: Any) -> Request:
        req = super().from_values(*args, **kwargs)
        return Request(req.environ.copy())


def normalize_request(request: AWSPreparedRequest | _Request) -> Request:
    if isinstance(request, Request):
        return request
    if isinstance(request, _Request):
        return Request(request.environ.copy())
    body = request.body if request.body is not None else b""
    if isinstance(request.body, AwsChunkedWrapper):
        body = request.body.read()
    parsed_url = urlparse(request.url)
    normalized_request = Request.from_values(
        method=request.method,
        base_url=f"{parsed_url.scheme}://{parsed_url.netloc}",
        path=parsed_url.path,
        query_string=parsed_url.query,
        data=body,
        headers=[
            (k, v) for k, v in request.headers.items() if k != "Transfer-Encoding"
        ],
    )
    return normalized_request


def determine_request_protocol(
    service_model: ServiceModel, content_type: str | None = None
) -> str:
    protocol = str(service_model.protocol)
    supported_protocols = service_model.metadata.get("protocols", [protocol])
    content_type = content_type if content_type is not None else ""
    if service_model.service_name == "s3":
        protocol = "rest-xml"
    elif content_type in JSON_TYPES:
        protocol = "rest-json" if content_type == APPLICATION_JSON else "json"
    elif content_type.startswith("application/x-www-form-urlencoded"):
        protocol = "ec2" if "ec2" in supported_protocols else "query"
    if protocol not in supported_protocols:
        raise NotImplementedError(
            f"Unsupported protocol [{protocol}] for service {service_model.service_name}"
        )
    return protocol
