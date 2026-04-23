from __future__ import annotations

from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from werkzeug.wrappers import Request as _Request
from werkzeug.wrappers.request import cached_property

from moto.settings import MAX_FORM_MEMORY_SIZE
from moto.utilities.constants import APPLICATION_JSON, JSON_TYPES

if TYPE_CHECKING:
    from botocore.awsrequest import AWSPreparedRequest

    from moto.core.model import ServiceModel


class Request(_Request):
    max_form_memory_size: int = MAX_FORM_MEMORY_SIZE

    @cached_property
    def data(self) -> bytes:
        if self.content_encoding and "aws-chunked" in self.content_encoding:
            return self.input_stream.getvalue()  # type: ignore[attr-defined]
        return super().get_data()

    @classmethod
    def from_values(cls, *args: Any, **kwargs: Any) -> Request:
        req = super().from_values(*args, **kwargs)
        return Request(req.environ)


def normalize_request(request: AWSPreparedRequest | _Request) -> Request:
    if isinstance(request, Request):
        return request
    if isinstance(request, _Request):
        return Request(request.environ.copy())
    parsed_url = urlparse(request.url)
    normalized_request = Request.from_values(
        method=request.method,
        base_url=f"{parsed_url.scheme}://{parsed_url.netloc}",
        path=parsed_url.path,
        query_string=parsed_url.query,
        data=request.body,
        headers=[(k, v) for k, v in request.headers.items()],
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
