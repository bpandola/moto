from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlparse

from botocore.httpchecksum import AwsChunkedWrapper
from werkzeug.local import LocalProxy
from werkzeug.wrappers import Request as WerkzeugRequest

from moto.settings import MAX_FORM_MEMORY_SIZE
from moto.utilities.constants import APPLICATION_JSON, JSON_TYPES

if TYPE_CHECKING:
    from botocore.awsrequest import AWSPreparedRequest

    from moto.core.model import ServiceModel

# Headers that describe how the body arrived on the wire, rather than the body we
# hand on.  The proxy de-chunks before we ever see the request and an
# AwsChunkedWrapper is read out in full, so Transfer-Encoding no longer applies.
# Compared case-insensitively - the casing is the client's choice, not ours.
TRANSPORT_HEADERS = frozenset({"transfer-encoding"})


class Request(WerkzeugRequest):
    #: True when this request was received by a real WSGI server (moto_server),
    #: which supplies its own Date header.  False for the in-process mocks and
    #: the proxy, where moto is the entire stack and has to supply it itself.
    from_wsgi_server = False

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Assigned rather than declared as a class attribute: flask.Request
        # exposes this as a config-backed property, and a class attribute here
        # would shadow it for BackendRequest, which inherits from both.
        self.max_form_memory_size = MAX_FORM_MEMORY_SIZE

    @classmethod
    def from_primitives(
        cls, method: str, url: str, headers: Any, body: Any = None
    ) -> Request:
        """Build a request out of the parts an HTTP message is made of."""
        if isinstance(body, AwsChunkedWrapper):
            body = body.read()
        parsed_url = urlparse(url)
        request = cast(
            Request,
            cls.from_values(
                method=method,
                base_url=f"{parsed_url.scheme}://{parsed_url.netloc}",
                path=parsed_url.path,
                query_string=parsed_url.query,
                data=body if body is not None else b"",
                headers=[
                    (key, value.decode("utf-8") if isinstance(value, bytes) else value)
                    for key, value in headers.items()
                    if key.lower() not in TRANSPORT_HEADERS
                ],
            ),
        )
        # werkzeug's EnvironBuilder discards a `Content-Length: 0` header instead
        # of writing it to the environ, so a bodiless request would arrive without
        # any Content-Length at all - unlike the same request over a real WSGI
        # server, where the client's header is preserved.  S3 returns 411 when
        # that header is missing (see _bucket_response_put/_bucket_response_post).
        request.environ.setdefault("CONTENT_LENGTH", "0")
        return request

    @property
    def raw_path(self) -> str:
        """The path as it arrived, before werkzeug percent-decoded it.

        S3 keys routinely contain characters - an encoded slash, most awkwardly -
        that `path` decodes away, and moto matches backend URLs against the
        encoded form.
        """
        # RAW_URI holds either a path or a full URL, depending on the server.  A
        # path may begin with a double slash, which urlparse would read as the
        # start of a netloc, so only parse when there is really a scheme to strip.
        raw_uri: str = self.environ.get("RAW_URI", "") or self.path
        parsed = urlparse(raw_uri)
        raw_path = parsed.path if parsed.scheme else raw_uri.split("?", 1)[0]
        if not raw_path:
            return "/"
        return raw_path if raw_path.startswith("/") else f"/{raw_path}"

    @property
    def raw_url(self) -> str:
        """The full URL as it arrived.  See `raw_path`."""
        raw_url = f"{self.url_root.rstrip('/')}{self.raw_path}"
        if self.query_string:
            raw_url += f"?{self.query_string.decode()}"
        return raw_url


def normalize_request(
    request: AWSPreparedRequest | WerkzeugRequest | Request,
) -> Request:
    """Turn however this request reached us into the one type the core acts on."""
    if isinstance(request, LocalProxy):
        # Flask hands out a proxy bound to the active request context.  It only
        # looks like a Request because LocalProxy forwards __class__, and it goes
        # unbound the moment that context ends - unwrap it so callers get the
        # real object.
        request = request._get_current_object()
    if isinstance(request, Request):
        return request
    if isinstance(request, WerkzeugRequest):
        return Request(request.environ.copy())
    # Anything else is a prepared request from botocore or from `responses`
    return Request.from_primitives(
        request.method, request.url, request.headers, request.body
    )


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
