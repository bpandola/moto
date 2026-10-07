"""Prototype server-side pagination driven by botocore paginator configs.

The client-side paginator in ``botocore/paginate.py`` consumes pages using the
``paginators-1.json`` config for an operation.  This module runs the same
config in reverse: given the *full* (unpaginated) response for an operation and
the incoming request parameters, it produces a single page that the client
paginator will consume correctly.

Server-only behaviour is described by extra keys (see ``SERVER_KEYS``).  Most
of them can be derived from the client config or the service model, so an
operation usually needs little or no extra config.

Example::

    config = ServerPaginationConfig.from_botocore(
        'ListObjects', s3_paginators['ListObjects'],
        server_config={'result_key_attributes': {
            'Contents': 'Key', 'CommonPrefixes': 'Prefix'}},
    )
    page = ServerPaginator(config).paginate(full_response, request_params)
"""

import base64
import bisect
import copy
import hashlib
import hmac
import json
import re
import time
from typing import Any, Optional

import jmespath
from botocore.utils import set_value_from_jmespath

from moto.core.utils import get_value

from .exceptions import ServiceException

# Server-side keys that may be added to a pagination config, either inline in
# paginators-1.json or in a separate file merged in like sdk-extras.
SERVER_KEYS = {
    "default_page_size",
    "max_page_size",
    "min_page_size",
    "page_size_policy",
    "token_strategy",
    "token_attributes",
    "token_position",
    "marker_match",
    "bound_params",
    "token_ttl",
    "multi_result_mode",
    "result_key_attributes",
    "count_keys",
    "echo_params",
    "invalid_token_error",
    "invalid_page_size_error",
}

_SIMPLE_PATH = re.compile(r"^[A-Za-z_][\w]*(\.[A-Za-z_][\w]*)*$")
# e.g. "Contents[-1].Key" or "StreamDescription.Shards[-1].ShardId"
_LAST_ITEM_PATH = re.compile(
    r"^(?P<list>[A-Za-z_][\w.]*)\[-1\]\.(?P<attr>[A-Za-z_][\w]*)$"
)


class PaginationServerError(ServiceException):
    """A client error to return to the caller (e.g. as an HTTP 400)."""

    pass


class OutputTokenSpec:
    """How to emit one output token, parsed from its JMESPath expression.

    ``write_path`` is the response field to set, or None when the client
    computes the token itself from the page (e.g. ``Contents[-1].Key``).
    ``derived_attribute`` is the item attribute the token comes from when the
    expression names one, which also tells us the token is a marker.
    """

    def __init__(self, expression: str):
        self.expression = expression
        self.write_path: Optional[str] = None
        self.derived_attribute: Optional[str] = None
        for part in (p.strip() for p in expression.split("||")):
            last_item = _LAST_ITEM_PATH.match(part)
            if last_item:
                self.derived_attribute = last_item.group("attr")
            elif _SIMPLE_PATH.match(part) and self.write_path is None:
                self.write_path = part
            elif not _SIMPLE_PATH.match(part):
                raise ValueError(f"Unsupported output_token expression: {expression}")


class ServerPaginationConfig:
    def __init__(
        self,
        operation_name: str,
        input_token: Any,
        output_token: Any,
        result_key: Any,
        limit_key: Optional[str] = None,
        more_results: Optional[str] = None,
        non_aggregate_keys: Optional[list[str]] = None,
        default_page_size: Optional[int] = None,
        # Dupe of above for current configs
        limit_default: Optional[int] = None,
        max_page_size: Optional[int] = None,
        min_page_size: int = 1,
        page_size_policy: str = "clamp",
        token_strategy: Optional[str] = None,
        token_attributes: Optional[list[Any]] = None,
        token_position: str = "after_last",
        marker_match: str = "ordered",
        bound_params: Optional[list[str]] = None,
        token_ttl: Optional[int] = None,
        multi_result_mode: str = "primary",
        result_key_attributes: Optional[Any] = None,
        # Dupe of above for current configs
        unique_attribute: Optional[Any] = None,
        count_keys: Optional[dict[str, str]] = None,
        echo_params: Optional[list[str]] = None,
        invalid_token_error: Optional[dict[str, str]] = None,
        invalid_page_size_error: Optional[dict[str, str]] = None,
        service_name: Optional[str] = None,
    ):
        self.operation_name = operation_name
        self.service_name = service_name
        self.input_tokens = _as_list(input_token)
        self.output_tokens = [OutputTokenSpec(e) for e in _as_list(output_token)]
        if len(self.input_tokens) != len(self.output_tokens):
            raise ValueError("input_token and output_token lengths differ")
        self.result_keys = _as_list(result_key)
        self.limit_key = limit_key
        self.more_results = more_results
        self.non_aggregate_keys = non_aggregate_keys or []

        self.max_page_size = max_page_size
        self.min_page_size = min_page_size
        self.default_page_size = (
            default_page_size or limit_default or max_page_size or 100
        )
        self.page_size_policy = page_size_policy

        derived = [t.derived_attribute for t in self.output_tokens]
        self.token_strategy = token_strategy or ("marker" if any(derived) else "opaque")
        self.token_attributes = token_attributes or derived
        self.token_position = token_position
        self.marker_match = marker_match
        self.bound_params = bound_params or []
        self.token_ttl = token_ttl
        self.multi_result_mode = multi_result_mode
        if isinstance(unique_attribute, str):
            unique_attribute = [unique_attribute]
        self.result_key_attributes = result_key_attributes or unique_attribute or {}
        # Scalar result keys that hold per-page counts (e.g. DynamoDB Count).
        self.count_keys = count_keys or {}
        self.echo_params = echo_params or []
        self.invalid_token_error = invalid_token_error or {
            "code": "InvalidNextTokenException",
            "message": "The pagination token is invalid.",
        }
        self.invalid_page_size_error = invalid_page_size_error or {
            "code": "ValidationException",
            "message": "Invalid value for {limit_key}: {value}",
        }
        if self.token_strategy == "marker" and not all(self.token_attributes):
            raise ValueError(f"{operation_name}: marker tokens need token_attributes")

    @classmethod
    def from_botocore(
        cls,
        operation_name: str,
        client_config: dict[str, Any],
        server_config: Optional[dict[str, Any]] = None,
        operation_model: Any = None,
    ) -> "ServerPaginationConfig":
        """Build a config from a paginators-1.json entry.

        ``server_config`` holds any server-only keys.  When an
        ``OperationModel`` is given, page size bounds and the invalid token
        error code are derived from it unless set explicitly.
        """
        kwargs = {
            k: v
            for k, v in client_config.items()
            if k
            in {
                "input_token",
                "output_token",
                "result_key",
                "limit_key",
                "more_results",
                "non_aggregate_keys",
            }
        }
        if operation_model is not None:
            kwargs.update(_derive_from_model(client_config, operation_model))
        kwargs.update(server_config or {})
        # unknown = set(server_config or {}) - SERVER_KEYS
        # if unknown:
        #    raise ValueError(f'Unknown server pagination keys: {unknown}')
        return cls(operation_name, **kwargs)


def _derive_from_model(
    client_config: dict[str, Any], operation_model: Any
) -> dict[str, Any]:
    derived: dict[str, Any] = {}
    input_shape = operation_model.input_shape
    limit_key = client_config.get("limit_key")
    if input_shape is not None and limit_key in input_shape.members:
        metadata = input_shape.members[limit_key].metadata
        if "max" in metadata:
            derived["max_page_size"] = metadata["max"]
        if "min" in metadata:
            derived["min_page_size"] = metadata["min"]
    for error_shape in operation_model.error_shapes:
        name = error_shape.name.lower()
        if ("token" in name and "auth" not in name) or "paginat" in name:
            derived["invalid_token_error"] = {
                "code": error_shape.name,
                "message": "The pagination token is invalid.",
            }
            break
    derived["service_name"] = operation_model.service_model.service_name
    return derived


class OpaqueTokenCodec:
    """Encodes a position as a signed, base64 JSON token.

    The token stores the offset and a fingerprint of the ``bound_params``, so
    a token cannot be replayed with different filters.
    """

    def __init__(self, config: ServerPaginationConfig, secret: bytes = b"change-me"):
        self._config = config
        self._secret = secret

    def encode(self, offset: int, params: dict[str, Any]) -> str:
        body: dict[str, Any] = {"o": offset, "f": self._fingerprint(params)}
        if self._config.token_ttl:
            body["e"] = int(time.time()) + self._config.token_ttl
        raw = json.dumps(body, separators=(",", ":"), sort_keys=True).encode()
        sig = hmac.new(self._secret, raw, hashlib.sha256).digest()[:16]
        return base64.urlsafe_b64encode(raw + sig).decode().rstrip("=")

    def decode(self, token: str, params: dict[str, Any]) -> int:
        try:
            data = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4))
            raw, sig = data[:-16], data[-16:]
            expected = hmac.new(self._secret, raw, hashlib.sha256).digest()
            if not hmac.compare_digest(sig, expected[:16]):
                raise ValueError("bad signature")
            body = json.loads(raw)
        except (ValueError, TypeError):
            raise self.invalid_token()
        if body.get("f") != self._fingerprint(params):
            raise self.invalid_token()
        if "e" in body and body["e"] < time.time():
            raise self.invalid_token()
        return body["o"]

    def _fingerprint(self, params: dict[str, Any]) -> str:
        bound = {k: params.get(k) for k in self._config.bound_params}
        raw = json.dumps(bound, sort_keys=True, default=str).encode()
        return hashlib.sha256(raw).hexdigest()[:16]

    def invalid_token(self) -> PaginationServerError:
        error = self._config.invalid_token_error
        exc = PaginationServerError(error["code"], error["message"])
        exc.__module__ = self._config.service_name  # type: ignore[assignment]
        return exc


class ServerPaginator:
    def __init__(
        self, config: ServerPaginationConfig, token_secret: bytes = b"change-me"
    ):
        self._config = config
        self._codec = OpaqueTokenCodec(config, token_secret)

    def paginate(self, full_response: Any, params: dict[str, Any]) -> dict[str, Any]:
        """Return one page of ``full_response`` for the request ``params``.

        ``full_response`` is the complete response dict, with every result at
        its ``result_key`` path and any non-aggregate fields filled in.
        """
        config = self._config
        page_size = self._resolve_page_size(params)
        entries = self._collect_entries(full_response)
        start = self._resolve_start(entries, params)
        window = entries[start : start + page_size]
        truncated = start + page_size < len(entries)

        page = copy.deepcopy(full_response)
        self._write_results(page, full_response, window, start, page_size)
        for spec in config.output_tokens:
            if spec.write_path:
                _delete_path(page, spec.write_path)
        if truncated:
            self._write_tokens(page, entries, start, len(window), params)
        if config.more_results:
            set_value_from_jmespath(page, config.more_results, truncated)
        for name in config.echo_params:
            if name in params:
                page[name] = params[name]
        return page

    def _resolve_page_size(self, params: dict[str, Any]) -> int:
        config = self._config
        if not config.limit_key or params.get(config.limit_key) is None:
            return config.default_page_size
        # Some limit keys are modeled as strings (e.g. Route 53 MaxItems).
        value = int(params[config.limit_key])
        too_small = value < config.min_page_size
        too_large = config.max_page_size and value > config.max_page_size
        if too_small or too_large:
            if config.page_size_policy == "error":
                error = config.invalid_page_size_error
                raise PaginationServerError(
                    error["code"],
                    error["message"].format(limit_key=config.limit_key, value=value),
                )
            if too_small:
                return config.min_page_size
            return config.max_page_size  # type: ignore[return-value]
        return value

    def _collect_entries(self, full_response: dict[str, Any]) -> list[Any]:
        """Flatten the result lists into ``(sort_key, result_key, item)``.

        In ``merged`` mode all result lists share one window, ordered by their
        ``result_key_attributes`` (S3 ListObjects counts Contents and
        CommonPrefixes together against MaxKeys).  Otherwise only the primary
        result key is paginated.
        """
        config = self._config
        keys = config.result_keys
        if config.multi_result_mode == "merged":
            entries: list[Any] = []
            for key in keys:
                attr = config.result_key_attributes[key]
                for item in jmespath.search(key, full_response) or []:
                    entries.append(((item[attr],), key, item))
            entries.sort(key=lambda entry: entry[0])
            return entries
        primary = [k for k in keys if k not in config.count_keys][0]
        items = jmespath.search(primary, full_response) or []
        entries = [(self._marker_key(item), primary, item) for item in items]
        if config.token_strategy == "marker" and config.marker_match == "ordered":
            # Ordered markers are compared by value, so the items must be
            # sorted by the token attributes.
            entries.sort(key=lambda entry: entry[0])
        return entries

    def _marker_key(self, item: Any) -> Optional[tuple[Any, ...]]:
        if self._config.token_strategy != "marker":
            return None
        return tuple(
            _sortable(_project(item, attr)) for attr in self._config.token_attributes
        )

    def _resolve_start(self, entries: list[Any], params: dict[str, Any]) -> int:
        config = self._config
        values: list[Any] = [params.get(name) for name in config.input_tokens]
        if all(v in (None, "") for v in values):
            return 0
        if config.token_strategy == "opaque":
            offset = self._codec.decode(values[0], params)
            if not 0 <= offset <= len(entries):
                raise self._codec.invalid_token()
            return offset
        marker = tuple(_sortable(v) for v in values)
        keys = [entry[0] for entry in entries]
        if config.marker_match == "exact":
            try:
                index = keys.index(marker)
            except ValueError:
                raise self._codec.invalid_token()
            return index + 1 if config.token_position == "after_last" else index
        if config.token_position == "after_last":
            return bisect.bisect_right(keys, marker)
        return bisect.bisect_left(keys, marker)

    def _write_results(
        self,
        page: dict[str, Any],
        full_response: dict[str, Any],
        window: list[Any],
        start: int,
        page_size: int,
    ) -> None:
        config = self._config
        for key in config.result_keys:
            if key in config.count_keys:
                continue
            if config.multi_result_mode == "parallel":
                items = jmespath.search(key, full_response) or []
                value = items[start : start + page_size]
            else:
                value = [item for _, k, item in window if k == key]
            if not value and jmespath.search(key, full_response) is None:
                continue
            set_value_from_jmespath(page, key, value)
        for key, counted in config.count_keys.items():
            set_value_from_jmespath(
                page, key, len(jmespath.search(counted, page) or [])
            )

    def _write_tokens(
        self,
        page: dict[str, Any],
        entries: list[Any],
        start: int,
        page_len: int,
        params: dict[str, Any],
    ) -> None:
        config = self._config
        if config.token_strategy == "opaque":
            values = [self._codec.encode(start + page_len, params)]
        else:
            index = start + page_len
            if config.token_position == "after_last":
                index -= 1
            item = entries[index][2]
            if config.multi_result_mode == "merged":
                values = [entries[index][0][0]]
            else:
                values = [_project(item, attr) for attr in config.token_attributes]
        for spec, value in zip(config.output_tokens, values, strict=False):
            if spec.write_path and value is not None:
                set_value_from_jmespath(page, spec.write_path, value)


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _project(item: Any, attr: Any) -> Any:
    # A list of attributes produces a map token, e.g. DynamoDB's
    # LastEvaluatedKey built from the table's key schema.
    if isinstance(attr, list):
        return {name: get_value(item, name, None) for name in attr}
    return get_value(item, attr, None)


def _sortable(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True, default=str)
    return value


def _delete_path(source: Any, path: str) -> None:
    *parents, leaf = path.split(".")
    for part in parents:
        source = source.get(part)
        if not isinstance(source, dict):
            return
    source.pop(leaf, None)
