# TODO:
# RuleFactory for each protocol
# Loop through service.protocols and add rules for each one
# The localstack RequiredArgsRule can probably be done better with a Constraint(s)
# Find a way to subclass mapadapter to automatically do match plus constraint
#
# The nice thing about this module is that none of the werkzeug stuff should leak outside of it
# In other words, however it does the routing, it's only output is the Action and Arguments
#
# If we need to pass args to a Rule subclass, see here for how to work with rule.empty()
# https://github.com/pallets/werkzeug/blob/11c9fe9272e281b90abe89dc59f86e44ee453bab/src/werkzeug/routing/rules.py#L528
#
# Moto has MOTO_S3_CUSTOM_ENDPOINTS and S3_IGNORE_SUBDOMAIN_BUCKETNAME
# Both of which are going to cause routing problems
# Maybe we keep uses url_bases in urls.py but just get rid of all the url_paths?

from __future__ import annotations

import functools
import re
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, cast
from urllib.parse import unquote

from werkzeug.exceptions import MethodNotAllowed, NotFound
from werkzeug.routing import BaseConverter, Map, MapAdapter, Rule

from moto import settings
from moto.core.model import OperationModel, ServiceModel, StructureShape
from moto.core.request import Request, determine_request_protocol
from moto.core.utils import get_service_model

URI_LABEL_REGEX = re.compile(r"({.+?})")
"""https://smithy.io/2.0/spec/http-bindings.html#labels"""

URI_LABEL_TO_RULE_VAR_REPLACEMENTS = {"-": "_HYPHEN_"}
URI_LABEL_TO_RULE_VAR_TRANSLATION_TABLE = str.maketrans(
    URI_LABEL_TO_RULE_VAR_REPLACEMENTS
)


class GreedyLabelConverter(BaseConverter):
    """Like Werkzeug's PathConverter, but also matches leading slashes.

    Greedy labels must still match at least one character, otherwise
    e.g. `PUT /bucket/` would match `/{Bucket}/{Key+}` with an empty Key.
    """

    NAME = "greedy_label"

    regex = ".+?"
    weight = 200
    part_isolating = False


class ImplicitGreedyLabelConverter(GreedyLabelConverter):
    """Converter for a last label that isn't modeled as greedy (see `to_werkzeug_rule_string`).

    Unlike explicit greedy labels (e.g. an S3 key like `folder/`), these labels
    can't end in a slash, so e.g. `PUT /bucket/` still matches `/{Bucket}`
    with `Bucket=bucket` (via the trailing slash fallback in the router).
    """

    NAME = "implicit_greedy_label"

    regex = ".*?[^/]"


def to_werkzeug_rule_string(smithy_uri: str) -> str:
    """Converts a Smithy uri into a Werkzeug rule string.

    The last label in a uri is always treated as greedy, even without a `+`.
    Some services model labels that can contain slashes (e.g. ARNs) as
    non-greedy, but clients don't always encode those slashes.

    Examples:
        "/resources/{resource_id}/methods/{http_method}" -> "/resources/<resource_id>/methods/<implicit_greedy_label:http_method>"
        "/v20180820/tags/{resourceArn+}" -> "/v20180820/tags/<greedy_label:resourceArn>"
    """

    def to_rule_variable(match: re.Match[str]) -> str:
        """Transform uri label into a rule variable placeholder."""
        uri_label: str = match.group(0)
        # Strip curly braces.
        uri_label = uri_label[1:-1]
        # Add converter prefix for greedy labels.
        # NOTE: Instead of defaulting the last label to greedy, we could add
        # a `+` to the affected labels in the service-2.moto-extras.json files.
        # That is more precise, but has to be done for each operation as needed.
        prefix = ""
        is_last_label = match.end() == len(smithy_uri.rstrip("/"))
        if uri_label.endswith("+"):
            uri_label = uri_label.rstrip("+")
            prefix = f"{GreedyLabelConverter.NAME}:"
        elif is_last_label:
            prefix = f"{ImplicitGreedyLabelConverter.NAME}:"
        variable_name = uri_label.translate(URI_LABEL_TO_RULE_VAR_TRANSLATION_TABLE)
        rule_variable = f"<{prefix}{variable_name}>"
        return rule_variable

    rule_string = URI_LABEL_REGEX.sub(to_rule_variable, smithy_uri)
    return rule_string


def add_s3_subdomain() -> bool:
    if settings.S3_IGNORE_SUBDOMAIN_BUCKETNAME or settings.get_s3_custom_endpoints():
        return False
    return True


@dataclass
class ActionConstraintContext:
    request: Request


class ActionConstraint:
    def accept(self, context: ActionConstraintContext) -> bool:
        raise NotImplementedError


class RequiredArg(ActionConstraint):
    """Requires a query string argument (REST protocols).

    Only the query string is checked, so matching never parses the request body.
    """

    def __init__(self, name: str, value: str | None = None):
        self.name = name
        self.value = value

    def accept(self, context: ActionConstraintContext) -> bool:
        values = context.request.args
        if self.name not in values:
            return False
        if self.value is not None:
            return values.get(self.name) == self.value
        return True


class RequiredHeader(ActionConstraint):
    def __init__(self, name: str, value: str | None = None):
        self.name = name
        self.value = value

    def accept(self, context: ActionConstraintContext) -> bool:
        headers = context.request.headers
        if self.name not in headers:
            return False
        if self.value is not None:
            return headers[self.name] == self.value
        return True


class ActionCandidate:
    def __init__(
        self,
        operation: OperationModel,
        constraints: list[ActionConstraint] | None = None,
    ):
        self.operation = operation
        self.constraints = constraints or []

    def add_constraint(self, constraint: ActionConstraint) -> None:
        self.constraints.append(constraint)

    @property
    def weight(self) -> int:
        weight = 10 * len(self.constraints)
        if self.operation.deprecated:
            weight -= 5
        return weight


class ActionSelector:
    def __init__(self, candidates: list[ActionCandidate]):
        self.candidates = sorted(
            candidates, key=lambda candidate: candidate.weight, reverse=True
        )

    def select_action(self, request: Request) -> OperationModel | None:
        context = ActionConstraintContext(request)
        for candidate in self.candidates:
            if all(constraint.accept(context) for constraint in candidate.constraints):
                return candidate.operation
        return None


class SmithyRule(Rule):
    """
    A Werkzeug Rule extension which initially acts as a normal rule (i.e. matches a path and method).

    This rule matches if one of its sub-rules _might_ match.
    It cannot be assumed that one of the fine-grained rules matches, just because this rule initially matches.
    If this rule matches, the caller _must_ call `match_request` in order to find the actual fine-grained matching rule.
    The result of `match_request` is only meaningful if this wrapping rule also matches.
    """

    def __init__(
        self,
        string: str,
        operations: list[OperationModel],
        methods: list[str],
        **kwargs: Any,
    ) -> None:
        super().__init__(string, methods=methods, **kwargs)
        self.candidates = self._initialize_candidates(operations)
        # From Werkzeug: "If GET is present [...] and HEAD is not, HEAD is added automatically."
        # This behavior is undesirable for our use-case, so we reverse it.
        if self.methods and "HEAD" in self.methods and "HEAD" not in methods:
            self.methods = {method.upper() for method in methods}

    def match_request(self, request: Request) -> OperationModel | None:
        action_selector = ActionSelector(self.candidates)
        value = action_selector.select_action(request)
        return value

    @staticmethod
    def _initialize_candidates(
        operations: list[OperationModel],
    ) -> list[ActionCandidate]:
        candidates = []
        for operation in operations:
            candidate = ActionCandidate(operation)
            for name, value in operation.http_trait.query_args.items():
                candidate.add_constraint(RequiredArg(name, value))
            input_shape = operation.input_shape
            if isinstance(input_shape, StructureShape):
                for query_arg in input_shape.required_query_args:
                    if query_arg in operation.http_trait.query_args:
                        continue  # Already constrained, possibly with a value.
                    candidate.add_constraint(RequiredArg(query_arg))
                for header_name in input_shape.required_headers:
                    candidate.add_constraint(RequiredHeader(header_name))
            candidates.append(candidate)
        return candidates


def to_uri_params(arguments: Mapping[str, Any]) -> dict[str, Any]:
    """Post-process Werkzeug mapped arguments back to Smithy label names and percent-decoded values."""

    def to_label_name(arg: str) -> str:
        label_name = arg
        for original, substitution in URI_LABEL_TO_RULE_VAR_REPLACEMENTS.items():
            label_name = label_name.replace(substitution, original)
        return label_name

    uri_params = {
        to_label_name(name): unquote(value) for name, value in arguments.items()
    }
    return uri_params


@functools.lru_cache
def _host_prefix_regex(host_prefix: str, endpoint_prefix: str) -> re.Pattern[str]:
    """Builds a regex that extracts the labels of a host prefix from a request host.

    Example:
        ("{AccountId}.", "s3-control") -> ^(?P<AccountId>[^.]+)\\.s3\\-control\\.
    """
    parts = []
    for part in URI_LABEL_REGEX.split(host_prefix):
        if URI_LABEL_REGEX.fullmatch(part):
            parts.append(f"(?P<{part[1:-1]}>[^.]+)")
        else:
            parts.append(re.escape(part))
    # Require the endpoint prefix after the host prefix, so that hosts which don't
    # have the host prefix (e.g. localhost or 127.0.0.1 in server mode) don't match.
    return re.compile(f"^{''.join(parts)}{re.escape(endpoint_prefix)}\\.", re.I)


def get_host_labels(operation: OperationModel, host: str) -> dict[str, str]:
    """Extracts host label values (https://smithy.io/2.0/spec/endpoint-traits.html#hostlabel-trait)
    from the request host, if the host has them.

    Example:
        "{AccountId}." + "123456789012.s3-control.us-east-1.amazonaws.com" -> {"AccountId": "123456789012"}
        "{AccountId}." + "localhost:5000" -> {}
    """
    # botocore's stubs type `endpoint` as a str, but it's the endpoint trait dict.
    endpoint = cast(dict[str, str], operation.endpoint or {})
    host_prefix = endpoint.get("hostPrefix", "")
    endpoint_prefix = operation.service_model.endpoint_prefix
    if "{" not in host_prefix or not endpoint_prefix:
        return {}
    match = _host_prefix_regex(host_prefix, endpoint_prefix).match(host)
    return match.groupdict() if match else {}


def _create_map(rules: list[Rule]) -> Map:
    return Map(
        rules=rules,
        # don't be strict about trailing slashes when matching
        strict_slashes=False,
        # we can't really use werkzeug's merge-slashes since it uses HTTP redirects to solve it
        merge_slashes=False,
        converters={
            GreedyLabelConverter.NAME: GreedyLabelConverter,
            ImplicitGreedyLabelConverter.NAME: ImplicitGreedyLabelConverter,
        },
        default_subdomain="",
    )


def _get_path_info_for_matching(request: Request) -> str:
    path_info = request.raw_path
    # Werkzeug's MapAdapter.match strips leading slashes, which loses the leading
    # slash of an S3 key in a virtual-hosted request (e.g. bucket.s3.amazonaws.com//key),
    # so encode it to keep it in the matched label.
    if path_info.startswith("//"):
        path_info = "/%2F" + path_info[2:]
    return path_info


def _match_path(
    matcher: MapAdapter, path_info: str, method: str
) -> tuple[Rule, Mapping[str, Any]]:
    """Match the path as-is, falling back to the path without trailing slashes.

    Trailing slashes are optional in Smithy matching, but they can also be
    significant, e.g. an S3 key like `folder/`, so they are only stripped if
    the path doesn't match otherwise:
    https://smithy.io/2.0/spec/http-bindings.html#literal-character-sequences
    """
    try:
        return matcher.match(path_info, method, return_rule=True)  # type: ignore[return-value]
    except (NotFound, MethodNotAllowed):
        stripped = path_info.rstrip("/") or "/"
        if stripped == path_info:
            raise
        return matcher.match(stripped, method, return_rule=True)  # type: ignore[return-value]


class ProtocolRouter:
    """Routes a request to an operation of a service, for a specific protocol.

    Subclasses implement `_match`; host labels are added to the params for all protocols.
    """

    def __init__(self, service_model: ServiceModel):
        self.service_model = service_model

    def match(self, request: Request) -> tuple[OperationModel, dict[str, Any]]:
        operation, params = self._match(request)
        if operation is None:
            raise NotFound()
        # Host labels are included with the uri params, but not used for matching,
        # since requests don't always arrive on the prefixed host (e.g. in server mode).
        params.update(get_host_labels(operation, request.host))
        return operation, params

    def _match(self, request: Request) -> tuple[OperationModel | None, dict[str, Any]]:
        raise NotImplementedError


class RPCRouter(ProtocolRouter):
    """Base router for the Query/EC2 and JSON protocols.

    The request path doesn't identify the operation in these protocols, but it can
    identify a resource.  Subclasses can set PATH_TEMPLATES to extract those
    identifiers into the params.
    """

    PATH_TEMPLATES: list[str] = []

    def __init__(self, service_model: ServiceModel):
        super().__init__(service_model)
        self._path_map = (
            _create_map([Rule(to_werkzeug_rule_string(t)) for t in self.PATH_TEMPLATES])
            if self.PATH_TEMPLATES
            else None
        )

    def _get_path_params(self, request: Request) -> dict[str, Any]:
        if self._path_map is None:
            return {}
        matcher = self._path_map.bind(request.host)
        path_info = _get_path_info_for_matching(request)
        try:
            _, arguments = _match_path(matcher, path_info, request.method)
        except (NotFound, MethodNotAllowed):
            return {}
        return to_uri_params(arguments)


class QueryRouter(RPCRouter):
    """Routes Query/EC2 protocol requests by their `Action` parameter."""

    def __init__(self, service_model: ServiceModel):
        super().__init__(service_model)
        self._operations_by_name = {
            name: service_model.operation_model(name)
            for name in service_model.operation_names
        }

    def _match(self, request: Request) -> tuple[OperationModel | None, dict[str, Any]]:
        operation = self._operations_by_name.get(request.values.get("Action", ""))
        return operation, self._get_path_params(request)


class JSONRouter(RPCRouter):
    """Routes JSON protocol requests by their `X-Amz-Target` header."""

    def __init__(self, service_model: ServiceModel):
        super().__init__(service_model)
        target_prefix = service_model.metadata.get("targetPrefix")
        self._operations_by_target = (
            {
                f"{target_prefix}.{name}": service_model.operation_model(name)
                for name in service_model.operation_names
            }
            if target_prefix
            else {}
        )

    def _match(self, request: Request) -> tuple[OperationModel | None, dict[str, Any]]:
        target = request.headers.get("X-Amz-Target", "")
        operation = self._operations_by_target.get(target)
        return operation, self._get_path_params(request)


class RestRouter(ProtocolRouter):
    """Routes REST protocol (rest-json, rest-xml) requests by their path and method,
    using a Werkzeug Map, then by their query string and headers (see SmithyRule).
    """

    def __init__(self, service_model: ServiceModel):
        super().__init__(service_model)
        # group all operations by their path and method
        path_index: dict[tuple[str, str], list[OperationModel]] = defaultdict(list)
        for name in service_model.operation_names:
            operation = service_model.operation_model(name)
            http_trait = operation.http_trait
            path_index[(http_trait.path, http_trait.method)].append(operation)
        rules = []
        for (path, method), operations in path_index.items():
            rules.extend(self._create_rules(path, method, operations))
        self._map = _create_map(rules)

    def _create_rules(
        self, path: str, method: str, operations: list[OperationModel]
    ) -> list[Rule]:
        """Creates the rules for the operations with the same path and method.

        Host prefixes (e.g. "{AccountId}.") aren't used for matching, since
        requests don't always arrive on those hosts (e.g. in server mode).
        Host labels are extracted after matching; see get_host_labels().
        """
        rule_string = to_werkzeug_rule_string(path)
        return [SmithyRule(string=rule_string, operations=operations, methods=[method])]

    def _get_subdomain(self, request: Request) -> str | None:
        return None

    def _match(self, request: Request) -> tuple[OperationModel | None, dict[str, Any]]:
        matcher = self._map.bind(request.host, subdomain=self._get_subdomain(request))
        path_info = _get_path_info_for_matching(request)
        try:
            rule, arguments = _match_path(matcher, path_info, request.method)
        except MethodNotAllowed as e:
            raise NotFound() from e
        assert isinstance(rule, SmithyRule)
        return rule.match_request(request), to_uri_params(arguments)


class S3RestRouter(RestRouter):
    """Also routes virtual-hosted-style requests, where the bucket is the subdomain."""

    def _create_rules(
        self, path: str, method: str, operations: list[OperationModel]
    ) -> list[Rule]:
        rules = super()._create_rules(path, method, operations)
        if path.startswith("/{Bucket}") and add_s3_subdomain():
            bucket_path = path.replace("/{Bucket}", "", 1) or "/"
            rules.append(
                SmithyRule(
                    string=to_werkzeug_rule_string(bucket_path),
                    operations=operations,
                    methods=[method],
                    subdomain="<Bucket>",
                )
            )
        return rules

    def _get_subdomain(self, request: Request) -> str | None:
        # Imported here to avoid a circular import.
        from moto.s3.responses import S3Response

        if S3Response.subdomain_based_buckets(request):
            return request.host.split(".", 1)[0]
        return None


# SQS requests can be sent to the queue url, which identifies the queue:
# https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-queue-message-identifiers.html
SQS_QUEUE_URL_PATH_TEMPLATES = ["/{AccountId}/{QueueName}"]


class SQSQueryRouter(QueryRouter):
    PATH_TEMPLATES = SQS_QUEUE_URL_PATH_TEMPLATES


class SQSJSONRouter(JSONRouter):
    PATH_TEMPLATES = SQS_QUEUE_URL_PATH_TEMPLATES


PROTOCOL_ROUTERS: dict[str, type[ProtocolRouter]] = {
    "ec2": QueryRouter,
    "json": JSONRouter,
    "query": QueryRouter,
    "rest-json": RestRouter,
    "rest-xml": RestRouter,
}
SERVICE_SPECIFIC_ROUTERS: dict[str, dict[str, type[ProtocolRouter]]] = {
    "s3": {"rest-xml": S3RestRouter},
    "sqs": {"json": SQSJSONRouter, "query": SQSQueryRouter},
}


def get_router_class(service_name: str, protocol: str) -> type[ProtocolRouter] | None:
    if service_name in SERVICE_SPECIFIC_ROUTERS:
        if protocol in SERVICE_SPECIFIC_ROUTERS[service_name]:
            return SERVICE_SPECIFIC_ROUTERS[service_name][protocol]
    return PROTOCOL_ROUTERS.get(protocol)


class ServiceOperationRouter:
    """
    A router implementation which abstracts the routing of incoming HTTP requests to a specific
    operation within a service.

    Requests are routed by a ProtocolRouter for the request's protocol, which is created
    the first time a request for that protocol is routed.
    """

    def __init__(self, service: ServiceModel):
        self._service_model = service
        self._routers: dict[str, ProtocolRouter] = {}

    def match(self, request: Request) -> tuple[OperationModel, dict[str, Any]]:
        """
        Matches the given request to the operation it targets (or raises an exception if no operation matches).

        :param request: The request of which the targeting operation needs to be found
        :return: A tuple with the matched operation and the (already parsed) path params
        :raises: Werkzeug's NotFound exception in case the given request does not match any operation
        """
        protocol = determine_request_protocol(self._service_model, request.content_type)
        return self._get_router(protocol).match(request)

    def _get_router(self, protocol: str) -> ProtocolRouter:
        if protocol not in self._routers:
            router_cls = get_router_class(self._service_model.service_name, protocol)
            if router_cls is None:
                # e.g. smithy-rpc-v2-cbor, which isn't supported yet.
                raise NotFound()
            self._routers[protocol] = router_cls(self._service_model)
        return self._routers[protocol]


@functools.cache
def get_service_router(service_name: str) -> ServiceOperationRouter:
    service_model = get_service_model(service_name)
    service_router = ServiceOperationRouter(service_model)
    return service_router
