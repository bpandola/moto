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
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote

from werkzeug.datastructures import MultiDict
from werkzeug.exceptions import MethodNotAllowed, NotFound
from werkzeug.routing import BaseConverter, Map, MapAdapter, Rule

from moto import settings
from moto.core.model import OperationModel, ServiceModel, StructureShape
from moto.core.request import Request, determine_request_protocol
from moto.core.utils import get_service_model

if TYPE_CHECKING:
    from moto.s3.responses import S3Response

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

    def _get_values(self, context: ActionConstraintContext) -> MultiDict[str, str]:
        return context.request.args

    def accept(self, context: ActionConstraintContext) -> bool:
        values = self._get_values(context)
        if self.name not in values:
            return False
        if self.value is not None:
            return values.get(self.name) == self.value
        return True


class RequiredParam(RequiredArg):
    """Requires a query string or form body parameter (Query/EC2 protocols)."""

    def _get_values(self, context: ActionConstraintContext) -> MultiDict[str, str]:
        return context.request.values


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


class QueryProtocolRule(SmithyRule):
    def __init__(
        self,
        string: str,
        operations: list[OperationModel],
        methods: list[str],
        **kwargs: Any,
    ) -> None:
        super().__init__(
            string=string, operations=operations, methods=methods, **kwargs
        )
        for candidate in self.candidates:
            operation = candidate.operation
            candidate.add_constraint(RequiredParam("Action", operation.name))


class JsonProtocolRule(SmithyRule):
    def __init__(
        self,
        string: str,
        operations: list[OperationModel],
        methods: list[str],
        **kwargs: Any,
    ) -> None:
        super().__init__(
            string=string, operations=operations, methods=methods, **kwargs
        )
        for candidate in self.candidates:
            operation = candidate.operation
            candidate.add_constraint(
                RequiredHeader(
                    "X-Amz-Target",
                    f"{operation.service_model.metadata.get('targetPrefix')}.{operation.name}",
                )
            )


def _create_service_map(service: ServiceModel) -> dict[str, Map]:
    """
    Creates a Werkzeug Map object with all rules necessary for the specific service.
    :param service: botocore service model to create the rules for
    :return: a Map instance which is used to perform the in-service operation routing
    """
    ops = [service.operation_model(op_name) for op_name in service.operation_names]
    # protocol = str(service.protocol)

    # group all operations by their path and method
    path_index: dict[tuple[str, str], list[OperationModel]] = defaultdict(list)
    for op in ops:
        http_op = op.http_trait
        path_index[(http_op.path, http_op.method)].append(op)

    protocol_to_rules: dict[str, Map] = {}
    protocol = str(service.protocol)
    supported_protocols = service.metadata.get("protocols", [protocol])
    for protocol in supported_protocols:
        rules = []
        # create a matching rule for each (path, method) combination
        for (path, method), ops in path_index.items():
            # translate the requestUri to a Werkzeug rule string
            rule_string = to_werkzeug_rule_string(path)
            # for protocol in service.protocols:
            if protocol.startswith("rest"):
                if len(ops) == 1:
                    # if there is only a single operation for a (path, method) combination,
                    # the default Werkzeug rule can be used directly (this is the case for most rules)
                    op = ops[0]
                    subdomain = None
                    if op.endpoint:
                        host_prefix = op.endpoint.get("hostPrefix")  # type: ignore[attr-defined]
                        # Only labeled host prefixes (e.g. "{AccountId}.") carry
                        # request data.  Literal prefixes (e.g. "cp.") are ignored,
                        # since Moto doesn't receive requests on those hosts.
                        if host_prefix and host_prefix.startswith("{"):
                            subdomain = f"<{host_prefix[1:-2]}>"
                    rules.append(
                        SmithyRule(
                            string=rule_string,
                            operations=[op],
                            methods=[method],
                            subdomain=subdomain,
                        )
                    )  # type: ignore
                    if add_s3_subdomain():
                        if op.http_trait.path.startswith("/{Bucket}"):
                            new_path = op.http_trait.path.replace("/{Bucket}", "", 1)
                            if new_path == "":
                                new_path = "/"
                            rule_string = to_werkzeug_rule_string(new_path)
                            rules.append(
                                SmithyRule(
                                    string=rule_string,
                                    operations=ops,
                                    methods=[method],
                                    subdomain="<Bucket>",
                                )
                            )
                else:
                    # if there is an ambiguity with only the (path, method) combination,
                    # a custom rule - which can use additional request metadata - needs to be used
                    rules.append(
                        SmithyRule(string=rule_string, methods=[method], operations=ops)
                    )
                    s3_ops = [
                        op for op in ops if op.http_trait.path.startswith("/{Bucket}")
                    ]
                    if s3_ops and add_s3_subdomain():
                        new_path = s3_ops[0].http_trait.path.replace("/{Bucket}", "", 1)
                        if new_path == "":
                            new_path = "/"
                        rule_string = to_werkzeug_rule_string(new_path)
                        rules.append(
                            SmithyRule(
                                string=rule_string,
                                methods=[method],
                                operations=s3_ops,
                                subdomain="<Bucket>",
                            )
                        )

            elif protocol in ("query", "ec2"):
                rules.append(
                    QueryProtocolRule(
                        string=rule_string, operations=ops, methods=["POST", "GET"]
                    )
                )
                if service.service_name == "sqs":
                    rule_string = to_werkzeug_rule_string("/{AccountId}/{QueueName}")
                    rules.append(
                        QueryProtocolRule(
                            string=rule_string,
                            operations=ops,
                            methods=["POST", "GET"],
                        )
                    )
            elif protocol.startswith("json"):
                rules.append(
                    JsonProtocolRule(
                        string=rule_string, operations=ops, methods=[method]
                    )
                )
                if service.service_name == "sqs":
                    rule_string = to_werkzeug_rule_string("/{AccountId}/{QueueName}")
                    rules.append(
                        JsonProtocolRule(
                            string=rule_string,
                            operations=ops,
                            methods=["POST"],
                        )
                    )
        protocol_to_rules[protocol] = Map(
            rules=rules,
            # don't be strict about trailing slashes when matching
            strict_slashes=False,
            # we can't really use werkzeug's merge-slashes since it uses HTTP redirects to solve it
            merge_slashes=False,
            # get service-specific converters
            converters={
                GreedyLabelConverter.NAME: GreedyLabelConverter,
                ImplicitGreedyLabelConverter.NAME: ImplicitGreedyLabelConverter,
            },
            default_subdomain="",
        )
    return protocol_to_rules


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


class ServiceOperationRouter:
    """
    A router implementation which abstracts the (quite complex) routing of incoming HTTP requests to a specific
    operation within a "REST" service (rest-xml, rest-json).
    """

    _map: dict[str, Map]

    def __init__(self, service: ServiceModel):
        self._service_model = service
        self._map = _create_service_map(service)

    def match(
        self, request: Request, s3_response: S3Response | None = None
    ) -> tuple[OperationModel, dict[str, Any]]:
        """
        Matches the given request to the operation it targets (or raises an exception if no operation matches).

        :param request: The request of which the targeting operation needs to be found
        :return: A tuple with the matched operation and the (already parsed) path params
        :raises: Werkzeug's NotFound exception in case the given request does not match any operation
        """
        protocol = determine_request_protocol(self._service_model, request.content_type)
        protocol_map = self._map[protocol]
        matcher: MapAdapter = protocol_map.bind(
            request.host,
            subdomain=request.host.split(".", 1)[0]
            if (
                s3_response is not None and s3_response.subdomain_based_buckets(request)
            )
            or ".s3-control" in request.host
            else None,
        )

        method = request.method
        path_info = self._get_path_info_for_matching(request)
        try:
            rule, arguments = self._match_path(matcher, path_info, method)
        except MethodNotAllowed as e:
            raise NotFound() from e

        assert isinstance(rule, SmithyRule)
        operation = rule.match_request(request)
        if operation is None:
            raise NotFound()

        return operation, to_uri_params(arguments)

    @staticmethod
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

    @staticmethod
    def _get_path_info_for_matching(request: Request) -> str:
        path_info = request.raw_path
        # Werkzeug's MapAdapter.match strips leading slashes, which loses the leading
        # slash of an S3 key in a virtual-hosted request (e.g. bucket.s3.amazonaws.com//key),
        # so encode it to keep it in the matched label.
        if path_info.startswith("//"):
            path_info = "/%2F" + path_info[2:]
        return path_info


@functools.lru_cache(10)
def get_service_router(service_name: str) -> ServiceOperationRouter:
    service_model = get_service_model(service_name)
    service_router = ServiceOperationRouter(service_model)
    return service_router
