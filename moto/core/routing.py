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

import re
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote, urlparse

from werkzeug.exceptions import MethodNotAllowed, NotFound
from werkzeug.routing import Map, MapAdapter, PathConverter, Rule

from moto import settings
from moto.core.model import OperationModel, ServiceModel, StructureShape
from moto.core.request import Request, determine_request_protocol

if TYPE_CHECKING:
    from moto.s3.responses import S3Response

PATH_PARAM_REGEX = re.compile(r"({.+?})")

PATH_PARAM_TO_RULE_VAR_REPLACEMENTS = {"-": "_HYPHEN_"}
PATH_PARAM_TO_RULE_VAR_TRANSLATION_TABLE = str.maketrans(
    PATH_PARAM_TO_RULE_VAR_REPLACEMENTS
)


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
    def __init__(self, name: str, value: str | None = None):
        self.name = name
        self.value = value

    def accept(self, context: ActionConstraintContext) -> bool:
        values = context.request.values
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


def get_raw_path(request: Request) -> str:
    """
    Returns the raw_path inside the request without the query string. The request can either be a Quart Request
    object (that encodes the raw path in request.scope['raw_path']) or a Werkzeug WSGI request (that encodes the raw
    URI in request.environ['RAW_URI']).

    :param request: the request object
    :return: the raw path if any
    """
    if hasattr(request, "environ"):
        # werkzeug/flask request (already a string, and contains the query part)
        # we need to parse it, because the RAW_URI can contain a full URL if it is specified in the HTTP request
        raw_uri: str = request.environ.get("RAW_URI", "")
        if raw_uri.startswith("//"):
            # if the RAW_URI starts with double slashes, `urlparse` will fail to decode it as path only
            # it also means that we already only have the path, so we just need to remove the query string
            raw_uri = "/%2F" + raw_uri[2:]
            # return raw_uri.split("?")[0]
        return urlparse(raw_uri or request.path).path

    raise ValueError("cannot extract raw path from request object %s")


def to_werkzeug_rule_string(smithy_uri: str) -> str:
    """Converts a Smithy uri to a Werkzeug rule string.

    Examples:
        "/resources/{resource_id}/methods/{http_method}" -> "/resources/<resource_id>/methods/<http_method>"
        "/v20180820/tags/{resourceArn+}" -> "/v20180820/tags/<path:resourceArn>"
    """

    def to_rule_variable(match: re.Match[str]) -> str:
        """
        Transforms a request URI path param to a valid Werkzeug Rule string variable placeholder.
        This transformation function should be used in combination with _path_param_regex on the request URIs (without any
        query params).

        :param match: Regex match which contains a single group. The match group is a request URI path param, including the
                        surrounding curly braces.
        :return: Werkzeug rule string variable placeholder which is semantically equal to the given request URI path param

        """
        # get the group match and strip the curly braces
        request_uri_variable: str = match.group(0)[1:-1]  # type: ignore[assignment]

        # if the request URI param is greedy (f.e. /foo/{Bar+}), add Werkzeug's "path" prefix (/foo/{path:Bar})
        greedy_prefix = ""
        if request_uri_variable.endswith("+"):
            greedy_prefix = "path:"
            request_uri_variable = request_uri_variable.strip("+")

        # replace forbidden chars (not allowed in Werkzeug rule variable names) with their placeholder
        escaped_request_uri_variable = request_uri_variable.translate(
            PATH_PARAM_TO_RULE_VAR_TRANSLATION_TABLE
        )

        return f"<{greedy_prefix}{escaped_request_uri_variable}>"

    rule_string = PATH_PARAM_REGEX.sub(to_rule_variable, smithy_uri)
    return rule_string


class BaseSmithyRule(Rule):
    """
    Small extension to Werkzeug's Rule class which reverts unwanted assumptions made by Werkzeug.
    Reverted assumptions:
    - Werkzeug automatically matches HEAD requests to the corresponding GET request (i.e. Werkzeug's rule automatically
      adds the HEAD HTTP method to a rule which should only match GET requests). This is implemented to simplify
      implementing an app compliant with HTTP (where a HEAD request needs to return the headers of a corresponding GET
      request), but it is unwanted for our strict rule matching in here.
    """

    def __init__(self, string: str, methods: list[str], **kwargs: Any) -> None:
        super().__init__(string=string, methods=methods, **kwargs)

        # Make sure Werkzeug's Rule does not add any other methods
        # (f.e. the HEAD method even though the rule should only match GET)
        if self.methods and "HEAD" in self.methods and "HEAD" not in methods:
            self.methods = {method.upper() for method in methods}


def to_uri_params(werkzeug_path_variables: Mapping[str, Any]) -> dict[str, Any]:
    def post_process_arg_name(arg_key: str) -> str:
        """
        Reverses previous manipulations to the path parameters names (like replacing forbidden characters with
        placeholders).
        :param arg_key: Path param key name extracted using Werkzeug rules
        :return: Post-processed ("un-sanitized") path param key
        """
        result = arg_key
        for original, substitution in PATH_PARAM_TO_RULE_VAR_REPLACEMENTS.items():
            result = result.replace(substitution, original)
        return result

    # post process the arg keys and values
    # - the path param keys need to be "un-sanitized", i.e. sanitized rule variable names need to be reverted
    # - the path param values might still be url-encoded
    uri_params = {
        post_process_arg_name(name): unquote(value)
        for name, value in werkzeug_path_variables.items()
    }
    return uri_params


class GreedyPathConverter(PathConverter):
    """
    This converter makes sure that the path ``/mybucket//mykey`` can be matched to the pattern
    ``<Bucket>/<path:Key>`` and will result in `Key` being `/mykey`.
    """

    regex = ".*?"
    # regex = "[^/].*?"
    part_isolating = False
    """From the werkzeug docs: If a custom converter can match a forward slash, /, it should have the
    attribute part_isolating set to False. This will ensure that rules using the custom converter are
    correctly matched."""


class ConstrainedRule(BaseSmithyRule):
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
        super().__init__(string=string, methods=methods, **kwargs)
        self.candidates = []
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
            self.candidates.append(candidate)

    def match_request(self, request: Request) -> OperationModel | None:
        """
        Function which needs to be called by a caller if the _RequestMatchingRule already matched using Werkzeug's
        default matching mechanism.

        :param request: to perform the fine-grained matching on
        :return: matching fine-grained rule
        :raises: NotFound if none of the fine-grained rules matches
        """
        action_selector = ActionSelector(self.candidates)
        value = action_selector.select_action(request)
        return value


class QueryProtocolRule(ConstrainedRule):
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
            candidate.add_constraint(RequiredArg("Action", operation.name))


class JsonProtocolRule(ConstrainedRule):
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
                        subdomain = op.endpoint.get("hostPrefix")  # type: ignore[attr-defined]
                        subdomain = (
                            f"<{subdomain[1:-2]}>" if subdomain is not None else None
                        )
                        assert subdomain != "<Bucket>"
                    rules.append(
                        ConstrainedRule(
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
                                ConstrainedRule(
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
                        ConstrainedRule(
                            string=rule_string, methods=[method], operations=ops
                        )
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
                            ConstrainedRule(
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
            converters={"path": GreedyPathConverter},
            default_subdomain="default"
            if add_s3_subdomain()
            else "",  # "s3" if service.service_name == "s3" else "",
        )
    return protocol_to_rules


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
        # bind the map to get the actual matcher
        matcher: MapAdapter = protocol_map.bind(
            request.host,
            # subdomain=request.host.split(".", 1)[0]
            # if (request.host.find("s3") > 1 or request.server[0].endswith(".localhost"))  # type: ignore[index]
            # and add_s3_subdomain()
            # else None,
            subdomain=request.host.split(".", 1)[0]
            if (
                s3_response is not None and s3_response.subdomain_based_buckets(request)
            )
            or ".s3-control" in request.host
            else None,
        )

        # perform the matching
        try:
            # some services (at least S3) allow OPTIONS request (f.e. for CORS preflight requests) without them being
            # specified. the specs do _not_ contain any operations on OPTIONS methods at all.
            # avoid matching issues for preflight requests by matching against a similar GET request instead.
            method = request.method if request.method != "OPTIONS" else "GET"

            path = get_raw_path(request)
            # trailing slashes are ignored in smithy matching,
            # see https://smithy.io/1.0/spec/core/http-traits.html#literal-character-sequences and this
            # makes sure that, e.g., in s3, `GET /mybucket/` is not matched to `GetBucket` and not to
            # `GetObject` and the associated rule.
            path = path.rstrip("/") if len(path) > 1 else path

            rule, args = matcher.match(path, method=method, return_rule=True)
        except MethodNotAllowed as e:
            # MethodNotAllowed (405) exception is raised if a path is matching, but the method does not.
            # Our router handles this as a 404.
            raise NotFound() from e

        assert isinstance(rule, ConstrainedRule)
        operation = rule.match_request(request)
        if operation is None:
            raise NotFound()

        return operation, to_uri_params(args)
