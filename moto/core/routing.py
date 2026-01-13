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

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from typing import (
    Any,
    AnyStr,
    NamedTuple,
    TypeVar,
)
from urllib.parse import parse_qs, unquote, urlparse

from werkzeug.datastructures import Headers, MultiDict
from werkzeug.exceptions import MethodNotAllowed, NotFound
from werkzeug.routing import Map, MapAdapter, PathConverter, Rule

from moto.core.model import OperationModel, ServiceModel, StructureShape
from moto.core.request import Request, determine_request_protocol
from moto.core.utils import get_service_model

# Regex to find path parameters in requestUris of AWS service specs (f.e. /{param1}/{param2+})
path_param_regex = re.compile(r"({.+?})")
# Translation table which replaces characters forbidden in Werkzeug rule names with temporary replacements
# Note: The temporary replacements must not occur in any requestUri of any operation in any service!
_rule_replacements = {"-": "_0_"}
# String translation table for #_rule_replacements for str#translate
_rule_replacement_table = str.maketrans(_rule_replacements)


@dataclass
class ActionConstraintContext:
    request: Request


class ActionConstraint:
    def accept(self, context: ActionConstraintContext) -> bool:
        raise NotImplementedError


class DataValueConstraint(ActionConstraint):
    def __init__(self, name: str, value: str):
        self.name = name
        self.value = value

    def accept(self, context: ActionConstraintContext) -> bool:
        return context.request.values.get(self.name) == self.value


class HeaderValueConstraint(ActionConstraint):
    def __init__(self, name: str, value: str):
        self.name = name
        self.value = value

    def accept(self, context: ActionConstraintContext) -> bool:
        return context.request.headers.get(self.name) == self.value


class ActionCandidate:
    def __init__(
        self,
        operation: OperationModel,
        constraints: list[ActionConstraint] | None = None,
    ):
        self.operation = operation
        self.constraints = constraints or []


class ActionSelector:
    def __init__(self, candidates: list[ActionCandidate]):
        self.candidates = candidates

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
            return raw_uri.split("?")[0]
        return urlparse(raw_uri or request.path).path

    raise ValueError("cannot extract raw path from request object %s")


class StrictMethodRule(Rule):
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


# Should be singular
def transform_path_params_to_rule_vars(match: re.Match[AnyStr]) -> str:
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
        _rule_replacement_table
    )

    return f"<{greedy_prefix}{escaped_request_uri_variable}>"


def post_process_arg_name(arg_key: str) -> str:
    """
    Reverses previous manipulations to the path parameters names (like replacing forbidden characters with
    placeholders).
    :param arg_key: Path param key name extracted using Werkzeug rules
    :return: Post-processed ("un-sanitized") path param key
    """
    result = arg_key
    for original, substitution in _rule_replacements.items():
        result = result.replace(substitution, original)
    return result


HTTP_METHODS = ("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "TRACE")

E = TypeVar("E")
RequestArguments = Mapping[str, Any]


class GreedyPathConverter(PathConverter):
    """
    This converter makes sure that the path ``/mybucket//mykey`` can be matched to the pattern
    ``<Bucket>/<path:Key>`` and will result in `Key` being `/mykey`.
    """

    regex = ".*?"

    part_isolating = False
    """From the werkzeug docs: If a custom converter can match a forward slash, /, it should have the
    attribute part_isolating set to False. This will ensure that rules using the custom converter are
    correctly matched."""


class _HttpOperation(NamedTuple):
    """Useful intermediary representation of the 'http' block of an operation to make code cleaner"""

    operation: OperationModel
    path: str
    method: str
    query_args: Mapping[str, list[str]]
    header_args: list[str]
    deprecated: bool

    @staticmethod
    def from_operation(op: OperationModel) -> _HttpOperation:
        # I don't know if any of this holds if we use our loader to load the model...
        # See here: https://github.com/boto/botocore/blob/e2a72fcedae63c2875e56c007b3ffaa491375653/botocore/handlers.py#L1093

        # ***original localstack comment****
        # botocore >= 1.28 might modify the internal model (specifically for S3).
        # It will modify the request URI to strip the bucket name from the path and set the original value at
        # "authPath".
        # Since botocore 1.31.2, botocore will strip the query from the `authPart`
        # We need to add it back from `requestUri` field
        # Use authPath if set, otherwise use the regular requestUri.
        if auth_path := op.http.get("authPath"):
            path, sep, query = op.http.get("requestUri", "").partition("?")
            uri = f"{auth_path.rstrip('/')}{sep}{query}"
        else:
            uri = op.http.get("requestUri", "/")

        method = op.http.get("method", "POST")
        deprecated = op.deprecated

        # requestUris can contain mandatory query args (f.e. /apikeys?mode=import)
        path_query = uri.split("?")
        path = path_query[0]
        header_args = []
        query_args: dict[str, list[str]] = {}

        if len(path_query) > 1:
            # parse the query args of the request URI (they are mandatory)
            query_args = parse_qs(path_query[1], keep_blank_values=True)
            # for mandatory keys without values, keep an empty list (instead of [''] - the result of parse_qs)
            query_args = {k: list(filter(None, v)) for k, v in query_args.items()}

        # find the required header and query parameters of the input shape
        input_shape = op.input_shape
        if isinstance(input_shape, StructureShape):
            for required_member in input_shape.required_members:
                member_shape = input_shape.members[required_member]
                location = member_shape.serialization.get("location")
                if location is not None:
                    if location == "header":
                        header_name = member_shape.serialization.get("name")
                        header_args.append(header_name)
                    elif location == "querystring":
                        query_name = member_shape.serialization.get("name")
                        # do not overwrite potentially already existing query params with specific values
                        if query_name not in query_args:
                            # an empty list defines a required query param only needs to be present
                            # (no specific value will be enforced when matching)
                            query_args[query_name] = []  # type: ignore[index]

        return _HttpOperation(op, path, method, query_args, header_args, deprecated)  # type: ignore[arg-type]


class _RequiredArgsRule:
    """
    Specific Rule implementation which checks if a set of certain required header and query parameters are matched by
    a specific request.
    """

    endpoint: Any
    required_query_args: Mapping[str, list[Any]] | None
    required_header_args: list[str]
    match_score: int

    def __init__(self, operation: _HttpOperation) -> None:
        super().__init__()
        self.endpoint = operation.operation
        self.required_query_args = operation.query_args or {}
        self.required_header_args = operation.header_args or []
        self.match_score = (
            10
            + 10 * len(self.required_query_args)
            + 10 * len(self.required_header_args)
        )
        # If this operation is deprecated, the score is a bit less high (bot not as much as a matching required arg)
        if operation.deprecated:
            self.match_score -= 5

    def matches(self, query_args: MultiDict[str, Any], headers: Headers) -> bool:
        """
        Returns true if the given query args and the given headers of a request match the required query args and
        headers of this rule.
        :param query_args: query arguments of the incoming request
        :param headers: headers of the incoming request
        :return: True if the query args and headers match the required args of this rule
        """
        if self.required_query_args:
            for key, values in self.required_query_args.items():
                if key not in query_args:
                    return False
                # if a required query arg also has a list of required values set, the values need to match as well
                if values:
                    query_arg_values = query_args.getlist(key)
                    for value in values:
                        if value not in query_arg_values:
                            return False

        if self.required_header_args:
            for key in self.required_header_args:
                if key not in headers:
                    return False

        return True


class _RequestMatchingRule(StrictMethodRule):
    """
    A Werkzeug Rule extension which initially acts as a normal rule (i.e. matches a path and method).

    This rule matches if one of its sub-rules _might_ match.
    It cannot be assumed that one of the fine-grained rules matches, just because this rule initially matches.
    If this rule matches, the caller _must_ call `match_request` in order to find the actual fine-grained matching rule.
    The result of `match_request` is only meaningful if this wrapping rule also matches.
    """

    def __init__(
        self, string: str, operations: list[_HttpOperation], method: str, **kwargs: Any
    ) -> None:
        super().__init__(string=string, methods=[method], **kwargs)
        # Create a rule which checks all required arguments (not only the path and method)
        rules = [_RequiredArgsRule(op) for op in operations]
        # Sort the rules descending based on their rule score
        # (i.e. the first matching rule will have the highest score)=
        self.rules = sorted(rules, key=lambda rule: rule.match_score, reverse=True)

    def match_request(self, request: Request) -> _RequiredArgsRule:
        """
        Function which needs to be called by a caller if the _RequestMatchingRule already matched using Werkzeug's
        default matching mechanism.

        :param request: to perform the fine-grained matching on
        :return: matching fine-grained rule
        :raises: NotFound if none of the fine-grained rules matches
        """
        for rule in self.rules:
            if rule.matches(request.args, request.headers):
                return rule
        raise NotFound()


def _create_service_map(service: ServiceModel) -> dict[str, Map]:
    """
    Creates a Werkzeug Map object with all rules necessary for the specific service.
    :param service: botocore service model to create the rules for
    :return: a Map instance which is used to perform the in-service operation routing
    """
    ops = [service.operation_model(op_name) for op_name in service.operation_names]
    # protocol = str(service.protocol)

    # group all operations by their path and method
    path_index: dict[tuple[str, str], list[_HttpOperation]] = defaultdict(list)
    for op in ops:
        http_op = _HttpOperation.from_operation(op)
        path_index[(http_op.path, http_op.method)].append(http_op)

    protocol_to_rules: dict[str, Map] = {}
    protocol = str(service.protocol)
    supported_protocols = service.metadata.get("protocols", [protocol])
    for protocol in supported_protocols:
        rules = []
        # create a matching rule for each (path, method) combination
        for (path, method), ops in path_index.items():
            # translate the requestUri to a Werkzeug rule string
            rule_string = path_param_regex.sub(transform_path_params_to_rule_vars, path)
            # for protocol in service.protocols:
            if protocol.startswith("rest"):
                if len(ops) == 1:
                    # if there is only a single operation for a (path, method) combination,
                    # the default Werkzeug rule can be used directly (this is the case for most rules)
                    op = ops[0]
                    rules.append(
                        StrictMethodRule(
                            string=rule_string, methods=[method], endpoint=op.operation
                        )
                    )  # type: ignore
                    if op.path.startswith("/{Bucket}"):
                        new_path = op.path.replace("/{Bucket}", "", 1)
                        if new_path == "":
                            new_path = "/"
                        rule_string = path_param_regex.sub(
                            transform_path_params_to_rule_vars, new_path
                        )
                        rules.append(
                            StrictMethodRule(
                                string=rule_string,
                                methods=[method],
                                endpoint=op.operation,
                                subdomain="<Bucket>",
                            )
                        )
                else:
                    # if there is an ambiguity with only the (path, method) combination,
                    # a custom rule - which can use additional request metadata - needs to be used
                    rules.append(
                        _RequestMatchingRule(
                            string=rule_string, method=method, operations=ops
                        )
                    )
                    for op in ops:
                        if op.path.startswith("/{Bucket}"):
                            new_path = op.path.replace("/{Bucket}", "", 1)
                            if new_path == "":
                                new_path = "/"
                            rule_string = path_param_regex.sub(
                                transform_path_params_to_rule_vars, new_path
                            )
                            rules.append(
                                StrictMethodRule(
                                    string=rule_string,
                                    methods=[method],
                                    endpoint=op.operation,
                                    subdomain="<Bucket>",
                                )
                            )

            elif protocol in ("query", "ec2"):
                candidate_list = []
                for op in ops:
                    candidate = ActionCandidate(
                        op.operation, [DataValueConstraint("Action", op.operation.name)]
                    )
                    candidate_list.append(candidate)
                rules.append(
                    StrictMethodRule(
                        string=rule_string,
                        methods=["POST", "GET"],
                        endpoint=ActionSelector(candidate_list),
                    )
                )
                if service.service_name == "sqs":
                    rule_string = path_param_regex.sub(
                        transform_path_params_to_rule_vars, "/{AccountId}/{QueueName}"
                    )
                    rules.append(
                        StrictMethodRule(
                            string=rule_string,
                            methods=["POST", "GET"],
                            endpoint=ActionSelector(candidate_list),
                        )
                    )
            elif protocol.startswith("json"):
                candidate_list = []
                for op in ops:
                    candidate = ActionCandidate(
                        op.operation,
                        [
                            HeaderValueConstraint(
                                "X-Amz-Target",
                                f"{service.metadata.get('targetPrefix')}.{op.operation.name}",
                            )
                        ],
                    )
                    candidate_list.append(candidate)
                rules.append(
                    StrictMethodRule(
                        string=rule_string,
                        methods=[method],
                        endpoint=ActionSelector(candidate_list),
                    )
                )
                if service.service_name == "sqs":
                    rule_string = path_param_regex.sub(
                        transform_path_params_to_rule_vars, "/{AccountId}/{QueueName}"
                    )
                    rules.append(
                        StrictMethodRule(
                            string=rule_string,
                            methods=["POST"],
                            endpoint=ActionSelector(candidate_list),
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
            default_subdomain="s3" if service.service_name == "s3" else "",
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

    def match(self, request: Request) -> tuple[OperationModel, Mapping[str, Any]]:
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
            subdomain=request.host.split(".", 1)[0]
            if request.host.find("s3") > 1
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

        # if the found rule is a _RequestMatchingRule, the multi rule matching needs to be invoked to perform the
        # fine-grained matching based on the whole request
        if isinstance(rule, _RequestMatchingRule):
            rule = rule.match_request(request)  # type: ignore[assignment]

        if isinstance(rule.endpoint, ActionSelector):
            operation = rule.endpoint.select_action(request)
            if operation is None:
                raise NotFound()
            return operation, args

        # post process the arg keys and values
        # - the path param keys need to be "un-sanitized", i.e. sanitized rule variable names need to be reverted
        # - the path param values might still be url-encoded
        args = {post_process_arg_name(k): unquote(v) for k, v in args.items()}

        # extract the operation model from the rule
        operation = rule.endpoint

        return operation, args


def test_op_router() -> None:
    model = get_service_model("mq")
    router = ServiceOperationRouter(model)
    req = Request.from_values(
        method="POST", path="/v1/brokers/broker-id-test/users/username-test"
    )
    op, args = router.match(req)

    assert op.name == "CreateUser"
    assert args["broker-id"] == "broker-id-test"
    assert args["username"] == "username-test"


def test_s3_router() -> None:
    model = get_service_model("s3")
    router = ServiceOperationRouter(model)
    req = Request.from_values(method="GET", path="/my-bucket-name?list-type=2")
    op, args = router.match(req)

    assert op.name == "ListObjectsV2"
    assert args["Bucket"] == "my-bucket-name"
