# mypy: ignore-errors
# Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
"""Request parsers for the various protocol types.

The module contains classes that can take an HTTP response, and given
an output shape, parse the response into a dict according to the
rules in the output shape.

There are many similarities amongst the different protocols with regard
to response parsing, and the code is structured in a way to avoid
code duplication when possible.  The diagram below is a diagram
showing the inheritance hierarchy of the response classes.

::



                                 +--------------+
                                 |ResponseParser|
                                 +--------------+
                                    ^    ^    ^
               +--------------------+    |    +-------------------+
               |                         |                        |
    +----------+----------+       +------+-------+        +-------+------+
    |BaseXMLResponseParser|       |BaseRestParser|        |BaseJSONParser|
    +---------------------+       +--------------+        +--------------+
              ^         ^          ^           ^           ^        ^
              |         |          |           |           |        |
              |         |          |           |           |        |
              |        ++----------+-+       +-+-----------++       |
              |        |RestXMLParser|       |RestJSONParser|       |
        +-----+-----+  +-------------+       +--------------+  +----+-----+
        |QueryParser|                                          |JSONParser|
        +-----------+                                          +----------+


The diagram above shows that there is a base class, ``ResponseParser`` that
contains logic that is similar amongst all the different protocols (``query``,
``json``, ``rest-json``, ``rest-xml``).  Amongst the various services there
is shared logic that can be grouped several ways:

* The ``query`` and ``rest-xml`` both have XML bodies that are parsed in the
  same way.
* The ``json`` and ``rest-json`` protocols both have JSON bodies that are
  parsed in the same way.
* The ``rest-json`` and ``rest-xml`` protocols have additional attributes
  besides body parameters that are parsed the same (headers, query string,
  status code).

This is reflected in the class diagram above.  The ``BaseXMLResponseParser``
and the BaseJSONParser contain logic for parsing the XML/JSON body,
and the BaseRestParser contains logic for parsing out attributes that
come from other parts of the HTTP response.  Classes like the
``RestXMLParser`` inherit from the ``BaseXMLResponseParser`` to get the
XML body parsing logic and the ``BaseRestParser`` to get the HTTP
header/status code/query string parsing.

Additionally, there are event stream parsers that are used by the other parsers
to wrap streaming bodies that represent a stream of events. The
BaseEventStreamParser extends from ResponseParser and defines the logic for
parsing values from the headers and payload of a message from the underlying
binary encoding protocol. Currently, event streams support parsing bodies
encoded as JSON and XML through the following hierarchy.


                                  +--------------+
                                  |ResponseParser|
                                  +--------------+
                                    ^    ^    ^
               +--------------------+    |    +------------------+
               |                         |                       |
    +----------+----------+   +----------+----------+    +-------+------+
    |BaseXMLResponseParser|   |BaseEventStreamParser|    |BaseJSONParser|
    +---------------------+   +---------------------+    +--------------+
                     ^                ^        ^                 ^
                     |                |        |                 |
                     |                |        |                 |
                   +-+----------------+-+    +-+-----------------+-+
                   |EventStreamXMLParser|    |EventStreamJSONParser|
                   +--------------------+    +---------------------+

Return Values
=============

Each call to ``parse()`` returns a dict has this form::

    Standard Response

    {
      "ResponseMetadata": {"RequestId": <requestid>}
      <response keys>
    }

    Error response

    {
      "ResponseMetadata": {"RequestId": <requestid>}
      "Error": {
        "Code": <string>,
        "Message": <string>,
        "Type": <string>,
        <additional keys>
      }
    }

"""

from __future__ import annotations

import base64
import json
import logging
import re
from collections import defaultdict
from typing import TYPE_CHECKING, Any, TypedDict
from urllib.parse import urlparse
from xml.etree import ElementTree as ETree
from xml.etree.ElementTree import ParseError as XMLParseError

from botocore.utils import is_json_value_header, parse_timestamp

if TYPE_CHECKING:
    from botocore.model import OperationModel, ServiceModel

LOG = logging.getLogger(__name__)

DEFAULT_TIMESTAMP_PARSER = parse_timestamp


def _text_content(func):
    # This decorator hides the difference between
    # an XML node with text or a plain string.  It's used
    # to ensure that scalar processing operates only on text
    # strings, which allows the same scalar handlers to be used
    # for XML nodes from the body and HTTP headers.
    def _get_text_content(self, shape, node_or_string):
        if hasattr(node_or_string, "text"):
            text = node_or_string.text
            if text is None:
                # If an XML node is empty <foo></foo>,
                # we want to parse that as an empty string,
                # not as a null/None value.
                text = ""
        else:
            text = node_or_string
        return func(self, shape, text)

    return _get_text_content


class ResponseParserError(Exception):
    pass


# Sentinel to signal the absence of a field in the input

UNDEFINED = object()


class RequestDict(TypedDict):
    url_path: str
    method: str
    headers: dict[str, Any]
    body: str
    values: dict[str, Any]


class ParsedDict(TypedDict):
    action: str
    params: dict[str, Any]


class RequestParser:
    DEFAULT_ENCODING = "utf-8"
    MAP_TYPE = dict

    def __init__(
        self,
        service_model: ServiceModel,
        timestamp_parser=None,
        blob_parser=None,
        map_type=None,
    ):
        self.service_model = service_model
        if timestamp_parser is None:
            timestamp_parser = DEFAULT_TIMESTAMP_PARSER
        self._timestamp_parser = timestamp_parser
        if blob_parser is None:
            blob_parser = self._default_blob_parser
        self._blob_parser = blob_parser
        if map_type is not None:
            self.MAP_TYPE = map_type

    def _default_blob_parser(self, value):
        # Blobs are always returned as bytes type (this matters on python3).
        # We don't decode this to a str because it's entirely possible that the
        # blob contains binary data that actually can't be decoded.
        return base64.b64decode(value)

    def _parse_action(self, request_dict: RequestDict) -> str:
        raise NotImplementedError()

    def _parse_params(
        self, request_dict: RequestDict, operation_model: OperationModel
    ) -> dict[str, Any]:
        shape = operation_model.input_shape
        if shape is None:
            return {}
        parsed = self._do_parse(request_dict, operation_model.input_shape)
        return parsed

    def parse(self, request_dict: RequestDict) -> ParsedDict:
        action = self._parse_action(request_dict)
        operation_model = self.service_model.operation_model(action)
        params = self._parse_params(request_dict, operation_model)
        parsed: ParsedDict = {
            "action": action,
            "params": params,
        }
        return parsed

    def _do_parse(self, request_dict, shape):
        raise NotImplementedError("%s._do_parse" % self.__class__.__name__)

    def _parse_shape(self, shape, node):
        handler = getattr(self, "_handle_%s" % shape.type_name, self._default_handle)
        return handler(shape, node)

    def _handle_list(self, shape, node):
        # Enough implementations share list serialization that it's moved
        # up here in the base class.
        parsed = []
        member_shape = shape.member
        for item in node:
            parsed.append(self._parse_shape(member_shape, item))
        return parsed

    def _default_handle(self, shape, value):
        return value


class QueryParser(RequestParser):
    def _parse_params(self, request_dict, operation_model):
        parsed = self.MAP_TYPE()
        shape = operation_model.input_shape
        if shape is None:
            return parsed
        parsed = self._do_parse(request_dict, shape)
        return parsed if parsed is not UNDEFINED else {}

    def _parse_action(self, request_dict):
        params = request_dict["values"]
        return params.get("Action", "UnknownAction")

    def _do_parse(self, request_dict, shape):
        parsed = self._parse_shape(shape, request_dict["values"])
        return parsed if parsed is not UNDEFINED else {}

    def _parse_shape(self, shape, node, prefix=""):
        handler = getattr(self, "_handle_%s" % shape.type_name, self._default_handle)
        return handler(shape, node, prefix)

    def _gonna_recurse(self, query_params, prefix):
        if prefix == "":
            return False
        return not any([param_key.startswith(prefix) for param_key in query_params])

    def _handle_structure(self, shape, query_params, prefix=""):
        if self._gonna_recurse(query_params, prefix):
            return UNDEFINED
        parsed = self.MAP_TYPE()
        members = shape.members
        for member_name in members:
            member_shape = members[member_name]
            member_prefix = self._get_serialized_name(member_shape, member_name)
            if prefix:
                member_prefix = "%s.%s" % (prefix, member_prefix)
            value = self._parse_shape(member_shape, query_params, member_prefix)
            parsed_key = self._parsed_key_name(member_name)
            if value is not UNDEFINED:
                parsed[parsed_key] = value
        return parsed if parsed != {} else UNDEFINED

    def _handle_list(self, shape, node, prefix=""):
        # The query protocol serializes empty lists as an empty string.
        if node.get(prefix, UNDEFINED) == "":
            return []

        if self._is_shape_flattened(shape):
            list_prefix = prefix
            if shape.member.serialization.get("name"):
                name = self._get_serialized_name(shape.member, default_name="")
                # Replace '.Original' with '.{name}'.
                list_prefix = ".".join(prefix.split(".")[:-1] + [name])
        else:
            list_name = shape.member.serialization.get("name", "member")
            list_prefix = f"{prefix}.{list_name}"
        parsed_list = []
        i = 1
        while True:
            element_name = f"{list_prefix}.{i}"
            element_shape = shape.member
            value = self._parse_shape(element_shape, node, element_name)
            if value is UNDEFINED:
                break
            parsed_list.append(value)
            i += 1
        return parsed_list if parsed_list != [] else UNDEFINED

    def _handle_map(self, shape, query_params, prefix=""):
        if self._is_shape_flattened(shape):
            full_prefix = prefix
        else:
            full_prefix = f"{prefix}.entry"
        template = full_prefix + ".{i}.{suffix}"
        key_shape = shape.key
        value_shape = shape.value
        key_suffix = self._get_serialized_name(key_shape, default_name="key")
        value_suffix = self._get_serialized_name(value_shape, "value")
        parsed_map = self.MAP_TYPE()
        i = 1
        while True:
            key_name = template.format(i=i, suffix=key_suffix)
            value_name = template.format(i=i, suffix=value_suffix)
            key = self._parse_shape(key_shape, query_params, key_name)
            value = self._parse_shape(value_shape, query_params, value_name)
            if key is UNDEFINED:
                break
            parsed_map[key] = value
            i += 1
        return parsed_map if parsed_map != {} else UNDEFINED

    def _handle_blob(self, shape, query_params, prefix=""):
        # Blob args must be base64 encoded.
        value = self._default_handle(shape, query_params, prefix)
        if value is UNDEFINED:
            return value
        return self._blob_parser(value)

    def _handle_timestamp(self, shape, query_params, prefix=""):
        value = self._default_handle(shape, query_params, prefix)
        if value is UNDEFINED:
            return value
        return self._timestamp_parser(value)
        # return self._convert_str_to_timestamp(value)

    def _handle_boolean(self, shape, query_params, prefix=""):
        value = self._default_handle(shape, query_params, prefix)
        if value is True or value is False:
            return value
        try:
            return value.lower() == "true"
        except AttributeError:
            pass
        return UNDEFINED

    def _handle_integer(self, shape, query_params, prefix=""):
        value = self._default_handle(shape, query_params, prefix)
        if value is UNDEFINED:
            return value
        return int(value)

    def _handle_float(self, shape, query_params, prefix=""):
        value = self._default_handle(shape, query_params, prefix)
        if value is UNDEFINED:
            return value
        return float(value)

    _handle_double = _handle_float
    _handle_long = _handle_integer

    def _default_handle(self, shape, value, prefix=""):
        default_value = shape.metadata.get("default", UNDEFINED)
        return value.get(prefix, default_value)

    def _get_serialized_name(self, shape, default_name):
        return shape.serialization.get("name", default_name)

    def _parsed_key_name(self, member_name):
        key_name = member_name
        # key_name = xform_name(key_name)
        return key_name

    def _is_shape_flattened(self, shape):
        return shape.serialization.get("flattened")


class EC2QueryParser(QueryParser):
    """EC2 specific customizations to the query protocol serializers.

    The EC2 model is almost, but not exactly, similar to the query protocol
    serializer.  This class encapsulates those differences.  The model
    will have be marked with a ``protocol`` of ``ec2``, so you don't need
    to worry about wiring this class up correctly.

    """

    def _get_serialized_name(self, shape, default_name):
        if "queryName" in shape.serialization:
            return shape.serialization["queryName"]
        elif "name" in shape.serialization:
            # A locationName is always capitalized
            # on input for the ec2 protocol.
            name = shape.serialization["name"]
            return name[0].upper() + name[1:]
        else:
            return default_name

    def _handle_list(self, shape, node, prefix=""):
        parsed_list = []
        i = 1
        while True:
            element_name = f"{prefix}.{i}"
            element_shape = shape.member
            value = self._parse_shape(element_shape, node, element_name)
            if value is UNDEFINED:
                break
            parsed_list.append(value)
            i += 1
        return parsed_list if parsed_list != [] else UNDEFINED


class BaseJSONParser(RequestParser):
    def _handle_structure(self, shape, value):
        if shape.is_document_type:
            final_parsed = value
        else:
            member_shapes = shape.members
            if value is None:
                # If the comes across the wire as "null" (None in python),
                # we should be returning this unchanged, instead of as an
                # empty dict.
                return None
            final_parsed = self.MAP_TYPE()
            for member_name in member_shapes:
                member_shape = member_shapes[member_name]
                json_name = member_shape.serialization.get("name", member_name)
                raw_value = value.get(json_name)
                if raw_value is not None:
                    final_parsed[member_name] = self._parse_shape(
                        member_shapes[member_name], raw_value
                    )
        return final_parsed

    def _handle_map(self, shape, value):
        parsed = self.MAP_TYPE()
        key_shape = shape.key
        value_shape = shape.value
        for key, value in value.items():
            actual_key = self._parse_shape(key_shape, key)
            actual_value = self._parse_shape(value_shape, value)
            parsed[actual_key] = actual_value
        return parsed

    def _handle_blob(self, shape, value):
        return self._blob_parser(value)

    def _handle_timestamp(self, shape, value):
        return self._timestamp_parser(value)

    def _handle_float(self, shape, value):
        if value is UNDEFINED:
            return value
        return float(value)

    _handle_double = _handle_float

    def _parse_body_as_json(self, body_contents):
        if not body_contents:
            return {}
        try:
            body = body_contents.decode(self.DEFAULT_ENCODING)
        except (UnicodeDecodeError, AttributeError):
            body = body_contents
        try:
            original_parsed = json.loads(body)
            return original_parsed
        except ValueError:
            # if the body cannot be parsed, include
            # the literal string as the message
            return {"message": body}


class JSONParser(BaseJSONParser):
    def _parse_action(self, request_dict):
        headers = request_dict["headers"]
        target = headers.get("X-Amz-Target", "UnknownOperation")
        target_prefix = self.service_model.metadata.get("targetPrefix")
        if target_prefix:
            target = target.replace(f"{target_prefix}.", "")
        return target

    def _do_parse(self, request_dict, shape):
        parsed = self.MAP_TYPE()
        if shape is not None:
            parsed = self._handle_json_body(request_dict["body"], shape)
        return parsed

    def _handle_json_body(self, raw_body, shape):
        # The json.loads() gives us the primitive JSON types,
        # but we need to traverse the parsed JSON data to convert
        # to richer types (blobs, timestamps, etc.
        parsed_json = self._parse_body_as_json(raw_body)
        return self._parse_shape(shape, parsed_json)


class BaseRestParser(RequestParser):
    def _parse_params(self, request_dict, operation_model):
        try:
            parsed_uri = urlparse(operation_model.http["requestUri"])
            uri_regexp = self.uri_to_regexp(parsed_uri.path)
            match = re.match(uri_regexp, request_dict["url_path"])
            self.uri_match = match
        except Exception:
            pass
        return super()._parse_params(request_dict, operation_model)

    def uri_to_regexp(self, uri):
        """converts uri w/ placeholder to regexp
        '/cars/{carName}/drivers/{DriverName}' -> '^/cars/.*/drivers/[^/]*$'
        '/cars/{carName}/drivers/{DriverName}/drive' -> '^/cars/.*/drivers/.*/drive$'
        """

        def _convert(elem, is_last):
            if not re.match("^{.*}$", elem):
                return elem
            greedy = "+" in elem
            name = (
                elem.replace("{", "")
                .replace("}", "")
                .replace("+", "")
                .replace("-", "_")
            )
            if is_last:
                capture = "[^/]" if not greedy else "."
                return f"(?P<{name}>{capture}+)"
            return f"(?P<{name}>[^/]*)"

        elems = uri.split("/")
        num_elems = len(elems)
        regexp = "^{}$".format(
            "/".join(
                [_convert(elem, (i == num_elems - 1)) for i, elem in enumerate(elems)]
            )
        )
        return regexp

    def _parse_action(self, request_dict):
        method_urls = defaultdict(lambda: defaultdict(str))
        op_names = self.service_model.operation_names
        for op_name in op_names:
            op_model = self.service_model.operation_model(op_name)
            _method = op_model.http["method"]
            parsed_uri = urlparse(op_model.http["requestUri"])
            uri_regexp = self.uri_to_regexp(parsed_uri.path)
            method_urls[_method][uri_regexp] = op_model.name
        regexp_and_names = method_urls[request_dict["method"]]
        for regexp, name in regexp_and_names.items():
            match = re.match(regexp, request_dict["url_path"])
            self.uri_match = match
            if match:
                return name
        return "UnknownOperation"

    def _do_parse(self, request_dict, shape):
        final_parsed = {}
        self._add_modeled_parse(request_dict, shape, final_parsed)
        return final_parsed

    def _add_modeled_parse(self, request_dict, shape, final_parsed):
        if shape is None:
            return final_parsed
        member_shapes = shape.members
        self._parse_non_payload_attrs(request_dict, shape, member_shapes, final_parsed)
        self._parse_payload(request_dict, shape, member_shapes, final_parsed)

    def _parse_payload(self, response, shape, member_shapes, final_parsed):
        if "payload" in shape.serialization:
            # If a payload is specified in the output shape, then only that
            # shape is used for the body payload.
            payload_member_name = shape.serialization["payload"]
            body_shape = member_shapes[payload_member_name]
            if body_shape.type_name in ["string", "blob"]:
                # This is a stream
                body = response["body"]
                if isinstance(body, bytes):
                    body = body.decode(self.DEFAULT_ENCODING)
                if body != "":
                    final_parsed[payload_member_name] = body
            else:
                original_parsed = self._initial_body_parse(response["body"])
                value = self._parse_shape(body_shape, original_parsed)
                # Payload for empty dict is <foo /> for XML but not for JSON...
                # Need to utilize subclasses here...
                # may have to clean this up with UNDEFINED vs if value...
                # For now do this isinstance hack that only returns {} if body not empty!
                if value or (
                    response["body"] and value == {} and isinstance(self, RestXMLParser)
                ):
                    final_parsed[payload_member_name] = value
        else:
            original_parsed = self._initial_body_parse(response["body"])
            body_parsed = self._parse_shape(shape, original_parsed)
            final_parsed.update(body_parsed)

    def _parse_non_payload_attrs(self, response, shape, member_shapes, final_parsed):
        headers = response["headers"]
        for name in member_shapes:
            member_shape = member_shapes[name]
            location = member_shape.serialization.get("location")
            if location is None:
                continue
            elif location == "headers":
                final_parsed[name] = self._parse_header_map(member_shape, headers)
            elif location == "header":
                header_name = member_shape.serialization.get("name", name)
                if header_name in headers:
                    final_parsed[name] = self._parse_shape(
                        member_shape, headers[header_name]
                    )
            elif location == "uri":
                member_name = member_shape.serialization.get("name", name)
                if self.uri_match:
                    try:
                        # TODO: Should this be here?!
                        from urllib import parse

                        value = self.uri_match.group(member_name)
                        value = parse.unquote(value)
                        final_parsed[name] = self._parse_shape(member_shape, value)
                    except IndexError:
                        # do nothing if param is not found
                        pass
            elif location == "querystring":
                qs = response["values"]
                member_name = member_shape.serialization.get("name", name)
                if member_shape.type_name == "list":
                    # this is for our poor man's multidict and then a real multidict
                    get = qs.get_list if hasattr(qs, "get_list") else qs.get
                    value = get(member_name, [])
                elif member_shape.type_name == "map":
                    value = qs
                    # Is this right?  Did this for rest-xml case "String to string maps in querystring"
                    final_parsed[name] = value
                    return
                else:
                    value = qs.get(member_name, None)
                if value is None:
                    return
                final_parsed[name] = self._parse_shape(member_shape, value)

    def _parse_header_map(self, shape, headers):
        # Note that headers are case-insensitive, so we .lower()
        # all header names and header prefixes.
        parsed = self.MAP_TYPE()
        prefix = shape.serialization.get("name", "").lower()
        for header_name in headers:
            if header_name.lower().startswith(prefix):
                # The key name inserted into the parsed hash
                # strips off the prefix.
                name = header_name[len(prefix) :]
                parsed[name] = headers[header_name]
        return parsed

    def _initial_body_parse(self, body_contents):
        # This method should do the initial xml/json parsing of the
        # body.  We still need to walk the parsed body in order
        # to convert types, but this method will do the first round
        # of parsing.
        raise NotImplementedError("_initial_body_parse")

    def _handle_list(self, shape, node):
        location = shape.serialization.get("location")
        if location == "header" and not isinstance(node, list):
            # List in headers may be a comma separated string as per RFC7230
            node = [e.strip() for e in node.split(",")]
        return super()._handle_list(shape, node)


class RestJSONParser(BaseRestParser, BaseJSONParser):
    def _initial_body_parse(self, body_contents):
        return self._parse_body_as_json(body_contents)

    # def _handle_integer(self, shape, value):
    #     return int(value)

    def _handle_string(self, shape, value):
        parsed = value
        if is_json_value_header(shape):
            decoded = base64.b64decode(value).decode(self.DEFAULT_ENCODING)
            parsed = json.loads(decoded)
        return parsed

    # Has to handle text from query string or JSON value
    def _handle_boolean(self, shape, value):
        if value is True or value is False:
            return value
        try:
            return value.lower() == "true"
        except AttributeError:
            pass
        return UNDEFINED

    # _handle_long = _handle_integer


class BaseXMLParser(RequestParser):
    _namespace_re = re.compile("{.*}")

    def _handle_map(self, shape, node):
        parsed = {}
        key_shape = shape.key
        value_shape = shape.value
        key_location_name = key_shape.serialization.get("name") or "key"
        value_location_name = value_shape.serialization.get("name") or "value"
        if shape.serialization.get("flattened") and not isinstance(node, list):
            node = [node]
        for keyval_node in node:
            for single_pair in keyval_node:
                # Within each <entry> there's a <key> and a <value>
                tag_name = self._node_tag(single_pair)
                if tag_name == key_location_name:
                    key_name = self._parse_shape(key_shape, single_pair)
                elif tag_name == value_location_name:
                    val_name = self._parse_shape(value_shape, single_pair)
                else:
                    raise ResponseParserError("Unknown tag: %s" % tag_name)
            parsed[key_name] = val_name
        return parsed

    def _node_tag(self, node):
        return self._namespace_re.sub("", node.tag)

    def _handle_list(self, shape, node):
        # When we use _build_name_to_xml_node, repeated elements are aggregated
        # into a list.  However, we can't tell the difference between a scalar
        # value and a single element flattened list.  So before calling the
        # real _handle_list, we know that "node" should actually be a list if
        # it's flattened, and if it's not, then we make it a one element list.
        if shape.serialization.get("flattened") and not isinstance(node, list):
            node = [node]
        return super()._handle_list(shape, node)

    def _handle_structure(self, shape, node):
        parsed = {}
        members = shape.members
        xml_dict = self._build_name_to_xml_node(node)
        for member_name in members:
            member_shape = members[member_name]
            if (
                "location" in member_shape.serialization
                or member_shape.serialization.get("eventheader")
            ):
                # All members with locations have already been handled,
                # so we don't need to parse these members.
                continue
            xml_name = self._member_key_name(member_shape, member_name)
            member_node = xml_dict.get(xml_name)
            if member_node is not None:
                parsed[member_name] = self._parse_shape(member_shape, member_node)
            elif member_shape.serialization.get("xmlAttribute"):
                attribs = {}
                location_name = member_shape.serialization["name"]
                for key, value in node.attrib.items():
                    new_key = self._namespace_re.sub(
                        location_name.split(":")[0] + ":", key
                    )
                    attribs[new_key] = value
                if location_name in attribs:
                    parsed[member_name] = attribs[location_name]
        return parsed

    def _member_key_name(self, shape, member_name):
        # This method is needed because we have to special case flattened list
        # with a serialization name.  If this is the case we use the
        # locationName from the list's member shape as the key name for the
        # surrounding structure.
        if shape.type_name == "list" and shape.serialization.get("flattened"):
            list_member_serialized_name = shape.member.serialization.get("name")
            if list_member_serialized_name is not None:
                return list_member_serialized_name
        serialized_name = shape.serialization.get("name")
        if serialized_name is not None:
            return serialized_name
        return member_name

    def _build_name_to_xml_node(self, parent_node):
        # If the parent node is actually a list. We should not be trying
        # to serialize it to a dictionary. Instead, return the first element
        # in the list.
        if isinstance(parent_node, list):
            return self._build_name_to_xml_node(parent_node[0])
        xml_dict = {}
        for item in parent_node:
            key = self._node_tag(item)
            if key in xml_dict:
                # If the key already exists, the most natural
                # way to handle this is to aggregate repeated
                # keys into a single list.
                # <foo>1</foo><foo>2</foo> -> {'foo': [Node(1), Node(2)]}
                if isinstance(xml_dict[key], list):
                    xml_dict[key].append(item)
                else:
                    # Convert from a scalar to a list.
                    xml_dict[key] = [xml_dict[key], item]
            else:
                xml_dict[key] = item
        return xml_dict

    def _parse_xml_string_to_dom(self, xml_string):
        try:
            parser = ETree.XMLParser(
                target=ETree.TreeBuilder(), encoding=self.DEFAULT_ENCODING
            )
            parser.feed(xml_string)
            root = parser.close()
        except XMLParseError as e:
            raise ResponseParserError(
                "Unable to parse response (%s), "
                "invalid XML received. Further retries may succeed:\n%s"
                % (e, xml_string)
            )
        return root

    @_text_content
    def _handle_boolean(self, shape, value):
        if value == "true":
            return True
        else:
            return False

    @_text_content
    def _handle_float(self, shape, text):
        return float(text)

    @_text_content
    def _handle_timestamp(self, shape, text):
        return self._timestamp_parser(text)

    @_text_content
    def _handle_integer(self, shape, text):
        return int(text)

    @_text_content
    def _handle_string(self, shape, text):
        parsed = text
        # This if might be duplicated in JSON parser - can we consolidate?
        if is_json_value_header(shape):
            decoded = base64.b64decode(text).decode(self.DEFAULT_ENCODING)
            parsed = json.loads(decoded)
        return parsed

    @_text_content
    def _handle_blob(self, shape, text):
        return self._blob_parser(text)

    _handle_character = _handle_string
    _handle_double = _handle_float
    _handle_long = _handle_integer


class RestXMLParser(BaseRestParser, BaseXMLParser):
    def _initial_body_parse(self, body_contents):
        if not body_contents:
            return ETree.Element("")
        return self._parse_xml_string_to_dom(body_contents)

    @_text_content
    def _handle_string(self, shape, text):
        text = super()._handle_string(shape, text)
        return text


PROTOCOL_PARSERS = {
    "ec2": EC2QueryParser,
    "json": JSONParser,
    "query": QueryParser,
    "rest-json": RestJSONParser,
    "rest-xml": RestXMLParser,
}
