import base64
import json
import re
import sys
from typing import Any, Union

from moto.core.responses import TYPE_RESPONSE, ActionResult, BaseResponse
from moto.utilities.aws_headers import amz_crc32
from moto.utilities.utils import ARN_PARTITION_REGEX

from .exceptions import FunctionAlreadyExists, UnknownFunctionException
from .models import LambdaBackend
from .utils import get_backend


class LambdaResponse(BaseResponse):
    def __init__(self) -> None:
        super().__init__(service_name="awslambda")
        self.automated_parameter_parsing = True

    @property
    def backend(self) -> LambdaBackend:
        return get_backend(self.current_account, self.region)

    @staticmethod
    def _re_encode_zipfile(params: dict[str, Any]) -> dict[str, Any]:
        """Re-encode blob fields back to base64 strings.

        The automated parameter parser decodes blob types from base64 to bytes,
        but the backend expects base64-encoded strings for ZipFile fields.
        """
        for key in ("Code", "Content"):
            if key in params and isinstance(params[key], dict):
                if "ZipFile" in params[key] and isinstance(
                    params[key]["ZipFile"], bytes
                ):
                    params[key]["ZipFile"] = base64.b64encode(
                        params[key]["ZipFile"]
                    ).decode("utf-8")
        if "ZipFile" in params and isinstance(params["ZipFile"], bytes):
            params["ZipFile"] = base64.b64encode(params["ZipFile"]).decode("utf-8")
        return params

    def add_permission(self) -> str:
        function_name = self._get_param("FunctionName")
        qualifier = self._get_param("Qualifier")
        statement = self.body
        statement = self.backend.add_permission(function_name, qualifier, statement)
        return json.dumps({"Statement": json.dumps(statement)})

    def get_policy(self) -> str:
        function_name = self._get_param("FunctionName")
        qualifier = self._get_param("Qualifier")
        return self.backend.get_policy(function_name, qualifier)

    def remove_permission(self) -> TYPE_RESPONSE:
        function_name = self._get_param("FunctionName")
        statement_id = self._get_param("StatementId")
        revision = self._get_param("RevisionId", "")
        if self.backend.get_function(function_name):
            self.backend.remove_permission(function_name, statement_id, revision)
            return 204, {"status": 204}, "{}"
        else:
            return 404, {"status": 404}, "{}"

    @amz_crc32
    def invoke(self) -> tuple[int, dict[str, str], Union[str, bytes]]:
        response_headers: dict[str, str] = {}

        function_name = self._get_param("FunctionName")
        qualifier = self._get_param("Qualifier")

        payload = self.backend.invoke(
            function_name, qualifier, self.body, self.headers, response_headers
        )
        if payload is not None:
            if self.headers.get("X-Amz-Invocation-Type") != "Event":
                if sys.getsizeof(payload) > 6000000:
                    response_headers["Content-Length"] = "142"
                    response_headers["x-amz-function-error"] = "Unhandled"
                    error_dict = {
                        "errorMessage": "Response payload size exceeded maximum allowed payload size (6291556 bytes).",
                        "errorType": "Function.ResponseSizeTooLarge",
                    }
                    payload = json.dumps(error_dict).encode("utf-8")

            response_headers["content-type"] = "application/json"
            if self.headers.get("X-Amz-Invocation-Type") == "Event":
                status_code = 202
                response_headers["status"] = "202"
            elif self.headers.get("X-Amz-Invocation-Type") == "DryRun":
                status_code = 204
                response_headers["status"] = "204"
            else:
                if (
                    self.headers.get("X-Amz-Log-Type") != "Tail"
                    and "x-amz-log-result" in response_headers
                ):
                    del response_headers["x-amz-log-result"]
                status_code = 200
            return status_code, response_headers, payload
        else:
            return 404, response_headers, "{}"

    @amz_crc32
    def invoke_async(self) -> tuple[int, dict[str, str], Union[str, bytes]]:
        response_headers: dict[str, Any] = {}

        function_name = self._get_param("FunctionName")

        fn = self.backend.get_function(function_name, None)
        payload = fn.invoke(self.body, self.headers, response_headers)
        response_headers["Content-Length"] = str(len(payload))
        response_headers["status"] = 202
        return 202, response_headers, payload

    def list_functions(self) -> str:
        func_version = self._get_param("FunctionVersion")
        result: dict[str, list[dict[str, Any]]] = {"Functions": []}

        for fn in self.backend.list_functions(func_version):
            json_data = fn.get_configuration()
            result["Functions"].append(json_data)

        return json.dumps(result)

    def list_versions_by_function(self) -> str:
        function_name = self._get_param("FunctionName")
        result: dict[str, Any] = {"Versions": []}

        functions = self.backend.list_versions_by_function(function_name)
        for fn in functions:
            json_data = fn.get_configuration()
            result["Versions"].append(json_data)

        return json.dumps(result)

    def list_aliases(self) -> TYPE_RESPONSE:
        function_name = self._get_param("FunctionName")
        result: dict[str, Any] = {"Aliases": []}

        aliases = self.backend.list_aliases(function_name)
        for alias in aliases:
            json_data = alias.to_json()
            result["Aliases"].append(json_data)

        return 200, {}, json.dumps(result)

    def create_function(self) -> TYPE_RESPONSE:
        params = self._re_encode_zipfile(self._get_params())
        function_name = params["FunctionName"].rsplit(":", 1)[-1]
        try:
            self.backend.get_function(function_name, None)
        except UnknownFunctionException:
            fn = self.backend.create_function(params)
            config = fn.get_configuration(on_create=True)
            return 201, {"status": 201}, json.dumps(config)
        raise FunctionAlreadyExists(function_name)

    def create_function_url_config(self) -> TYPE_RESPONSE:
        function_name = self._get_param("FunctionName")
        config = self.backend.create_function_url_config(
            function_name, self._get_params()
        )
        return 201, {"status": 201}, json.dumps(config.to_dict())

    def delete_function_url_config(self) -> TYPE_RESPONSE:
        function_name = self._get_param("FunctionName")
        self.backend.delete_function_url_config(function_name)
        return 204, {"status": 204}, "{}"

    def get_function_url_config(self) -> TYPE_RESPONSE:
        function_name = self._get_param("FunctionName")
        config = self.backend.get_function_url_config(function_name)
        return 201, {"status": 201}, json.dumps(config.to_dict())

    def update_function_url_config(self) -> str:
        function_name = self._get_param("FunctionName")
        config = self.backend.update_function_url_config(
            function_name, self._get_params()
        )
        return json.dumps(config.to_dict())

    def create_event_source_mapping(self) -> TYPE_RESPONSE:
        fn = self.backend.create_event_source_mapping(self._get_params())
        config = fn.get_configuration()
        return 201, {"status": 201}, json.dumps(config)

    def list_event_source_mappings(self) -> str:
        event_source_arn = self._get_param("EventSourceArn")
        function_name = self._get_param("FunctionName")
        esms = self.backend.list_event_source_mappings(event_source_arn, function_name)
        result = {"EventSourceMappings": [esm.get_configuration() for esm in esms]}
        return json.dumps(result)

    def get_event_source_mapping(self) -> TYPE_RESPONSE:
        uuid = self._get_param("UUID")
        result = self.backend.get_event_source_mapping(uuid)
        if result:
            return 200, {}, json.dumps(result.get_configuration())
        else:
            err = {
                "Type": "User",
                "Message": "The resource you requested does not exist.",
            }
            headers = {"x-amzn-errortype": "ResourceNotFoundException", "status": 404}
            return 404, headers, json.dumps(err)

    def update_event_source_mapping(self) -> TYPE_RESPONSE:
        uuid = self._get_param("UUID")
        result = self.backend.update_event_source_mapping(uuid, self._get_params())
        if result:
            return 202, {"status": 202}, json.dumps(result.get_configuration())
        else:
            return 404, {}, "{}"

    def delete_event_source_mapping(self) -> TYPE_RESPONSE:
        uuid = self._get_param("UUID")
        esm = self.backend.delete_event_source_mapping(uuid)
        if esm:
            json_result = esm.get_configuration()
            json_result.update({"State": "Deleting"})
            return 202, {"status": 202}, json.dumps(json_result)
        else:
            return 404, {}, "{}"

    def publish_version(self) -> TYPE_RESPONSE:
        function_name = self._get_param("FunctionName")
        description = self._get_param("Description")

        fn = self.backend.publish_version(function_name, description)
        config = fn.get_configuration()  # type: ignore[union-attr]
        return 201, {"status": 201}, json.dumps(config)

    def delete_function(self) -> TYPE_RESPONSE:
        function_name = self._get_param("FunctionName")
        qualifier = self._get_param("Qualifier", None)

        self.backend.delete_function(function_name, qualifier)
        return 204, {"status": 204}, ""

    @staticmethod
    def _set_configuration_qualifier(  # type: ignore[misc]
        configuration: dict[str, Any], function_name: str, qualifier: str
    ) -> dict[str, Any]:
        # Qualifier may be explicitly passed or part of function name or ARN, extract it here
        if re.match(ARN_PARTITION_REGEX, function_name):
            # Extract from ARN
            if ":" in function_name.split(":function:")[-1]:
                qualifier = function_name.split(":")[-1]
        else:
            # Extract from function name
            if ":" in function_name:
                qualifier = function_name.split(":")[1]

        if qualifier is None or qualifier == "$LATEST":
            configuration["Version"] = "$LATEST"
        if qualifier == "$LATEST":
            configuration["FunctionArn"] += ":$LATEST"
        return configuration

    def get_function(self) -> str:
        function_name = self._get_param("FunctionName")
        qualifier = self._get_param("Qualifier", None)

        fn = self.backend.get_function(function_name, qualifier)

        code = fn.get_code()
        code["Configuration"] = self._set_configuration_qualifier(
            code["Configuration"], function_name, qualifier
        )
        return json.dumps(code)

    def get_function_configuration(self) -> str:
        function_name = self._get_param("FunctionName")
        qualifier = self._get_param("Qualifier", None)

        fn = self.backend.get_function(function_name, qualifier)

        resp = self._set_configuration_qualifier(
            fn.get_configuration(), function_name, qualifier
        )
        return json.dumps(resp)

    def _get_aws_region(self, full_url: str) -> str:
        region = self.region_regex.search(full_url)
        if region:
            return region.group(1)
        else:
            return self.default_region

    def list_tags(self) -> str:
        function_arn = self._get_param("Resource")

        tags = self.backend.list_tags(function_arn)
        return json.dumps({"Tags": tags})

    def tag_resource(self) -> str:
        function_arn = self._get_param("Resource")

        self.backend.tag_resource(function_arn, self._get_param("Tags"))
        return "{}"

    def untag_resource(self) -> TYPE_RESPONSE:
        function_arn = self._get_param("Resource")
        tag_keys = self._get_param("TagKeys")

        self.backend.untag_resource(function_arn, tag_keys)
        return 204, {"status": 204}, "{}"

    def update_function_configuration(self) -> TYPE_RESPONSE:
        function_name = self._get_param("FunctionName")
        qualifier = self._get_param("Qualifier")
        resp = self.backend.update_function_configuration(
            function_name, qualifier, body=self._get_params()
        )

        if resp:
            return 200, {}, json.dumps(resp)
        else:
            return 404, {"status": 404}, "{}"

    def update_function_code(self) -> TYPE_RESPONSE:
        function_name = self._get_param("FunctionName")
        qualifier = self._get_param("Qualifier")
        resp = self.backend.update_function_code(
            function_name, qualifier, body=self._re_encode_zipfile(self._get_params())
        )

        if resp:
            return 200, {}, json.dumps(resp)
        else:
            return 404, {"status": 404}, "{}"

    def get_function_code_signing_config(self) -> str:
        function_name = self._get_param("FunctionName")
        resp = self.backend.get_function_code_signing_config(function_name)
        return json.dumps(resp)

    def get_function_concurrency(self) -> TYPE_RESPONSE:
        function_name = self._get_param("FunctionName")
        self.backend.get_function(function_name)

        resp = self.backend.get_function_concurrency(function_name)
        return 200, {}, json.dumps({"ReservedConcurrentExecutions": resp})

    def delete_function_concurrency(self) -> TYPE_RESPONSE:
        function_name = self._get_param("FunctionName")
        self.backend.get_function(function_name)

        self.backend.delete_function_concurrency(function_name)

        return 204, {"status": 204}, "{}"

    def put_function_concurrency(self) -> TYPE_RESPONSE:
        function_name = self._get_param("FunctionName")
        self.backend.get_function(function_name)

        concurrency = self._get_param("ReservedConcurrentExecutions")
        resp = self.backend.put_function_concurrency(function_name, concurrency)

        return 200, {}, json.dumps({"ReservedConcurrentExecutions": resp})

    def list_layers(self) -> str:
        layers = self.backend.list_layers()
        return json.dumps({"Layers": layers})

    def delete_layer_version(self) -> str:
        layer_name = self._get_param("LayerName")
        layer_version = self._get_param("VersionNumber")
        self.backend.delete_layer_version(layer_name, layer_version)
        return "{}"

    def get_layer_version(self) -> str:
        layer_name = self._get_param("LayerName")
        layer_version = self._get_param("VersionNumber")
        layer = self.backend.get_layer_version(layer_name, layer_version)
        return json.dumps(layer.get_layer_version())

    def list_layer_versions(self) -> str:
        layer_name = self._get_param("LayerName")
        layer_versions = self.backend.list_layer_versions(layer_name)
        layer_versions = sorted(layer_versions, key=lambda lv: lv.version, reverse=True)
        return json.dumps(
            {"LayerVersions": [lv.get_layer_version() for lv in layer_versions]}
        )

    def publish_layer_version(self) -> TYPE_RESPONSE:
        spec = self._re_encode_zipfile(self._get_params())
        layer_version = self.backend.publish_layer_version(spec)
        config = layer_version.get_layer_version()
        return 201, {"status": 201}, json.dumps(config)

    def create_alias(self) -> TYPE_RESPONSE:
        function_name = self._get_param("FunctionName")
        alias_name = self._get_param("Name")
        description = self._get_param("Description", "")
        function_version = self._get_param("FunctionVersion")
        routing_config = self._get_param("RoutingConfig")
        alias = self.backend.create_alias(
            name=alias_name,
            function_name=function_name,
            function_version=function_version,
            description=description,
            routing_config=routing_config,
        )
        return 201, {"status": 201}, json.dumps(alias.to_json())

    def delete_alias(self) -> TYPE_RESPONSE:
        function_name = self._get_param("FunctionName")
        alias_name = self._get_param("Name")
        self.backend.delete_alias(name=alias_name, function_name=function_name)
        return 201, {"status": 201}, "{}"

    def get_alias(self) -> TYPE_RESPONSE:
        function_name = self._get_param("FunctionName")
        alias_name = self._get_param("Name")
        alias = self.backend.get_alias(name=alias_name, function_name=function_name)
        return 201, {"status": 201}, json.dumps(alias.to_json())

    def update_alias(self) -> TYPE_RESPONSE:
        function_name = self._get_param("FunctionName")
        alias_name = self._get_param("Name")
        description = self._get_param("Description")
        function_version = self._get_param("FunctionVersion")
        routing_config = self._get_param("RoutingConfig")
        alias = self.backend.update_alias(
            name=alias_name,
            function_name=function_name,
            function_version=function_version,
            description=description,
            routing_config=routing_config,
        )
        return 201, {"status": 201}, json.dumps(alias.to_json())

    def put_function_event_invoke_config(self) -> ActionResult:
        function_name = self._get_param("FunctionName")
        response = self.backend.put_function_event_invoke_config(
            function_name, self._get_params()
        )
        return ActionResult(response)

    def get_function_event_invoke_config(self) -> ActionResult:
        function_name = self._get_param("FunctionName")
        response = self.backend.get_function_event_invoke_config(function_name)
        return ActionResult(response)

    def delete_function_event_invoke_config(self) -> TYPE_RESPONSE:
        function_name = self._get_param("FunctionName")
        self.backend.delete_function_event_invoke_config(function_name)
        return 204, {"status": 204}, json.dumps({})

    def update_function_event_invoke_config(self) -> str:
        function_name = self._get_param("FunctionName")
        response = self.backend.update_function_event_invoke_config(
            function_name, self._get_params()
        )
        return json.dumps(response)

    def list_function_event_invoke_configs(self) -> str:
        function_name = self._get_param("FunctionName")
        return json.dumps(
            self.backend.list_function_event_invoke_configs(function_name)
        )

    def add_layer_version_permission(self) -> str:
        statement = self.body
        layer_name = self._get_param("LayerName")
        version_number = self._get_param("VersionNumber")
        statement, revision_id = self.backend.add_layer_version_permission(
            layer_name=layer_name,
            version_number=version_number,
            statement=statement,
        )
        return json.dumps(
            {"Statement": json.dumps(statement), "RevisionId": revision_id}
        )

    def get_layer_version_policy(self) -> str:
        layer_name = self._get_param("LayerName")
        version_number = self._get_param("VersionNumber")
        return self.backend.get_layer_version_policy(
            layer_name=layer_name, version_number=version_number
        )

    def remove_layer_version_permission(self) -> TYPE_RESPONSE:
        layer_name = self._get_param("LayerName")
        version_number = self._get_param("VersionNumber")
        statement_id = self._get_param("StatementId")
        revision = self._get_param("RevisionId", "")
        if self.backend.get_layer_version(layer_name, version_number):
            self.backend.remove_layer_version_permission(
                layer_name, version_number, statement_id, revision
            )
            return 204, {"status": 204}, "{}"
        else:
            return 404, {"status": 404}, "{}"
