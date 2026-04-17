"""Handles incoming apigatewaymanagementapi requests, invokes methods, returns responses."""

import json
from typing import Any

from moto.core.responses import TYPE_RESPONSE, BaseResponse

from .models import ApiGatewayManagementApiBackend, apigatewaymanagementapi_backends


class ApiGatewayManagementApiResponse(BaseResponse):
    """Handler for ApiGatewayManagementApi requests and responses."""

    def __init__(self) -> None:
        super().__init__(service_name="apigatewaymanagementapi")
        self.automated_parameter_parsing = True

    def setup_class(
        self, request: Any, full_url: str, headers: Any, use_raw_body: bool = False
    ) -> None:
        super().setup_class(request, full_url, headers, use_raw_body=True)

    @property
    def apigatewaymanagementapi_backend(self) -> ApiGatewayManagementApiBackend:
        """Return backend instance specific for this region."""
        return apigatewaymanagementapi_backends[self.current_account][self.region]

    def delete_connection(self) -> str:
        connection_id = self._get_param("ConnectionId")
        self.apigatewaymanagementapi_backend.delete_connection(
            connection_id=connection_id
        )
        return "{}"

    def get_connection(self) -> str:
        connection_id = self._get_param("ConnectionId")
        connection = self.apigatewaymanagementapi_backend.get_connection(
            connection_id=connection_id
        )
        return json.dumps(connection.to_dict())

    def post_to_connection(self) -> str:
        connection_id = self._get_param("ConnectionId")
        data = self._get_param("Data")
        if isinstance(data, str):
            data = data.encode("utf-8")
        self.apigatewaymanagementapi_backend.post_to_connection(
            data=data,
            connection_id=connection_id,
        )
        return "{}"

    @staticmethod
    def connect_to_apigateway(  # type: ignore[misc]
        request: Any, full_url: str, headers: Any
    ) -> TYPE_RESPONSE:
        self = ApiGatewayManagementApiResponse()
        self.setup_class(request, full_url, headers, use_raw_body=True)
        # Extract connectionId from path for requests that include a stage prefix
        # (automated parsing won't match these URLs against the botocore model)
        connection_id = self.path.split("/@connections/")[-1]
        if not hasattr(self, "params") or not self.params:
            self.params = {}
        self.params["ConnectionId"] = connection_id
        self.params["Data"] = self.body
        if request.method == "GET":
            return 200, {}, self.get_connection()
        elif request.method == "DELETE":
            return 200, {}, self.delete_connection()
        else:
            return 200, {}, self.post_to_connection()
