"""Handles incoming dsql requests, invokes methods, returns responses."""

from urllib.parse import unquote

from moto.core.responses import ActionResult, BaseResponse

from .models import AuroraDSQLBackend, dsql_backends


class AuroraDSQLResponse(BaseResponse):
    """Handler for AuroraDSQL requests and responses."""

    def __init__(self) -> None:
        super().__init__(service_name="dsql")
        self.automated_parameter_parsing = True

    @property
    def dsql_backend(self) -> AuroraDSQLBackend:
        """Return backend instance specific for this region."""
        return dsql_backends[self.current_account][self.region]

    def create_cluster(self) -> ActionResult:
        params = self._get_params()
        deletion_protection_enabled = params.get("deletionProtectionEnabled", True)
        tags = params.get("tags")
        client_token = params.get("clientToken")
        cluster = self.dsql_backend.create_cluster(
            deletion_protection_enabled=deletion_protection_enabled,
            tags=tags,
            client_token=client_token,
        )
        return ActionResult(cluster)

    def delete_cluster(self) -> ActionResult:
        identifier = self._get_param("identifier")
        cluster = self.dsql_backend.delete_cluster(identifier=identifier)
        result = {
            "identifier": cluster.identifier,
            "arn": cluster.arn,
            "status": "DELETING",
            "creationTime": cluster.creation_time,
        }
        return ActionResult(result)

    def get_cluster(self) -> ActionResult:
        identifier = self._get_param("identifier")
        cluster = self.dsql_backend.get_cluster(identifier=identifier)
        return ActionResult(cluster)

    def get_vpc_endpoint_service_name(self) -> ActionResult:
        identifier = self._get_param("identifier")
        result = self.dsql_backend.get_vpc_endpoint_service_name(identifier)
        return ActionResult(result)

    def list_tags_for_resource(self) -> ActionResult:
        resource_arn = unquote(self._get_param("resourceArn"))
        identifier = resource_arn.split("/")[-1]
        tags = self.dsql_backend.list_tags_for_resource(identifier)
        return ActionResult({"tags": tags})
