"""Exceptions raised by the appmesh service."""

from moto.core.exceptions import ServiceException


class MeshError(ServiceException):
    pass


class MeshNotFoundError(MeshError):
    code = "MeshNotFound"

    def __init__(self, mesh_name: str) -> None:
        super().__init__(
            f"There are no meshes with the name {mesh_name}.",
        )


class ResourceNotFoundError(MeshError):
    code = "ResourceNotFound"

    def __init__(self, resource_arn: str) -> None:
        super().__init__(
            f"There are no mesh resources with the arn {resource_arn}.",
        )


class MeshOwnerDoesNotMatchError(MeshError):
    code = "MeshOwnerDoesNotMatch"

    def __init__(self, mesh_name: str, mesh_owner: str) -> None:
        super().__init__(
            f"The owner of the mesh {mesh_name} does not match the owner name provided: {mesh_owner}.",
        )


class VirtualRouterNameAlreadyTakenError(MeshError):
    code = "VirtualRouterNameAlreadyTaken"

    def __init__(self, mesh_name: str, virtual_router_name: str) -> None:
        super().__init__(
            f"There is already a virtual router named {virtual_router_name} associated with the mesh {mesh_name}.",
        )


class VirtualRouterNotFoundError(MeshError):
    code = "VirtualRouterNotFound"

    def __init__(self, mesh_name: str, virtual_router_name: str) -> None:
        super().__init__(
            f"The mesh {mesh_name} does not have a virtual router named {virtual_router_name}.",
        )


class RouteNotFoundError(MeshError):
    code = "RouteNotFound"

    def __init__(
        self, mesh_name: str, virtual_router_name: str, route_name: str
    ) -> None:
        super().__init__(
            f"There is no route named {route_name} associated with router {virtual_router_name} in mesh {mesh_name}.",
        )


class RouteNameAlreadyTakenError(MeshError):
    code = "RouteNameAlreadyTaken"

    def __init__(
        self, mesh_name: str, virtual_router_name: str, route_name: str
    ) -> None:
        super().__init__(
            f"There is already a route named {route_name} associated with router {virtual_router_name} in mesh {mesh_name}.",
        )


class MissingRequiredFieldError(MeshError):
    code = "MissingRequiredField"

    def __init__(self, field_name: str) -> None:
        super().__init__(
            f"{field_name} must be defined.",
        )


class VirtualNodeNotFoundError(MeshError):
    code = "VirtualNodeNotFound"

    def __init__(self, mesh_name: str, virtual_node_name: str) -> None:
        super().__init__(
            f"{virtual_node_name} is not a virtual node associated with mesh {mesh_name}",
        )


class VirtualNodeNameAlreadyTakenError(MeshError):
    code = "VirtualNodeNameAlreadyTaken"

    def __init__(self, mesh_name: str, virtual_node_name: str) -> None:
        super().__init__(
            f"There is already a virtual node named {virtual_node_name} associated with mesh {mesh_name}",
        )


class VirtualGatewayNotFoundError(MeshError):
    code = "VirtualGatewayNotFound"

    def __init__(self, mesh_name: str, virtual_gateway_name: str) -> None:
        super().__init__(
            f"{virtual_gateway_name} is not a virtual gateway associated with mesh {mesh_name}",
        )


class VirtualGatewayNameAlreadyTakenError(MeshError):
    code = "VirtualGatewayNameAlreadyTaken"

    def __init__(self, mesh_name: str, virtual_gateway_name: str) -> None:
        super().__init__(
            f"There is already a virtual gateway named {virtual_gateway_name} associated with mesh {mesh_name}",
        )


class GatewayRouteNotFoundError(MeshError):
    code = "GatewayRouteNotFound"

    def __init__(
        self, mesh_name: str, virtual_gateway_name: str, gateway_route_name: str
    ) -> None:
        super().__init__(
            f"There is no gateway route named {gateway_route_name} associated with gateway {virtual_gateway_name} in mesh {mesh_name}.",
        )


class GatewayRouteNameAlreadyTakenError(MeshError):
    code = "GatewayRouteNameAlreadyTaken"

    def __init__(
        self, mesh_name: str, virtual_gateway_name: str, gateway_route_name: str
    ) -> None:
        super().__init__(
            f"There is already a gateway route named {gateway_route_name} associated with gateway {virtual_gateway_name} in mesh {mesh_name}.",
        )
