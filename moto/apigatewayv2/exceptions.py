from moto.core.exceptions import ServiceException


class APIGatewayV2Error(ServiceException):
    pass


class ApiNotFound(APIGatewayV2Error):
    code = "NotFoundException"

    def __init__(self, api_id: str):
        super().__init__(f"Invalid API identifier specified {api_id}")


class AuthorizerNotFound(APIGatewayV2Error):
    code = "NotFoundException"

    def __init__(self, authorizer_id: str):
        super().__init__(f"Invalid Authorizer identifier specified {authorizer_id}")


class ModelNotFound(APIGatewayV2Error):
    code = "NotFoundException"

    def __init__(self, model_id: str):
        super().__init__(f"Invalid Model identifier specified {model_id}")


class RouteResponseNotFound(APIGatewayV2Error):
    code = "NotFoundException"

    def __init__(self, rr_id: str):
        super().__init__(f"Invalid RouteResponse identifier specified {rr_id}")


class BadRequestException(APIGatewayV2Error):
    code = "BadRequestException"


class IntegrationNotFound(APIGatewayV2Error):
    code = "NotFoundException"

    def __init__(self, integration_id: str):
        super().__init__(f"Invalid Integration identifier specified {integration_id}")


class IntegrationResponseNotFound(APIGatewayV2Error):
    code = "NotFoundException"

    def __init__(self, int_res_id: str):
        super().__init__(
            f"Invalid IntegrationResponse identifier specified {int_res_id}"
        )


class RouteNotFound(APIGatewayV2Error):
    code = "NotFoundException"

    def __init__(self, route_id: str):
        super().__init__(f"Invalid Route identifier specified {route_id}")


class VpcLinkNotFound(APIGatewayV2Error):
    code = "NotFoundException"

    def __init__(self, vpc_link_id: str):
        super().__init__(f"Invalid VpcLink identifier specified {vpc_link_id}")


class UnknownProtocol(APIGatewayV2Error):
    code = "BadRequestException"
    message = "Invalid protocol specified. Must be one of [HTTP, WEBSOCKET]"


class DomainNameNotFound(APIGatewayV2Error):
    code = "NotFoundException"
    message = "The domain name resource specified in the request was not found."


class DomainNameAlreadyExists(APIGatewayV2Error):
    code = "ConflictException"
    message = "The domain name resource already exists."


class ApiMappingNotFound(APIGatewayV2Error):
    code = "NotFoundException"
    message = "The api mapping resource specified in the request was not found."


class StageNotFound(APIGatewayV2Error):
    code = "NotFoundException"
    message = "Invalid stage identifier specified"
