from moto.core.exceptions import ServiceException


class AppSyncExceptions(ServiceException):
    pass


class AWSValidationException(AppSyncExceptions):
    code = "ValidationException"


class ApiKeyValidityOutOfBoundsException(AppSyncExceptions):
    code = "ApiKeyValidityOutOfBoundsException"


class GraphqlAPINotFound(AppSyncExceptions):
    code = "NotFoundException"

    def __init__(self, api_id: str):
        super().__init__(f"GraphQL API {api_id} not found.")


class GraphQLSchemaException(AppSyncExceptions):
    code = "GraphQLSchemaException"


class GraphqlAPICacheNotFound(AppSyncExceptions):
    code = "NotFoundException"

    def __init__(self, op: str):
        super().__init__(
            f"Unable to {op} the cache as it doesn't exist, please create the cache first."
        )


class EventsAPINotFound(AppSyncExceptions):
    code = "NotFoundException"

    def __init__(self, api_id: str):
        super().__init__(f"Events API {api_id} not found.")


class BadRequestException(AppSyncExceptions):
    code = "BadRequestException"
