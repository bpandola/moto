from moto.core.exceptions import MotoServiceException


class NotFoundException(MotoServiceException):
    code = "NotFoundException"


class UnknownBroker(NotFoundException):
    message = "Can't find requested broker [{broker_id}]. Make sure your broker exists."

    def __init__(self, broker_id: str):
        super().__init__(broker_id=broker_id)
        self.error_attribute = "broker-id"


class UnknownConfiguration(NotFoundException):
    message = "Can't find requested configuration [{config_id}]. Make sure your configuration exists."

    def __init__(self, config_id: str):
        super().__init__(config_id=config_id)
        self.error_attribute = "configuration_id"


class UnknownUser(NotFoundException):
    message = "Can't find requested user [{username}]. Make sure your user exists."

    def __init__(self, username: str):
        super().__init__(username=username)
        self.error_attribute = "username"


class BadRequestException(MotoServiceException):
    code = "BadRequestException"


class UnknownEngineType(BadRequestException):
    message = (
        "Broker engine type [{engine_type}] is invalid. Valid values are: [ACTIVEMQ]"
    )

    def __init__(self, engine_type: str):
        super().__init__(engine_type=engine_type)
        self.error_attribute = "engineType"
