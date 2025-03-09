from moto.core.exceptions import MotoCoreError


class MQError(MotoCoreError):
    pass


class UnknownBroker(MQError):
    code = "NotFoundException"
    fmt = "Can't find requested broker [{broker_id}]. Make sure your broker exists."

    def __init__(self, broker_id: str):
        super().__init__(broker_id=broker_id)
        self.error_attribute = "broker-id"


class UnknownConfiguration(MQError):
    code = "NotFoundException"
    fmt = "Can't find requested configuration [{config_id}]. Make sure your configuration exists."

    def __init__(self, config_id: str):
        super().__init__(config_id=config_id)
        self.error_attribute = "configuration_id"


class UnknownUser(MQError):
    code = "NotFoundException"
    fmt = "Can't find requested user [{username}]. Make sure your user exists."

    def __init__(self, username: str):
        super().__init__(username=username)
        self.error_attribute = "username"


class UnknownEngineType(MQError):
    code = "BadRequestException"
    fmt = "Broker engine type [{engine_type}] is invalid. Valid values are: [ACTIVEMQ]"

    def __init__(self, engine_type: str):
        super().__init__(engine_type=engine_type)
        self.error_attribute = "engineType"
