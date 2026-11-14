from moto.core.exceptions import ServiceException


class DAXException(ServiceException):
    pass


class InvalidParameterValueException(DAXException):
    code = "InvalidParameterValueException"


class ClusterNotFoundFault(DAXException):
    code = "ClusterNotFoundFault"

    def __init__(self, name: str | None = None):
        # DescribeClusters and DeleteCluster use a different message for the same error
        msg = f"Cluster {name} not found." if name else "Cluster not found."
        super().__init__(msg)
