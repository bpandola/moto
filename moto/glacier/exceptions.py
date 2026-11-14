from moto.core.exceptions import ServiceException


class GlacierException(ServiceException):
    pass


class VaultNotFound(GlacierException):
    # Unmodeled code (the model has ResourceNotFoundException); kept as-is for now.
    code = "VaultNotFound"
    message = "Vault not found"
