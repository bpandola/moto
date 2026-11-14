"""Exceptions raised by the transfer service."""

from moto.core.exceptions import ServiceException


class TransferError(ServiceException):
    pass


class ServerNotFound(TransferError):
    code = "ServerNotFound"

    def __init__(self, server_id: str) -> None:
        super().__init__(
            f"There are no transfer protocol-enabled servers with ID {server_id}."
        )


class UserNotFound(TransferError):
    code = "UserNotFound"

    def __init__(self, user_name: str, server_id: str) -> None:
        super().__init__(
            f"{user_name} does not match any user associated with server {server_id}."
        )


class PublicKeyNotFound(TransferError):
    code = "PublicKeyNotFound"

    def __init__(self, user_name: str, server_id: str, ssh_public_key_id: str) -> None:
        super().__init__(
            f"{ssh_public_key_id} does not match any keys associated with user {user_name} for server {server_id}."
        )


class ConnectorNotFound(TransferError):
    code = "ResourceNotFoundException"

    def __init__(self, connector_id: str) -> None:
        super().__init__(f"Connector {connector_id} does not exist.")
