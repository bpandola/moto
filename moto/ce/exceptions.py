"""Exceptions raised by the ce service."""

from moto.core.exceptions import ServiceException


class CostExplorerException(ServiceException):
    pass


class CostCategoryNotFound(CostExplorerException):
    code = "ResourceNotFoundException"

    def __init__(self, ccd_id: str):
        super().__init__(f"No Cost Categories found with ID {ccd_id}")
