from moto.core.exceptions import ServiceException


class AmpException(ServiceException):
    pass


class ResourceNotFoundException(AmpException):
    code = "ResourceNotFoundException"

    def __init__(self, message: str, resource_id: str, resource_type: str):
        super().__init__(message)
        self.resourceId = resource_id
        self.resourceType = resource_type


class WorkspaceNotFound(ResourceNotFoundException):
    def __init__(self, workspace_id: str):
        super().__init__(
            "Workspace not found",
            resource_id=workspace_id,
            resource_type="AWS::APS::Workspace",
        )


class RuleGroupNamespaceNotFound(ResourceNotFoundException):
    def __init__(self, name: str):
        super().__init__(
            "RuleGroupNamespace not found",
            resource_id=name,
            resource_type="AWS::APS::RuleGroupNamespace",
        )


class AlertManagerDefinitionNotFound(ResourceNotFoundException):
    def __init__(self, workspace_id: str):
        super().__init__(
            "AlertManagerDefinition not found",
            resource_id=workspace_id,
            resource_type="AWS::APS::Workspace",
        )


class AlertManagerDefinitionAlreadyExists(AmpException):
    code = "ConflictException"
    message = "AlertManagerDefinition already exists"

    def __init__(self, workspace_id: str):
        super().__init__(self.message)
        self.resourceId = workspace_id
        self.resourceType = "AWS::APS::Workspace"
