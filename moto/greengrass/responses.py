from typing import Any

from moto.core.responses import ActionResult, BaseResponse, EmptyResult
from moto.core.utils import iso_8601_datetime_with_milliseconds

from .models import (
    FakeCoreDefinition,
    FakeCoreDefinitionVersion,
    FakeDeployment,
    FakeDeviceDefinition,
    FakeDeviceDefinitionVersion,
    FakeFunctionDefinition,
    FakeFunctionDefinitionVersion,
    FakeGroup,
    FakeGroupVersion,
    FakeResourceDefinition,
    FakeResourceDefinitionVersion,
    FakeSubscriptionDefinition,
    FakeSubscriptionDefinitionVersion,
    GreengrassBackend,
    greengrass_backends,
)

Definition = (
    FakeCoreDefinition
    | FakeDeviceDefinition
    | FakeFunctionDefinition
    | FakeResourceDefinition
    | FakeSubscriptionDefinition
)

DefinitionVersion = (
    FakeCoreDefinitionVersion
    | FakeDeviceDefinitionVersion
    | FakeFunctionDefinitionVersion
    | FakeResourceDefinitionVersion
    | FakeSubscriptionDefinitionVersion
    | FakeGroupVersion
)


def _definition_info(definition: Definition) -> dict[str, Any]:
    return {
        "Arn": definition.arn,
        "CreationTimestamp": iso_8601_datetime_with_milliseconds(
            definition.created_at_datetime
        ),
        "Id": definition.id,
        "LastUpdatedTimestamp": iso_8601_datetime_with_milliseconds(
            definition.update_at_datetime
        ),
        "LatestVersion": definition.latest_version,
        "LatestVersionArn": definition.latest_version_arn,
        "Name": definition.name,
    }


def _version_info(version: DefinitionVersion, definition_id: str) -> dict[str, Any]:
    return {
        "Arn": version.arn,
        "CreationTimestamp": iso_8601_datetime_with_milliseconds(
            version.created_at_datetime
        ),
        "Id": definition_id,
        "Version": version.version,
    }


def _group_info(group: FakeGroup) -> dict[str, Any]:
    return {
        "Arn": group.arn,
        "CreationTimestamp": iso_8601_datetime_with_milliseconds(
            group.created_at_datetime
        ),
        "Id": group.group_id,
        "LastUpdatedTimestamp": iso_8601_datetime_with_milliseconds(
            group.last_updated_datetime
        ),
        "LatestVersion": group.latest_version,
        "LatestVersionArn": group.latest_version_arn,
        "Name": group.name,
    }


def _deployment_info(deployment: FakeDeployment) -> dict[str, Any]:
    return {"DeploymentId": deployment.id, "DeploymentArn": deployment.arn}


class GreengrassResponse(BaseResponse):
    def __init__(self) -> None:
        super().__init__(service_name="greengrass")
        self.automated_parameter_parsing = True

    @property
    def greengrass_backend(self) -> GreengrassBackend:
        return greengrass_backends[self.current_account][self.region]

    def list_core_definitions(self) -> ActionResult:
        res = self.greengrass_backend.list_core_definitions()
        return ActionResult({"Definitions": [_definition_info(d) for d in res]})

    def create_core_definition(self) -> ActionResult:
        name = self._get_param("Name")
        initial_version = self._get_param("InitialVersion")
        res = self.greengrass_backend.create_core_definition(
            name=name, initial_version=initial_version
        )
        return ActionResult(_definition_info(res))

    def get_core_definition(self) -> ActionResult:
        core_definition_id = self._get_param("CoreDefinitionId")
        res = self.greengrass_backend.get_core_definition(
            core_definition_id=core_definition_id
        )
        return ActionResult(_definition_info(res))

    def delete_core_definition(self) -> EmptyResult:
        core_definition_id = self._get_param("CoreDefinitionId")
        self.greengrass_backend.delete_core_definition(
            core_definition_id=core_definition_id
        )
        return EmptyResult()

    def update_core_definition(self) -> EmptyResult:
        core_definition_id = self._get_param("CoreDefinitionId")
        name = self._get_param("Name")
        self.greengrass_backend.update_core_definition(
            core_definition_id=core_definition_id, name=name
        )
        return EmptyResult()

    def create_core_definition_version(self) -> ActionResult:
        core_definition_id = self._get_param("CoreDefinitionId")
        cores = self._get_param("Cores")

        res = self.greengrass_backend.create_core_definition_version(
            core_definition_id=core_definition_id, cores=cores
        )
        return ActionResult(_version_info(res, res.core_definition_id))

    def list_core_definition_versions(self) -> ActionResult:
        core_definition_id = self._get_param("CoreDefinitionId")
        res = self.greengrass_backend.list_core_definition_versions(core_definition_id)
        versions = [_version_info(v, v.core_definition_id) for v in res]
        return ActionResult({"Versions": versions})

    def get_core_definition_version(self) -> ActionResult:
        core_definition_id = self._get_param("CoreDefinitionId")
        core_definition_version_id = self._get_param("CoreDefinitionVersionId")
        res = self.greengrass_backend.get_core_definition_version(
            core_definition_id=core_definition_id,
            core_definition_version_id=core_definition_version_id,
        )
        result = _version_info(res, res.core_definition_id)
        result["Definition"] = res.definition
        return ActionResult(result)

    def create_device_definition(self) -> ActionResult:
        name = self._get_param("Name")
        initial_version = self._get_param("InitialVersion")
        res = self.greengrass_backend.create_device_definition(
            name=name, initial_version=initial_version
        )
        return ActionResult(_definition_info(res))

    def list_device_definitions(self) -> ActionResult:
        res = self.greengrass_backend.list_device_definitions()
        return ActionResult({"Definitions": [_definition_info(d) for d in res]})

    def create_device_definition_version(self) -> ActionResult:
        device_definition_id = self._get_param("DeviceDefinitionId")
        devices = self._get_param("Devices")

        res = self.greengrass_backend.create_device_definition_version(
            device_definition_id=device_definition_id, devices=devices
        )
        return ActionResult(_version_info(res, res.device_definition_id))

    def list_device_definition_versions(self) -> ActionResult:
        device_definition_id = self._get_param("DeviceDefinitionId")
        res = self.greengrass_backend.list_device_definition_versions(
            device_definition_id
        )
        versions = [_version_info(v, v.device_definition_id) for v in res]
        return ActionResult({"Versions": versions})

    def get_device_definition(self) -> ActionResult:
        device_definition_id = self._get_param("DeviceDefinitionId")
        res = self.greengrass_backend.get_device_definition(
            device_definition_id=device_definition_id
        )
        return ActionResult(_definition_info(res))

    def delete_device_definition(self) -> EmptyResult:
        device_definition_id = self._get_param("DeviceDefinitionId")
        self.greengrass_backend.delete_device_definition(
            device_definition_id=device_definition_id
        )
        return EmptyResult()

    def update_device_definition(self) -> EmptyResult:
        device_definition_id = self._get_param("DeviceDefinitionId")
        name = self._get_param("Name")
        self.greengrass_backend.update_device_definition(
            device_definition_id=device_definition_id, name=name
        )
        return EmptyResult()

    def get_device_definition_version(self) -> ActionResult:
        device_definition_id = self._get_param("DeviceDefinitionId")
        device_definition_version_id = self._get_param("DeviceDefinitionVersionId")
        res = self.greengrass_backend.get_device_definition_version(
            device_definition_id=device_definition_id,
            device_definition_version_id=device_definition_version_id,
        )
        result = _version_info(res, res.device_definition_id)
        result["Definition"] = {"Devices": res.devices}
        return ActionResult(result)

    def create_resource_definition(self) -> ActionResult:
        initial_version = self._get_param("InitialVersion")
        name = self._get_param("Name")
        res = self.greengrass_backend.create_resource_definition(
            name=name, initial_version=initial_version
        )
        return ActionResult(_definition_info(res))

    def list_resource_definitions(self) -> ActionResult:
        res = self.greengrass_backend.list_resource_definitions()
        return ActionResult({"Definitions": [_definition_info(d) for d in res]})

    def get_resource_definition(self) -> ActionResult:
        resource_definition_id = self._get_param("ResourceDefinitionId")
        res = self.greengrass_backend.get_resource_definition(
            resource_definition_id=resource_definition_id
        )
        return ActionResult(_definition_info(res))

    def delete_resource_definition(self) -> EmptyResult:
        resource_definition_id = self._get_param("ResourceDefinitionId")
        self.greengrass_backend.delete_resource_definition(
            resource_definition_id=resource_definition_id
        )
        return EmptyResult()

    def update_resource_definition(self) -> EmptyResult:
        resource_definition_id = self._get_param("ResourceDefinitionId")
        name = self._get_param("Name")
        self.greengrass_backend.update_resource_definition(
            resource_definition_id=resource_definition_id, name=name
        )
        return EmptyResult()

    def create_resource_definition_version(self) -> ActionResult:
        resource_definition_id = self._get_param("ResourceDefinitionId")
        resources = self._get_param("Resources")

        res = self.greengrass_backend.create_resource_definition_version(
            resource_definition_id=resource_definition_id, resources=resources
        )
        return ActionResult(_version_info(res, res.resource_definition_id))

    def list_resource_definition_versions(self) -> ActionResult:
        resource_definition_id = self._get_param("ResourceDefinitionId")
        res = self.greengrass_backend.list_resource_definition_versions(
            resource_definition_id
        )
        versions = [_version_info(v, v.resource_definition_id) for v in res]
        return ActionResult({"Versions": versions})

    def get_resource_definition_version(self) -> ActionResult:
        resource_definition_id = self._get_param("ResourceDefinitionId")
        resource_definition_version_id = self._get_param("ResourceDefinitionVersionId")
        res = self.greengrass_backend.get_resource_definition_version(
            resource_definition_id=resource_definition_id,
            resource_definition_version_id=resource_definition_version_id,
        )
        result = _version_info(res, res.resource_definition_id)
        result["Definition"] = {"Resources": res.resources}
        return ActionResult(result)

    def create_function_definition(self) -> ActionResult:
        initial_version = self._get_param("InitialVersion")
        name = self._get_param("Name")
        res = self.greengrass_backend.create_function_definition(
            name=name, initial_version=initial_version
        )
        return ActionResult(_definition_info(res))

    def list_function_definitions(self) -> ActionResult:
        res = self.greengrass_backend.list_function_definitions()
        return ActionResult({"Definitions": [_definition_info(d) for d in res]})

    def get_function_definition(self) -> ActionResult:
        function_definition_id = self._get_param("FunctionDefinitionId")
        res = self.greengrass_backend.get_function_definition(
            function_definition_id=function_definition_id,
        )
        return ActionResult(_definition_info(res))

    def delete_function_definition(self) -> EmptyResult:
        function_definition_id = self._get_param("FunctionDefinitionId")
        self.greengrass_backend.delete_function_definition(
            function_definition_id=function_definition_id,
        )
        return EmptyResult()

    def update_function_definition(self) -> EmptyResult:
        function_definition_id = self._get_param("FunctionDefinitionId")
        name = self._get_param("Name")
        self.greengrass_backend.update_function_definition(
            function_definition_id=function_definition_id, name=name
        )
        return EmptyResult()

    def create_function_definition_version(self) -> ActionResult:
        default_config = self._get_param("DefaultConfig")
        function_definition_id = self._get_param("FunctionDefinitionId")
        functions = self._get_param("Functions")

        res = self.greengrass_backend.create_function_definition_version(
            default_config=default_config,
            function_definition_id=function_definition_id,
            functions=functions,
        )
        return ActionResult(_version_info(res, res.function_definition_id))

    def list_function_definition_versions(self) -> ActionResult:
        function_definition_id = self._get_param("FunctionDefinitionId")
        res = self.greengrass_backend.list_function_definition_versions(
            function_definition_id=function_definition_id
        )
        versions = [_version_info(v, v.function_definition_id) for v in res.values()]
        return ActionResult({"Versions": versions})

    def get_function_definition_version(self) -> ActionResult:
        function_definition_id = self._get_param("FunctionDefinitionId")
        function_definition_version_id = self._get_param("FunctionDefinitionVersionId")
        res = self.greengrass_backend.get_function_definition_version(
            function_definition_id=function_definition_id,
            function_definition_version_id=function_definition_version_id,
        )
        result = _version_info(res, res.function_definition_id)
        result["Definition"] = {"Functions": res.functions}
        return ActionResult(result)

    def create_subscription_definition(self) -> ActionResult:
        initial_version = self._get_param("InitialVersion")
        name = self._get_param("Name")
        res = self.greengrass_backend.create_subscription_definition(
            name=name, initial_version=initial_version
        )
        return ActionResult(_definition_info(res))

    def list_subscription_definitions(self) -> ActionResult:
        res = self.greengrass_backend.list_subscription_definitions()
        return ActionResult({"Definitions": [_definition_info(d) for d in res]})

    def get_subscription_definition(self) -> ActionResult:
        subscription_definition_id = self._get_param("SubscriptionDefinitionId")
        res = self.greengrass_backend.get_subscription_definition(
            subscription_definition_id=subscription_definition_id
        )
        return ActionResult(_definition_info(res))

    def delete_subscription_definition(self) -> EmptyResult:
        subscription_definition_id = self._get_param("SubscriptionDefinitionId")
        self.greengrass_backend.delete_subscription_definition(
            subscription_definition_id=subscription_definition_id
        )
        return EmptyResult()

    def update_subscription_definition(self) -> EmptyResult:
        subscription_definition_id = self._get_param("SubscriptionDefinitionId")
        name = self._get_param("Name")
        self.greengrass_backend.update_subscription_definition(
            subscription_definition_id=subscription_definition_id, name=name
        )
        return EmptyResult()

    def create_subscription_definition_version(self) -> ActionResult:
        subscription_definition_id = self._get_param("SubscriptionDefinitionId")
        subscriptions = self._get_param("Subscriptions")
        res = self.greengrass_backend.create_subscription_definition_version(
            subscription_definition_id=subscription_definition_id,
            subscriptions=subscriptions,
        )
        return ActionResult(_version_info(res, res.subscription_definition_id))

    def list_subscription_definition_versions(self) -> ActionResult:
        subscription_definition_id = self._get_param("SubscriptionDefinitionId")
        res = self.greengrass_backend.list_subscription_definition_versions(
            subscription_definition_id=subscription_definition_id
        )
        versions = [
            _version_info(v, v.subscription_definition_id) for v in res.values()
        ]
        return ActionResult({"Versions": versions})

    def get_subscription_definition_version(self) -> ActionResult:
        subscription_definition_id = self._get_param("SubscriptionDefinitionId")
        subscription_definition_version_id = self._get_param(
            "SubscriptionDefinitionVersionId"
        )
        res = self.greengrass_backend.get_subscription_definition_version(
            subscription_definition_id=subscription_definition_id,
            subscription_definition_version_id=subscription_definition_version_id,
        )
        result = _version_info(res, res.subscription_definition_id)
        result["Definition"] = {"Subscriptions": res.subscriptions}
        return ActionResult(result)

    def create_group(self) -> ActionResult:
        initial_version = self._get_param("InitialVersion")
        name = self._get_param("Name")
        res = self.greengrass_backend.create_group(
            name=name, initial_version=initial_version
        )
        return ActionResult(_group_info(res))

    def list_groups(self) -> ActionResult:
        res = self.greengrass_backend.list_groups()
        return ActionResult({"Groups": [_group_info(group) for group in res]})

    def get_group(self) -> ActionResult:
        group_id = self._get_param("GroupId")
        res = self.greengrass_backend.get_group(group_id=group_id)
        return ActionResult(_group_info(res))

    def delete_group(self) -> EmptyResult:
        group_id = self._get_param("GroupId")
        self.greengrass_backend.delete_group(group_id=group_id)
        return EmptyResult()

    def update_group(self) -> EmptyResult:
        group_id = self._get_param("GroupId")
        name = self._get_param("Name")
        self.greengrass_backend.update_group(group_id=group_id, name=name)
        return EmptyResult()

    def create_group_version(self) -> ActionResult:
        group_id = self._get_param("GroupId")

        core_definition_version_arn = self._get_param("CoreDefinitionVersionArn")
        device_definition_version_arn = self._get_param("DeviceDefinitionVersionArn")
        function_definition_version_arn = self._get_param(
            "FunctionDefinitionVersionArn"
        )
        resource_definition_version_arn = self._get_param(
            "ResourceDefinitionVersionArn"
        )
        subscription_definition_version_arn = self._get_param(
            "SubscriptionDefinitionVersionArn"
        )

        res = self.greengrass_backend.create_group_version(
            group_id=group_id,
            core_definition_version_arn=core_definition_version_arn,
            device_definition_version_arn=device_definition_version_arn,
            function_definition_version_arn=function_definition_version_arn,
            resource_definition_version_arn=resource_definition_version_arn,
            subscription_definition_version_arn=subscription_definition_version_arn,
        )
        return ActionResult(_version_info(res, res.group_id))

    def list_group_versions(self) -> ActionResult:
        group_id = self._get_param("GroupId")
        res = self.greengrass_backend.list_group_versions(group_id=group_id)
        versions = [_version_info(v, v.group_id) for v in res]
        return ActionResult({"Versions": versions})

    def get_group_version(self) -> ActionResult:
        group_id = self._get_param("GroupId")
        group_version_id = self._get_param("GroupVersionId")
        res = self.greengrass_backend.get_group_version(
            group_id=group_id,
            group_version_id=group_version_id,
        )
        result = _version_info(res, res.group_id)
        result["Definition"] = {
            "CoreDefinitionVersionArn": res.core_definition_version_arn,
            "DeviceDefinitionVersionArn": res.device_definition_version_arn,
            "FunctionDefinitionVersionArn": res.function_definition_version_arn,
            "ResourceDefinitionVersionArn": res.resource_definition_version_arn,
            "SubscriptionDefinitionVersionArn": res.subscription_definition_version_arn,
        }
        return ActionResult(result)

    def create_deployment(self) -> ActionResult:
        group_id = self._get_param("GroupId")
        group_version_id = self._get_param("GroupVersionId")
        deployment_type = self._get_param("DeploymentType")
        deployment_id = self._get_param("DeploymentId")

        res = self.greengrass_backend.create_deployment(
            group_id=group_id,
            group_version_id=group_version_id,
            deployment_type=deployment_type,
            deployment_id=deployment_id,
        )
        return ActionResult(_deployment_info(res))

    def list_deployments(self) -> ActionResult:
        group_id = self._get_param("GroupId")
        res = self.greengrass_backend.list_deployments(group_id=group_id)

        deployments = [
            {
                **_deployment_info(deployment),
                "CreatedAt": iso_8601_datetime_with_milliseconds(
                    deployment.created_at_datetime
                ),
                "DeploymentType": deployment.deployment_type,
                "GroupArn": deployment.group_arn,
            }
            for deployment in res
        ]
        return ActionResult({"Deployments": deployments})

    def get_deployment_status(self) -> ActionResult:
        group_id = self._get_param("GroupId")
        deployment_id = self._get_param("DeploymentId")

        res = self.greengrass_backend.get_deployment_status(
            group_id=group_id,
            deployment_id=deployment_id,
        )
        result = {
            "DeploymentStatus": res.deployment_status,
            "DeploymentType": res.deployment_type,
            "UpdatedAt": iso_8601_datetime_with_milliseconds(res.update_at_datetime),
        }
        return ActionResult(result)

    def reset_deployments(self) -> ActionResult:
        group_id = self._get_param("GroupId")

        res = self.greengrass_backend.reset_deployments(group_id=group_id)
        return ActionResult(_deployment_info(res))

    def associate_role_to_group(self) -> ActionResult:
        group_id = self._get_param("GroupId")
        role_arn = self._get_param("RoleArn")
        res = self.greengrass_backend.associate_role_to_group(
            group_id=group_id,
            role_arn=role_arn,
        )
        result = {
            "AssociatedAt": iso_8601_datetime_with_milliseconds(res.associated_at)
        }
        return ActionResult(result)

    def get_associated_role(self) -> ActionResult:
        group_id = self._get_param("GroupId")
        res = self.greengrass_backend.get_associated_role(group_id=group_id)
        result = {
            "AssociatedAt": iso_8601_datetime_with_milliseconds(res.associated_at),
            "RoleArn": res.role_arn,
        }
        return ActionResult(result)

    def disassociate_role_from_group(self) -> ActionResult:
        group_id = self._get_param("GroupId")
        self.greengrass_backend.disassociate_role_from_group(group_id=group_id)
        result = {"DisassociatedAt": iso_8601_datetime_with_milliseconds()}
        return ActionResult(result)
