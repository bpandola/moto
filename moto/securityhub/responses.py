"""Handles incoming securityhub requests, invokes methods, returns responses."""

import json
from typing import Any

from moto.core.responses import BaseResponse

from .models import SecurityHubBackend, securityhub_backends


class SecurityHubResponse(BaseResponse):
    def __init__(self) -> None:
        super().__init__(service_name="securityhub")
        self.automated_parameter_parsing = True

    @property
    def securityhub_backend(self) -> SecurityHubBackend:
        return securityhub_backends[self.current_account][self.region]

    def enable_security_hub(self) -> str:
        enable_default_standards = self._get_param("EnableDefaultStandards", True)
        tags = self._get_param("Tags", {})

        self.securityhub_backend.enable_security_hub(
            enable_default_standards=enable_default_standards,
            tags=tags,
        )
        return json.dumps({})

    def disable_security_hub(self) -> str:
        self.securityhub_backend.disable_security_hub()
        return json.dumps({})

    def describe_hub(self) -> str:
        hub_arn = self._get_param("HubArn")

        hub_info = self.securityhub_backend.describe_hub(hub_arn=hub_arn)
        return json.dumps(hub_info)

    def get_findings(self) -> str:
        filters = self._get_param("Filters")
        sort_criteria = self._get_param("SortCriteria")
        max_results = self._get_param("MaxResults")
        next_token = self._get_param("NextToken")

        findings, next_token = self.securityhub_backend.get_findings(
            filters=filters,
            sort_criteria=sort_criteria,
            max_results=max_results,
            next_token=next_token,
        )

        response = {"Findings": findings, "NextToken": next_token}
        return json.dumps(response)

    def batch_import_findings(self) -> str:
        findings = self._get_param("Findings", [])

        failed_count, success_count, failed_findings = (
            self.securityhub_backend.batch_import_findings(
                findings=findings,
            )
        )

        return json.dumps(
            {
                "FailedCount": failed_count,
                "FailedFindings": [
                    {
                        "ErrorCode": finding.get("ErrorCode"),
                        "ErrorMessage": finding.get("ErrorMessage"),
                        "Id": finding.get("Id"),
                    }
                    for finding in failed_findings
                ],
                "SuccessCount": success_count,
            }
        )

    def enable_organization_admin_account(self) -> str:
        admin_account_id = self._get_param("AdminAccountId")
        self.securityhub_backend.enable_organization_admin_account(
            admin_account_id=admin_account_id,
        )
        return json.dumps({})

    def update_organization_configuration(self) -> str:
        auto_enable = self._get_param("AutoEnable")
        auto_enable_standards = self._get_param("AutoEnableStandards")
        organization_configuration = self._get_param("OrganizationConfiguration")
        self.securityhub_backend.update_organization_configuration(
            auto_enable=auto_enable,
            auto_enable_standards=auto_enable_standards,
            organization_configuration=organization_configuration,
        )
        return json.dumps({})

    def get_administrator_account(self) -> str:
        administrator = self.securityhub_backend.get_administrator_account()

        return json.dumps(administrator)

    def describe_organization_configuration(self) -> str:
        response = self.securityhub_backend.describe_organization_configuration()
        return json.dumps(dict(response))

    def create_members(self) -> str:
        account_details = self._get_param("AccountDetails", [])
        unprocessed_accounts = self.securityhub_backend.create_members(
            account_details=account_details,
        )
        return json.dumps({"UnprocessedAccounts": unprocessed_accounts})

    def get_members(self) -> str:
        account_ids = self._get_param("AccountIds", [])
        members, unprocessed_accounts = self.securityhub_backend.get_members(
            account_ids=account_ids,
        )
        return json.dumps(
            {"Members": members, "UnprocessedAccounts": unprocessed_accounts}
        )

    def list_members(self) -> str:
        only_associated = self._get_param("OnlyAssociated")
        max_results = self._get_param("MaxResults")
        if max_results is not None:
            max_results = int(max_results)
        next_token = self._get_param("NextToken")

        members, next_token = self.securityhub_backend.list_members(
            only_associated=only_associated,
            max_results=max_results,
            next_token=next_token,
        )
        response: dict[str, Any] = {"Members": members}
        if next_token:
            response["NextToken"] = next_token
        return json.dumps(response)

    def get_master_account(self) -> str:
        master = self.securityhub_backend.get_master_account()
        return json.dumps(master)
