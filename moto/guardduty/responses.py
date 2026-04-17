import json
from typing import Any, cast

from moto.core.parse import PROTOCOL_PARSERS
from moto.core.responses import (
    BaseResponse,
    determine_request_protocol,
    get_service_model,
    normalize_request,
)

from .models import GuardDutyBackend, guardduty_backends

# Mapping from URL regex group names to botocore serialization names
_URI_PARAM_RENAME = {
    "detector_id": "DetectorId",
    "filter_name": "FilterName",
}


class GuardDutyResponse(BaseResponse):
    def __init__(self) -> None:
        super().__init__(service_name="guardduty")
        self.automated_parameter_parsing = True

    def parse_parameters(self, request: Any) -> None:
        normalized_request = normalize_request(request)
        service_model = get_service_model(self.boto3_service_name)
        operation_model = service_model.operation_model(self._get_action())
        protocol = determine_request_protocol(
            service_model, normalized_request.content_type
        )
        parser_cls = PROTOCOL_PARSERS[protocol]
        parser = parser_cls(operation_model, map_type=self.PROTOCOL_PARSER_MAP_TYPE)
        # Rename URL regex group names to match botocore model expectations
        url_params = {
            _URI_PARAM_RENAME.get(k, k): v
            for k, v in (self.uri_match.groupdict() if self.uri_match else {}).items()
        }
        parsed = parser.parse(
            {
                "method": normalized_request.method,
                "values": normalized_request.values,
                "headers": normalized_request.headers,
                "body": normalized_request.data,
                "url_path": normalized_request.path,
                "url_params": url_params,
            }
        )
        self.params = cast(Any, parsed)

    @property
    def guardduty_backend(self) -> GuardDutyBackend:
        return guardduty_backends[self.current_account][self.region]

    def _get_body_param(self, param_name: str) -> Any:
        """Get a parameter from the raw JSON body (camelCase) for values
        stored as-is in models that use camelCase keys internally."""
        if self.body:
            return json.loads(self.body).get(param_name)
        return None

    def create_filter(self) -> str:
        detector_id = self._get_param("DetectorId")
        name = self._get_param("Name")
        action = self._get_param("Action")
        description = self._get_param("Description")
        finding_criteria = self._get_body_param("findingCriteria")
        rank = self._get_param("Rank")

        self.guardduty_backend.create_filter(
            detector_id, name, action, description, finding_criteria, rank
        )
        return json.dumps({"name": name})

    def create_detector(self) -> str:
        enable = self._get_param("Enable")
        finding_publishing_frequency = self._get_param("FindingPublishingFrequency")
        data_sources = self._get_body_param("dataSources")
        tags = self._get_param("Tags")
        features = self._get_body_param("features")

        detector_id = self.guardduty_backend.create_detector(
            enable, finding_publishing_frequency, data_sources, tags, features
        )

        return json.dumps({"detectorId": detector_id})

    def delete_detector(self) -> str:
        detector_id = self._get_param("DetectorId")

        self.guardduty_backend.delete_detector(detector_id)
        return "{}"

    def delete_filter(self) -> str:
        detector_id = self._get_param("DetectorId")
        filter_name = self._get_param("FilterName")

        self.guardduty_backend.delete_filter(detector_id, filter_name)
        return "{}"

    def enable_organization_admin_account(self) -> str:
        admin_account = self._get_param("AdminAccountId")
        self.guardduty_backend.enable_organization_admin_account(admin_account)

        return "{}"

    def list_organization_admin_accounts(self) -> str:
        account_ids = self.guardduty_backend.list_organization_admin_accounts()
        accounts = [
            {"adminAccountId": a, "adminStatus": "ENABLED"} for a in account_ids
        ]

        return json.dumps({"adminAccounts": accounts})

    def list_detectors(self) -> str:
        detector_ids = self.guardduty_backend.list_detectors()

        return json.dumps({"detectorIds": detector_ids})

    def get_detector(self) -> str:
        detector_id = self._get_param("DetectorId")

        detector = self.guardduty_backend.get_detector(detector_id)
        return json.dumps(detector.to_json())

    def get_filter(self) -> str:
        detector_id = self._get_param("DetectorId")
        filter_name = self._get_param("FilterName")

        _filter = self.guardduty_backend.get_filter(detector_id, filter_name)
        return json.dumps(_filter.to_json())

    def update_detector(self) -> str:
        detector_id = self._get_param("DetectorId")
        enable = self._get_param("Enable")
        finding_publishing_frequency = self._get_param("FindingPublishingFrequency")
        data_sources = self._get_body_param("dataSources")
        features = self._get_body_param("features")

        self.guardduty_backend.update_detector(
            detector_id, enable, finding_publishing_frequency, data_sources, features
        )
        return "{}"

    def update_filter(self) -> str:
        detector_id = self._get_param("DetectorId")
        filter_name = self._get_param("FilterName")
        action = self._get_param("Action")
        description = self._get_param("Description")
        finding_criteria = self._get_body_param("findingCriteria")
        rank = self._get_param("Rank")

        self.guardduty_backend.update_filter(
            detector_id,
            filter_name,
            action=action,
            description=description,
            finding_criteria=finding_criteria,
            rank=rank,
        )
        return json.dumps({"name": filter_name})

    def get_administrator_account(self) -> str:
        """Get administrator account details."""
        detector_id = self._get_param("DetectorId")
        result = self.guardduty_backend.get_administrator_account(detector_id)
        return json.dumps(result)
