import json
from typing import Any

from moto.core.common_types import TYPE_RESPONSE
from moto.core.responses import BaseResponse

from .models import GlacierBackend, glacier_backends


class GlacierResponse(BaseResponse):
    def __init__(self) -> None:
        super().__init__(service_name="glacier")
        self.automated_parameter_parsing = True

    def setup_class(
        self, request: Any, full_url: str, headers: Any, use_raw_body: bool = False
    ) -> None:
        super().setup_class(request, full_url, headers, use_raw_body=True)

    def parse_parameters(self, request: Any) -> None:
        try:
            super().parse_parameters(request)
        except UnicodeDecodeError:
            # UploadArchive sends a binary body (blob payload) that cannot
            # be decoded as UTF-8.  Populate params from URI match and headers.
            self.params = {}
            if self.uri_match:
                self.params.update(self.uri_match.groupdict())
            self.params["archiveDescription"] = self.headers.get(
                "x-amz-archive-description", ""
            )

    @property
    def glacier_backend(self) -> GlacierBackend:
        return glacier_backends[self.current_account][self.region]

    def list_vaults(self) -> TYPE_RESPONSE:
        vaults = self.glacier_backend.list_vaults()
        response = json.dumps(
            {"Marker": None, "VaultList": [vault.to_dict() for vault in vaults]}
        )

        headers = {"content-type": "application/json"}
        return 200, headers, response

    def describe_vault(self) -> TYPE_RESPONSE:
        vault_name = self._get_param("vaultName")
        vault = self.glacier_backend.describe_vault(vault_name)
        headers = {"content-type": "application/json"}
        return 200, headers, json.dumps(vault.to_dict())

    def create_vault(self) -> TYPE_RESPONSE:
        vault_name = self._get_param("vaultName")
        self.glacier_backend.create_vault(vault_name)
        return 201, {"status": 201}, ""

    def delete_vault(self) -> TYPE_RESPONSE:
        vault_name = self._get_param("vaultName")
        self.glacier_backend.delete_vault(vault_name)
        return 204, {"status": 204}, ""

    def upload_archive(self) -> TYPE_RESPONSE:
        description = self._get_param("archiveDescription") or ""
        vault_name = self._get_param("vaultName")
        vault = self.glacier_backend.upload_archive(vault_name, self.body, description)
        headers = {
            "x-amz-archive-id": vault["archive_id"],
            "x-amz-sha256-tree-hash": vault["sha256"],
            "status": 201,
        }
        return 201, headers, ""

    def delete_archive(self) -> TYPE_RESPONSE:
        vault_name = self._get_param("vaultName")
        archive_id = self._get_param("archiveId")

        self.glacier_backend.delete_archive(vault_name, archive_id)
        return 204, {"status": 204}, ""

    def list_jobs(self) -> TYPE_RESPONSE:
        vault_name = self._get_param("vaultName")
        jobs = self.glacier_backend.list_jobs(vault_name)
        headers = {"content-type": "application/json"}
        response = json.dumps(
            {"JobList": [job.to_dict() for job in jobs], "Marker": None}
        )
        return 200, headers, response

    def initiate_job(self) -> TYPE_RESPONSE:
        account_id = self.uri.split("/")[1]
        vault_name = self._get_param("vaultName")
        job_params = self._get_param("jobParameters")
        job_type = job_params["Type"]
        archive_id = job_params.get("ArchiveId")
        tier = job_params.get("Tier") or "Standard"
        job_id = self.glacier_backend.initiate_job(
            vault_name, job_type, tier, archive_id
        )
        headers = {
            "x-amz-job-id": job_id,
            "Location": f"/{account_id}/vaults/{vault_name}/jobs/{job_id}",
            "status": 202,
        }
        return 202, headers, ""

    def describe_job(self) -> str:
        vault_name = self._get_param("vaultName")
        job_id = self._get_param("jobId")

        job = self.glacier_backend.describe_job(vault_name, job_id)
        return json.dumps(job.to_dict())  # type: ignore

    def get_job_output(self) -> TYPE_RESPONSE:
        vault_name = self._get_param("vaultName")
        job_id = self._get_param("jobId")
        output = self.glacier_backend.get_job_output(vault_name, job_id)
        if output is None:
            return 404, {"status": 404}, "404 Not Found"
        if isinstance(output, dict):
            headers = {"content-type": "application/json"}
            return 200, headers, json.dumps(output)
        else:
            headers = {"content-type": "application/octet-stream"}
            return 200, headers, output
