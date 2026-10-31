from typing import Any


def cfn_to_api_encryption(
    bucket_encryption_properties: dict[str, Any],
) -> dict[str, Any]:
    sse_algorithm = bucket_encryption_properties["ServerSideEncryptionConfiguration"][
        0
    ]["ServerSideEncryptionByDefault"]["SSEAlgorithm"]
    kms_master_key_id = bucket_encryption_properties[
        "ServerSideEncryptionConfiguration"
    ][0]["ServerSideEncryptionByDefault"].get("KMSMasterKeyID")
    apply_server_side_encryption_by_default = {"SSEAlgorithm": sse_algorithm}
    if kms_master_key_id:
        apply_server_side_encryption_by_default["KMSMasterKeyID"] = kms_master_key_id
    return {
        "Rules": [
            {
                "ApplyServerSideEncryptionByDefault": apply_server_side_encryption_by_default
            }
        ]
    }
