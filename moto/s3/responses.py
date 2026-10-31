import datetime
import io
import re
import urllib.parse
from collections.abc import Iterator
from typing import Any
from urllib.parse import parse_qs, unquote, urlencode, urlparse, urlunparse
from xml.dom import minidom

import xmltodict
from werkzeug.exceptions import Forbidden

from moto import settings
from moto.core.common_types import TYPE_RESPONSE
from moto.core.mime_types import APP_XML
from moto.core.parse import RequestParserError
from moto.core.request import Request
from moto.core.responses import ActionResult, BaseResponse, EmptyResult
from moto.core.utils import (
    ALT_DOMAIN_SUFFIXES,
    extract_region_from_aws_authorization,
    rfc_1123_datetime,
)
from moto.s3bucket_path.utils import (
    bucket_name_from_url as bucketpath_bucket_name_from_url,
)
from moto.utilities.utils import get_partition

from ..core.exceptions import SignatureDoesNotMatchError
from .exceptions import (
    AccessForbidden,
    BucketAccessDeniedError,
    BucketAlreadyExists,
    BucketAlreadyOwnedByYou,
    BucketNotEmpty,
    DuplicateTagKeys,
    HeadOnDeleteMarker,
    IllegalLocationConstraintException,
    IncompatibleLocationConstraintException,
    InvalidArgumentError,
    InvalidContinuationToken,
    InvalidLocationConstraintException,
    InvalidMaxPartArgument,
    InvalidMaxPartNumberArgument,
    InvalidNamespaceHeaderException,
    InvalidObjectState,
    InvalidPartOrder,
    InvalidRange,
    LockNotEnabled,
    MalformedACLError,
    MalformedXML,
    MethodNotAllowed,
    MissingBucket,
    MissingContentLength,
    MissingKey,
    MissingRequestBody,
    MissingUploadObjectWithObjectLockHeaders,
    MissingVersion,
    NoSuchBucketPolicy,
    NoSuchCORSConfiguration,
    NoSuchLifecycleConfiguration,
    NoSuchTagSet,
    NoSuchWebsiteConfiguration,
    NoSystemTags,
    NotAnIntegerException,
    NotModified,
    ObjectNotInActiveTierError,
    OwnershipControlsNotFoundError,
    PreconditionFailed,
    RangeNotSatisfiable,
    ReplicationConfigurationNotFoundError,
    S3AclAndGrantError,
    S3ClientError,
    ServerSideEncryptionConfigurationNotFoundError,
    VersioningNotEnabledForReplication,
)
from .models import (
    FakeAcl,
    FakeBucket,
    FakeDeleteMarker,
    FakeGrant,
    FakeGrantee,
    FakeKey,
    S3Backend,
    TransitionDefaultMinimumObjectSize,
    get_canned_acl,
    s3_backends,
)
from .utils import (
    ARCHIVE_STORAGE_CLASSES,
    bucket_name_from_url,
    bucket_policy_is_public,
    compute_checksum,
    cors_matches_origin,
    metadata_from_headers,
    parse_region_from_url,
)

DEFAULT_REGION_NAME = "us-east-1"

# IAM actions for the operations whose IAM action name differs from the operation name
# https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html
S3_IAM_ACTIONS = {
    "CompleteMultipartUpload": "PutObject",
    "CopyObject": "PutObject",
    "CreateMultipartUpload": "PutObject",
    "DeleteBucketCors": "PutBucketCORS",
    "DeleteBucketEncryption": "PutEncryptionConfiguration",
    "DeleteBucketLifecycle": "PutLifecycleConfiguration",
    "DeleteBucketOwnershipControls": "PutBucketOwnershipControls",
    "DeleteBucketReplication": "PutReplicationConfiguration",
    "DeleteBucketTagging": "PutBucketTagging",
    "DeleteObjects": "DeleteObject",
    "DeletePublicAccessBlock": "PutBucketPublicAccessBlock",
    "GetBucketAccelerateConfiguration": "GetAccelerateConfiguration",
    "GetBucketCors": "GetBucketCORS",
    "GetBucketEncryption": "GetEncryptionConfiguration",
    "GetBucketInventoryConfiguration": "GetInventoryConfiguration",
    "GetBucketLifecycleConfiguration": "GetLifecycleConfiguration",
    "GetBucketNotificationConfiguration": "GetBucketNotification",
    "GetBucketReplication": "GetReplicationConfiguration",
    "GetObjectLockConfiguration": "GetBucketObjectLockConfiguration",
    "GetPublicAccessBlock": "GetBucketPublicAccessBlock",
    "HeadBucket": "ListBucket",
    "HeadObject": "GetObject",
    "ListBucketInventoryConfigurations": "GetInventoryConfiguration",
    "ListBuckets": "ListAllMyBuckets",
    "ListMultipartUploads": "ListBucketMultipartUploads",
    "ListObjectVersions": "ListBucketVersions",
    "ListObjects": "ListBucket",
    "ListObjectsV2": "ListBucket",
    "ListParts": "ListMultipartUploadParts",
    "PostObject": "PutObject",
    "PutBucketAccelerateConfiguration": "PutAccelerateConfiguration",
    "PutBucketCors": "PutBucketCORS",
    "PutBucketEncryption": "PutEncryptionConfiguration",
    "PutBucketInventoryConfiguration": "PutInventoryConfiguration",
    "PutBucketLifecycleConfiguration": "PutLifecycleConfiguration",
    "PutBucketNotificationConfiguration": "PutBucketNotification",
    "PutBucketReplication": "PutReplicationConfiguration",
    "PutObjectLockConfiguration": "PutBucketObjectLockConfiguration",
    "PutPublicAccessBlock": "PutBucketPublicAccessBlock",
    "SelectObjectContent": "GetObject",
    "UploadPart": "PutObject",
    "UploadPartCopy": "PutObject",
}

# IAM actions for operations on a specific object version
S3_VERSION_IAM_ACTIONS = {
    "DeleteObject": "DeleteObjectVersion",
    "DeleteObjectTagging": "DeleteObjectVersionTagging",
    "GetObject": "GetObjectVersion",
    "GetObjectAcl": "GetObjectVersionAcl",
    "GetObjectAttributes": "GetObjectVersionAttributes",
    "GetObjectTagging": "GetObjectVersionTagging",
    "HeadObject": "GetObjectVersion",
    "PutObjectAcl": "PutObjectVersionAcl",
    "PutObjectTagging": "PutObjectVersionTagging",
}

ALLOWED_HEADER_OVERRIDES = {
    "ResponseContentType": "ContentType",
    "ResponseContentLanguage": "ContentLanguage",
    "ResponseExpires": "Expires",
    "ResponseCacheControl": "CacheControl",
    "ResponseContentDisposition": "ContentDisposition",
    "ResponseContentEncoding": "ContentEncoding",
}


class S3Response(BaseResponse):
    use_raw_body = True

    def __init__(self) -> None:
        super().__init__(service_name="s3")
        # Whatever format requests come in, we should never touch them
        # There are some nuances here - this decision should be method-specific, instead of service-specific
        # E.G.: we don't want to touch put_object(), but we might have to decompress put_object_configuration()
        # Taking the naive approach to never decompress anything from S3 for now
        self.allow_request_decompression = False
        self.automated_parameter_parsing = True
        # Errors raised before the action is called (e.g. authentication errors)
        # are always returned with a body.
        self._action_started = False
        self._delete_objects_access_denied = False
        self._malformed_body = False

    def setup_class(  # type: ignore[override]
        self, request: Request, full_url: str | None = None, headers: Any = None
    ) -> None:
        super().setup_class(request, full_url, headers)
        full_url = request.raw_url if full_url is None else full_url
        self.region = parse_region_from_url(full_url, use_default_region=False)
        if self.region is None:
            self.region = (
                extract_region_from_aws_authorization(
                    request.headers.get("Authorization", "")
                )
                or DEFAULT_REGION_NAME
            )
        self.bucket_name = self.parse_bucket_name_from_url(request, full_url)
        self.request = request
        if self.bucket_name:
            # Used by S3IAMRequest to raise bucket-specific errors
            self.data["BucketName"] = self.bucket_name
            self.backend.log_incoming_request(request, self.bucket_name)

    def parse_parameters(self, request: Request, body: bytes | None = None) -> None:
        # The payload of an aws-chunked upload has to be decoded before it is parsed
        content_sha256 = request.headers.get("x-amz-content-sha256")
        if content_sha256 == "STREAMING-UNSIGNED-PAYLOAD-TRAILER":
            self.body = self._handle_encoded_body(self.body)
        elif content_sha256 in [
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
        ]:
            self.body = self._handle_v4_chunk_signatures(
                self.raw_body, int(request.headers["x-amz-decoded-content-length"])
            )
        try:
            super().parse_parameters(request, body=self.body)
        except RequestParserError:
            # Reported as MalformedXML once the request has been authorized
            self.params = {}
            self._malformed_body = True

    def _authenticate_and_authorize_normal_action(self, resource: str = "*") -> None:
        """
        S3 validates the request, and authorizes it against the bucket/key
        resource using S3-specific IAM action names, before calling the action.
        """
        action = self._get_action()
        key_name = self._get_param("Key") if "Key" in self.uri_match else None

        if (
            self.method in ["PUT", "POST"]
            and self.bucket_name
            and key_name is None
            and action != "CreateBucket"
            and not self.headers.get("Content-Length")
        ):
            raise MissingContentLength()

        if self._malformed_body:
            raise MalformedXML()

        if key_name is not None:
            self._authorize_object_request(key_name)

        iam_action = S3_IAM_ACTIONS.get(action, action)
        if self._get_param("VersionId") is not None:
            iam_action = S3_VERSION_IAM_ACTIONS.get(action, iam_action)
        try:
            self._authenticate_and_authorize_s3_action(
                bucket_name=self.bucket_name or None,
                key_name=key_name,
                action=iam_action,
            )
        except BucketAccessDeniedError:
            if action != "DeleteObjects":
                raise
            # Every key will be reported as an error instead
            self._delete_objects_access_denied = True
        self._action_started = True

    def _authorize_object_request(self, key_name: str) -> None:
        # SDK requests tend to have Authorization set automatically
        # If users make an HTTP-request, such as `requests.get("https://bucket-name.s3.amazonaws.com/file-name")`,
        # The authorization-header may not be set
        authorized_request = "Authorization" in self.headers
        signed_url = "Signature=" in self.request.url
        try:
            key = self.backend.get_object(self.bucket_name, key_name)
            bucket = self.backend.get_bucket(self.bucket_name)
        except S3ClientError:
            key = bucket = None

        # Enforces policy when creating new objects, `if key` does not https://github.com/getmoto/moto/issues/7837
        if bucket:
            resource = f"arn:{bucket.partition}:s3:::{self.bucket_name}/{key_name}"

            # Authorization Workflow
            # https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-control-auth-workflow-object-operation.html

            # A bucket can deny all actions, regardless of who makes the request
            from moto.iam.access_control import PermissionResult

            method = self.method
            action = f"s3:{method.upper()[0]}{method.lower()[1:]}Object"
            bucket_permissions = bucket.get_permission(action, resource)
            if bucket_permissions == PermissionResult.DENIED:
                raise Forbidden(description="")

            if key:
                # If the request is not authorized, and not signed,
                # that means that the action should be allowed for anonymous users
                if not authorized_request and not signed_url:
                    # We already know that the bucket permissions do not explicitly deny this
                    # So bucket permissions are either not set, or do not explicitly allow
                    # Next check is to see if the ACL of the individual key allows this action
                    if bucket_permissions != PermissionResult.PERMITTED and (
                        key.acl and not key.acl.public_read
                    ):
                        raise Forbidden(description="")
        if not key and signed_url and not authorized_request:
            # coming in from requests.get(s3.generate_presigned_url())
            if self._invalid_headers(self.request.url, dict(self.request.headers)):
                raise SignatureDoesNotMatchError()

    def serialized(self, action_result: ActionResult) -> TYPE_RESPONSE:
        status_code, headers, body = super().serialized(action_result)
        if self.method == "HEAD" and self._action_started:
            # S3 never returns a body for a HEAD request
            body = ""
        return status_code, headers, body

    def _get_range(self, length: int) -> tuple[int, int] | None:
        """
        Returns the (first, last) byte positions requested by the Range header,
        or None if the full object should be returned.
        """
        range_header = self._get_param("Range")
        if not range_header:
            return None
        last = length - 1

        _, rspec = range_header.split("=")
        if "," in rspec:
            return None

        try:
            begin, end = [int(i) if i else None for i in rspec.split("-")]
        except ValueError:
            # if we can't parse the Range header, S3 just treat the request as a non-range request
            return None

        if (begin is None and end == 0) or (begin is not None and begin > last):
            raise InvalidRange(actual_size=str(length), range_requested=range_header)

        if begin is not None:  # byte range
            end = last if end is None else min(end, last)
        elif end is not None:  # suffix byte range
            begin = length - min(end, length)
            end = last
        else:
            # Treat as non-range request
            return None

        if begin > min(end, last):
            # Treat as non-range request if after the logic is applied, the start of the range is greater than the end
            return None
        return begin, end

    def _validate_object_lock_headers(self) -> None:
        bucket = self.backend.get_bucket(self.bucket_name)
        lock_enabled = bucket.object_lock_enabled

        lock_mode = self._get_param("ObjectLockMode")
        lock_until = self._get_param("ObjectLockRetainUntilDate")
        legal_hold = self._get_param("ObjectLockLegalHoldStatus")
        has_checksum = self._get_param("ContentMD5") or self._get_param(
            "ChecksumAlgorithm"
        )

        if lock_mode or lock_until or legal_hold == "ON":
            if not has_checksum:
                raise MissingUploadObjectWithObjectLockHeaders
            if not lock_enabled:
                raise LockNotEnabled

        elif lock_enabled and bucket.has_default_lock:
            if not has_checksum:
                raise MissingUploadObjectWithObjectLockHeaders

    @property
    def is_access_point(self) -> bool:
        return ".s3-accesspoint." in self.headers["host"]

    @property
    def backend(self) -> S3Backend:
        return s3_backends[self.current_account][get_partition(self.region)]

    @property
    def should_autoescape(self) -> bool:
        return True

    def list_buckets(self) -> ActionResult:
        prefix = self._get_param("Prefix")
        bucket_region = self._get_param("BucketRegion")
        max_buckets = self._get_param("MaxBuckets")
        all_buckets = self.backend.list_buckets(
            prefix=prefix, bucket_region=bucket_region, max_buckets=max_buckets
        )
        buckets = [
            {"Name": bucket.name, "CreationDate": bucket.creation_date_ISO8601}
            for bucket in all_buckets
        ]
        result = {
            "Owner": {
                "ID": "bcaf1ffd86f41161ca5fb16fd081034f",
                "DisplayName": "webfile",
            },
            "Buckets": buckets,
        }
        return ActionResult(result)

    def bucket_cors_preflight_check(self) -> ActionResult:
        try:
            bucket = self.backend.head_bucket(self.bucket_name)
        except MissingBucket:
            # AWS S3 seems to return 403 on OPTIONS and 404 on GET/HEAD
            raise Forbidden(description="")

        self._set_cors_headers_options(self.headers, bucket)
        return EmptyResult()

    def object_cors_preflight_check(self) -> ActionResult:
        # OPTIONS response doesn't depend on the key_name: always return 200 with CORS headers
        return self.bucket_cors_preflight_check()

    def abort_multipart_upload(self) -> ActionResult:
        upload_id = self._get_param("UploadId")
        self.backend.abort_multipart_upload(self.bucket_name, upload_id)
        return EmptyResult()

    @staticmethod
    def subdomain_based_buckets(request: Any) -> bool:
        if settings.S3_IGNORE_SUBDOMAIN_BUCKETNAME:
            return False
        host = request.headers.get("host", request.headers.get("Host"))
        if not host:
            host = urlparse(request.url).netloc

        custom_endpoints = settings.get_s3_custom_endpoints()
        if (
            host
            and custom_endpoints
            and any(host in endpoint for endpoint in custom_endpoints)
        ):
            # Default to path-based buckets for S3-compatible SDKs (Ceph, DigitalOcean Spaces, etc)
            return False

        if (
            not host
            or host.startswith("localhost")
            or host.startswith("localstack")
            or host.startswith("host.docker.internal")
            or re.match(r"^[^.]+$", host)
            or re.match(r"^.*\.svc\.cluster\.local:?\d*$", host)
        ):
            # Default to path-based buckets for (1) localhost, (2) localstack hosts (e.g. localstack.dev),
            # (3) local host names that do not contain a "." (e.g., Docker container host names), or
            # (4) kubernetes host names
            return False

        match = re.match(r"^([^\[\]:]+)(:\d+)?$", host)
        if match:
            match = re.match(
                r"((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)(\.|$)){4}", match.groups()[0]
            )
            if match:
                return False

        match = re.match(r"^\[(.+)\](:\d+)?$", host)
        if match:
            match = re.match(
                r"^(((?=.*(::))(?!.*\3.+\3))\3?|[\dA-F]{1,4}:)([\dA-F]{1,4}(\3|:\b)|\2){5}(([\dA-F]{1,4}(\3|:\b|$)|\2){2}|(((2[0-4]|1\d|[1-9])?\d|25[0-5])\.?\b){4})\Z",
                match.groups()[0],
                re.IGNORECASE,
            )
            if match:
                return False

        path_based = (
            host == "s3.amazonaws.com"
            or re.match(
                r"s3[\.\-](?:(?:dualstack|fips)[\.\-])?([^.]*)\.amazonaws\.com", host
            )
            or any(
                re.match(
                    r"s3[\.\-](?:(?:dualstack|fips)[\.\-])?([^.]*)\." + suffix, host
                )
                for suffix in ALT_DOMAIN_SUFFIXES
            )
        )
        return not path_based

    def parse_bucket_name_from_url(self, request: Any, url: str) -> str:
        bucket_name = ""
        if self.subdomain_based_buckets(request):
            bucket_name = bucket_name_from_url(url)  # type: ignore
        else:
            bucket_name = bucketpath_bucket_name_from_url(url)  # type: ignore

        if self.is_access_point:
            # import here to avoid circular dependency error
            from moto.s3control import s3control_backends

            ap_name = bucket_name[: -(len(self.current_account) + 1)]
            ap = s3control_backends[self.current_account][
                self.partition
            ].get_access_point(self.current_account, ap_name)
            bucket_name = ap.bucket

        return bucket_name

    def head_bucket(self) -> ActionResult:
        bucket = self.backend.head_bucket(self.bucket_name)
        self.response_headers["content-type"] = APP_XML
        return ActionResult({"BucketRegion": bucket.region_name})

    def _set_cors_headers_options(
        self, headers: dict[str, str], bucket: FakeBucket
    ) -> None:
        """
        TODO: smarter way of matching the right CORS rule:
        See https://docs.aws.amazon.com/AmazonS3/latest/userguide/cors.html

        "When Amazon S3 receives a preflight request from a browser, it evaluates
        the CORS configuration for the bucket and uses the first CORSRule rule
        that matches the incoming browser request to enable a cross-origin request."
        This here just uses all rules and the last rule will override the previous ones
        if they are re-defining the same headers.
        """

        def _to_string(header: list[str] | str | int) -> str:
            # We allow list and strs in header values. Transform lists in comma-separated strings
            if isinstance(header, list):
                return ", ".join(header)
            return str(header)

        for cors_rule in bucket.cors:
            if cors_rule.allowed_methods is not None:
                self.response_headers["Access-Control-Allow-Methods"] = _to_string(
                    cors_rule.allowed_methods
                )
            if cors_rule.allowed_origins is not None:
                origin = headers.get("Origin")
                if cors_matches_origin(origin, cors_rule.allowed_origins):  # type: ignore
                    self.response_headers["Access-Control-Allow-Origin"] = origin  # type: ignore
                else:
                    raise AccessForbidden(
                        "CORSResponse: This CORS request is not allowed. This is usually because the evalution of Origin, request method / Access-Control-Request-Method or Access-Control-Request-Headers are not whitelisted by the resource's CORS spec."
                    )
            if cors_rule.allowed_headers is not None:
                self.response_headers["Access-Control-Allow-Headers"] = _to_string(
                    cors_rule.allowed_headers
                )
            if cors_rule.exposed_headers is not None:
                self.response_headers["Access-Control-Expose-Headers"] = _to_string(
                    cors_rule.exposed_headers
                )
            if cors_rule.max_age_seconds is not None:
                self.response_headers["Access-Control-Max-Age"] = _to_string(
                    cors_rule.max_age_seconds
                )

    @staticmethod
    def _acl_to_dict(acl: Any) -> dict[str, Any]:
        """Convert an ACL object to a dict matching the botocore shape.

        Mirrors the logic and hardcoded values from the original
        S3_OBJECT_ACL_RESPONSE Jinja template.
        """
        grants = [
            {
                "Grantee": {
                    "Type": grant.grantees[0].type,
                    "URI": grant.grantees[0].uri or None,
                    "ID": grant.grantees[0].id or None,
                    "DisplayName": grant.grantees[0].display_name or None,
                },
                "Permission": grant.permissions[0],
            }
            for grant in acl.grants
        ]
        return {
            "Owner": {
                "ID": "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a",
                "DisplayName": "webfile",
            },
            "Grants": grants,
        }

    def _get_cors_headers_other(self) -> dict[str, Any]:
        """
        Returns a dictionary with the appropriate CORS headers
        Should be used for non-OPTIONS requests only
        Applicable if the 'Origin' header matches one of a CORS-rules - returns an empty dictionary otherwise
        """
        response_headers: dict[str, Any] = {}
        try:
            origin = self.headers.get("Origin")
            if not origin:
                return response_headers
            bucket = self.backend.get_bucket(self.bucket_name)

            def _to_string(header: list[str] | str | int) -> str:
                # We allow list and strs in header values. Transform lists in comma-separated strings
                if isinstance(header, list):
                    return ", ".join(header)
                return str(header)

            for cors_rule in bucket.cors:
                if cors_rule.allowed_origins is not None:
                    if cors_matches_origin(origin, cors_rule.allowed_origins):
                        response_headers["Access-Control-Allow-Origin"] = origin
                        if cors_rule.allowed_methods is not None:
                            response_headers["Access-Control-Allow-Methods"] = (
                                _to_string(cors_rule.allowed_methods)
                            )
                        if cors_rule.allowed_headers is not None:
                            response_headers["Access-Control-Allow-Headers"] = (
                                _to_string(cors_rule.allowed_headers)
                            )
                        if cors_rule.exposed_headers is not None:
                            response_headers["Access-Control-Expose-Headers"] = (
                                _to_string(cors_rule.exposed_headers)
                            )
                        if cors_rule.max_age_seconds is not None:
                            response_headers["Access-Control-Max-Age"] = _to_string(
                                cors_rule.max_age_seconds
                            )

                        return response_headers
        except S3ClientError:
            pass
        return response_headers

    def list_multipart_uploads(self) -> ActionResult:
        multiparts = list(
            self.backend.list_multipart_uploads(self.bucket_name).values()
        )
        prefix = self._get_param("Prefix")
        if prefix is not None:
            multiparts = [
                upload for upload in multiparts if upload.key_name.startswith(prefix)
            ]
        uploads = [
            {
                "Key": upload.key_name,
                "UploadId": upload.id,
                "Initiator": {
                    "ID": f"arn:aws:iam::{self.current_account}:user/user1-11111a31-17b5-4fb7-9df5-b111111f13de",
                    "DisplayName": "user1-11111a31-17b5-4fb7-9df5-b111111f13de",
                },
                "Owner": {
                    "ID": "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a",
                    "DisplayName": "webfile",
                },
                "StorageClass": "STANDARD",
                "Initiated": "2010-11-10T20:48:33.000Z",
                "ChecksumAlgorithm": upload.metadata.get("x-amz-checksum-algorithm"),
                "ChecksumType": upload.metadata.get("x-amz-checksum-type"),
            }
            for upload in multiparts
        ]
        result = {
            "Bucket": self.bucket_name,
            "KeyMarker": "",
            "UploadIdMarker": "",
            "MaxUploads": 1000,
            "IsTruncated": False,
            "Uploads": uploads,
        }
        return ActionResult(result)

    def list_objects(self) -> ActionResult:
        bucket = self.backend.get_bucket(self.bucket_name)
        prefix = self._get_param("Prefix")
        delimiter = self._get_param("Delimiter")
        max_keys = self._get_param("MaxKeys", settings.get_s3_default_max_keys())
        marker = self._get_param("Marker")
        encoding_type = self._get_param("EncodingType")

        (
            result_keys,
            result_folders,
            is_truncated,
            next_marker,
        ) = self.backend.list_objects(
            bucket=bucket,
            prefix=prefix,
            delimiter=delimiter,
            marker=marker,
            max_keys=max_keys,
        )

        contents = [
            {
                "Key": key.safe_name(encoding_type),
                "LastModified": key.last_modified_ISO8601,
                "ETag": key.etag,
                "Size": key.size,
                "StorageClass": key.storage_class,
                "Owner": {
                    "ID": "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a",
                    "DisplayName": "webfile",
                },
            }
            for key in result_keys
        ]

        result: dict[str, Any] = {
            "Name": bucket.name,
            "MaxKeys": max_keys,
            "IsTruncated": is_truncated,
            "Contents": contents,
        }
        if prefix is not None:
            result["Prefix"] = prefix
        if delimiter:
            result["Delimiter"] = delimiter
            result["CommonPrefixes"] = [{"Prefix": folder} for folder in result_folders]
        if encoding_type:
            result["EncodingType"] = encoding_type
        if next_marker:
            result["NextMarker"] = next_marker
        return ActionResult(result)

    def list_objects_v2(self) -> ActionResult:
        bucket = self.backend.get_bucket(self.bucket_name)

        continuation_token = self._get_param("ContinuationToken")
        if continuation_token is not None and continuation_token == "":
            raise InvalidContinuationToken()

        prefix = self._get_param("Prefix")
        delimiter = self._get_param("Delimiter")
        fetch_owner = self._get_param("FetchOwner", False)
        max_keys = self._get_param("MaxKeys", settings.get_s3_default_max_keys())
        start_after = self._get_param("StartAfter")
        encoding_type = self._get_param("EncodingType")

        (
            truncated_keys,
            is_truncated,
            next_continuation_token,
        ) = self.backend.list_objects_v2(
            bucket=bucket,
            prefix=prefix,
            delimiter=delimiter,
            continuation_token=continuation_token,
            start_after=start_after,
            max_keys=max_keys,
        )

        result_keys, result_folders = self._split_truncated_keys(truncated_keys)

        key_count = len(result_keys) + len(result_folders)

        if encoding_type == "url":
            prefix = urllib.parse.quote(prefix) if prefix else ""
            result_folders = [urllib.parse.quote(folder) for folder in result_folders]

        contents = []
        for key in result_keys:
            item: dict[str, Any] = {
                "Key": key.safe_name(encoding_type),
                "LastModified": key.last_modified_ISO8601,
                "ETag": key.etag,
                "Size": key.size,
                "StorageClass": key.storage_class,
            }
            if fetch_owner:
                item["Owner"] = {
                    "ID": "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a",
                    "DisplayName": "webfile",
                }
            if key.checksum_algorithm:
                item["ChecksumAlgorithm"] = [key.checksum_algorithm]
            contents.append(item)

        result: dict[str, Any] = {
            "Name": bucket.name,
            "Prefix": prefix or "",
            "MaxKeys": max_keys,
            "KeyCount": key_count,
            "IsTruncated": is_truncated,
            "Contents": contents,
        }
        if delimiter:
            result["Delimiter"] = delimiter
            result["CommonPrefixes"] = [{"Prefix": folder} for folder in result_folders]
        if encoding_type:
            result["EncodingType"] = encoding_type
        if next_continuation_token:
            result["NextContinuationToken"] = next_continuation_token
        if not continuation_token and start_after:
            result["StartAfter"] = start_after
        return ActionResult(result)

    def list_object_versions(self) -> ActionResult:
        delimiter = self._get_param("Delimiter")
        key_marker = self._get_param("KeyMarker")
        max_keys = self._get_param("MaxKeys", settings.get_s3_default_max_keys())
        prefix = self._get_param("Prefix", "")
        version_id_marker = self._get_param("VersionIdMarker")

        bucket = self.backend.get_bucket(self.bucket_name)
        (
            versions,
            common_prefixes,
            delete_markers,
            next_key_marker,
            next_version_id_marker,
        ) = self.backend.list_object_versions(
            self.bucket_name,
            delimiter=delimiter,
            key_marker=key_marker,
            max_keys=max_keys,
            prefix=prefix,
            version_id_marker=version_id_marker,
        )
        key_list = versions

        is_truncated = next_key_marker is not None

        owner = {
            "ID": "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a",
            "DisplayName": "webfile",
        }
        version_list = [
            {
                "Key": key.name,
                "VersionId": key.version_id if key.version_id is not None else "null",
                "IsLatest": getattr(key, "is_latest", False),
                "LastModified": key.last_modified_ISO8601,
                "ETag": key.etag,
                "Size": key.size,
                "StorageClass": key.storage_class,
                "Owner": owner,
            }
            for key in key_list
        ]
        delete_marker_list = [
            {
                "Key": marker.name,
                "VersionId": marker.version_id,
                "IsLatest": getattr(marker, "is_latest", False),
                "LastModified": marker.last_modified_ISO8601,
                "Owner": owner,
            }
            for marker in delete_markers
        ]

        result: dict[str, Any] = {
            "Name": bucket.name,
            "Prefix": prefix if prefix is not None else None,
            "Delimiter": delimiter,
            "KeyMarker": key_marker or "",
            "VersionIdMarker": version_id_marker or "",
            "MaxKeys": max_keys,
            "IsTruncated": is_truncated,
            "Versions": version_list,
            "DeleteMarkers": delete_marker_list,
        }
        if common_prefixes:
            result["CommonPrefixes"] = [{"Prefix": p} for p in common_prefixes]
        if is_truncated:
            result["NextKeyMarker"] = next_key_marker
            result["NextVersionIdMarker"] = next_version_id_marker
        return ActionResult(result)

    @staticmethod
    def _split_truncated_keys(truncated_keys: Any) -> Any:  # type: ignore[misc]
        result_keys = []
        result_folders = []
        for key in truncated_keys:
            if isinstance(key, FakeKey):
                result_keys.append(key)
            else:
                result_folders.append(key)
        return result_keys, result_folders

    def create_bucket(self) -> ActionResult:
        bucket_name = self.bucket_name
        # us-east-1, the default AWS region behaves a bit differently
        # - you should not use any location constraint
        location_constraint = self._get_param(
            "CreateBucketConfiguration.LocationConstraint"
        )
        if self.region == DEFAULT_REGION_NAME:
            # REGION = us-east-1 - we should never receive a LocationConstraint for `us-east-1`
            # However, you can create buckets in other regions from us-east-1
            if location_constraint == DEFAULT_REGION_NAME:
                raise InvalidLocationConstraintException

        else:
            # Non-Standard region - the LocationConstraint must be equal to the actual region the request was send to
            if not location_constraint:
                raise IllegalLocationConstraintException()
            if location_constraint != self.region:
                raise IncompatibleLocationConstraintException(location_constraint)

        bucket_region = (
            location_constraint if location_constraint else DEFAULT_REGION_NAME
        )
        bucket_namespace = self._get_param("BucketNamespace")
        if bucket_namespace == "account-regional":
            expected_suffix = f"-{self.get_current_account()}-{bucket_region}-an"
            if not bucket_name.endswith(expected_suffix):
                raise InvalidNamespaceHeaderException(
                    self.get_current_account(), bucket_region
                )
        try:
            new_bucket = self.backend.create_bucket(
                bucket_name, bucket_region, bucket_namespace=bucket_namespace
            )
        except BucketAlreadyExists:
            new_bucket = self.backend.get_bucket(bucket_name)
            if new_bucket.account_id == self.get_current_account():
                # special cases when the bucket belongs to self
                if (
                    new_bucket.region_name == DEFAULT_REGION_NAME
                    and self.region == DEFAULT_REGION_NAME
                ):
                    # us-east-1 has different behavior - creating a bucket there is an idempotent operation
                    pass
                else:
                    raise BucketAlreadyOwnedByYou(bucket_name=bucket_name)
            else:
                raise

        if self._get_param("ACL"):
            # TODO: Support the XML-based ACL format
            self.backend.put_bucket_acl(bucket_name, self._acl_from_headers())

        if self._get_param("ObjectLockEnabledForBucket"):
            new_bucket.object_lock_enabled = True
            new_bucket.versioning_status = "Enabled"

        ownership_rule = self._get_param("ObjectOwnership")
        if ownership_rule:
            new_bucket.ownership_rule = ownership_rule

        result: dict[str, Any] = {"Location": f"/{new_bucket.name}"}
        if new_bucket.bucket_namespace == "account-regional":
            result["BucketArn"] = new_bucket.arn
        return ActionResult(result)

    def put_object_lock_configuration(self) -> ActionResult:
        lock_config = self._get_param("ObjectLockConfiguration")
        if lock_config is None:
            raise MissingRequestBody()
        if "ObjectLockEnabled" not in lock_config:
            raise MalformedXML()
        default_retention = lock_config.get("Rule", {}).get("DefaultRetention")
        days = years = None
        if default_retention:
            days = default_retention.get("Days", 0)
            years = default_retention.get("Years", 0)
            if days and years:
                raise MalformedXML()

        self.backend.put_object_lock_configuration(
            self.bucket_name,
            lock_config["ObjectLockEnabled"] == "Enabled",
            (default_retention or {}).get("Mode"),
            days,
            years,
        )
        return EmptyResult()

    def put_bucket_versioning(self) -> ActionResult:
        status = self._get_param("VersioningConfiguration.Status")
        if not status:
            raise MalformedXML()
        self.backend.put_bucket_versioning(self.bucket_name, status)
        return EmptyResult()

    def put_bucket_lifecycle_configuration(self) -> ActionResult:
        rules = self._get_param("LifecycleConfiguration.Rules", [])
        self.backend.put_bucket_lifecycle_configuration(self.bucket_name, rules)
        return EmptyResult()

    def put_bucket_policy(self) -> ActionResult:
        self.backend.put_bucket_policy(self.bucket_name, self._get_param("Policy"))
        return EmptyResult()

    def put_bucket_acl(self) -> ActionResult:
        # Headers are first. If not set, then look at the body (consistent with the documentation):
        acls = self._acl_from_headers()
        if not acls:
            acls = self._acl_from_body()
        self.backend.put_bucket_acl(self.bucket_name, acls)
        return EmptyResult()

    def put_bucket_tagging(self) -> ActionResult:
        tagging = self._bucket_tagging_from_body()
        self.backend.put_bucket_tagging(self.bucket_name, tagging)
        return EmptyResult()

    def put_bucket_website(self) -> ActionResult:
        self.backend.put_bucket_website(
            self.bucket_name, self._get_param("WebsiteConfiguration")
        )
        return EmptyResult()

    def put_bucket_cors(self) -> ActionResult:
        self.backend.put_bucket_cors(
            self.bucket_name, self._get_param("CORSConfiguration.CORSRules", [])
        )
        return EmptyResult()

    def put_bucket_logging(self) -> ActionResult:
        self.backend.put_bucket_logging(
            self.bucket_name, self._get_param("BucketLoggingStatus.LoggingEnabled")
        )
        return EmptyResult()

    def put_bucket_notification_configuration(self) -> ActionResult:
        self.backend.put_bucket_notification_configuration(
            self.bucket_name, self._get_param("NotificationConfiguration", {})
        )
        return EmptyResult()

    def put_bucket_accelerate_configuration(self) -> ActionResult:
        self.backend.put_bucket_accelerate_configuration(
            self.bucket_name, self._get_param("AccelerateConfiguration.Status")
        )
        return EmptyResult()

    def put_public_access_block(self) -> ActionResult:
        self.backend.put_public_access_block(
            self.bucket_name, self._get_param("PublicAccessBlockConfiguration")
        )
        return EmptyResult()

    def put_bucket_encryption(self) -> ActionResult:
        self.backend.put_bucket_encryption(
            self.bucket_name, self._get_param("ServerSideEncryptionConfiguration")
        )
        return EmptyResult()

    def put_bucket_replication(self) -> ActionResult:
        bucket = self.backend.get_bucket(self.bucket_name)
        if not bucket.is_versioned:
            raise VersioningNotEnabledForReplication(bucket_name=self.bucket_name)
        self.backend.put_bucket_replication(
            self.bucket_name, self._get_param("ReplicationConfiguration")
        )
        return EmptyResult()

    def put_bucket_ownership_controls(self) -> ActionResult:
        rules = self._get_param("OwnershipControls.Rules", [])
        if not rules or not rules[0].get("ObjectOwnership"):
            raise MalformedXML()
        self.backend.put_bucket_ownership_controls(
            self.bucket_name, ownership=rules[0]["ObjectOwnership"]
        )
        return EmptyResult()

    def put_bucket_inventory_configuration(self) -> ActionResult:
        self.backend.put_bucket_inventory_configuration(
            self.bucket_name, self._get_param("InventoryConfiguration")
        )
        return EmptyResult()

    def get_bucket_accelerate_configuration(self) -> ActionResult:
        accelerate_configuration = self.backend.get_bucket_accelerate_configuration(
            self.bucket_name
        )
        result = {"Status": accelerate_configuration}
        return ActionResult(result)

    def get_bucket_acl(self) -> ActionResult:
        acl = self.backend.get_bucket_acl(self.bucket_name)
        return ActionResult(self._acl_to_dict(acl))

    def get_bucket_cors(self) -> ActionResult:
        cors = self.backend.get_bucket_cors(self.bucket_name)
        if len(cors) == 0:
            raise NoSuchCORSConfiguration(bucket_name=self.bucket_name)
        cors_rules = [
            {
                "ID": rule.id_,
                "AllowedOrigins": rule.allowed_origins,
                "AllowedMethods": rule.allowed_methods,
                "AllowedHeaders": rule.allowed_headers,
                "ExposeHeaders": rule.exposed_headers,
                "MaxAgeSeconds": rule.max_age_seconds,
            }
            for rule in cors
        ]
        return ActionResult({"CORSRules": cors_rules})

    def get_bucket_encryption(self) -> ActionResult:
        encryption = self.backend.get_bucket_encryption(self.bucket_name)
        if not encryption:
            raise ServerSideEncryptionConfigurationNotFoundError(
                bucket_name=self.bucket_name
            )
        rules = [
            {
                "ApplyServerSideEncryptionByDefault": rule[
                    "ApplyServerSideEncryptionByDefault"
                ],
                "BucketKeyEnabled": rule.get("BucketKeyEnabled", False),
            }
            for rule in encryption["Rules"]
        ]
        return ActionResult({"ServerSideEncryptionConfiguration": {"Rules": rules}})

    def get_bucket_lifecycle_configuration(self) -> ActionResult:
        rules = self.backend.get_bucket_lifecycle(self.bucket_name)
        if not rules:
            raise NoSuchLifecycleConfiguration(bucket_name=self.bucket_name)

        lifecycle_rules = []
        for rule in rules:
            r: dict[str, Any] = {"ID": rule.id, "Status": rule.status}

            if rule.filter:
                f: dict[str, Any] = {}
                if rule.filter.prefix is not None:
                    f["Prefix"] = rule.filter.prefix
                if rule.filter.tag_key:
                    f["Tag"] = {
                        "Key": rule.filter.tag_key,
                        "Value": rule.filter.tag_value,
                    }
                if rule.filter.and_filter:
                    and_f: dict[str, Any] = {}
                    if rule.filter.and_filter.prefix is not None:
                        and_f["Prefix"] = rule.filter.and_filter.prefix
                    if rule.filter.and_filter.tags:
                        and_f["Tags"] = [
                            {"Key": k, "Value": v}
                            for k, v in rule.filter.and_filter.tags.items()
                        ]
                    f["And"] = and_f
                r["Filter"] = f
            else:
                if rule.prefix is not None:
                    r["Prefix"] = rule.prefix
                else:
                    r["Filter"] = {}

            if rule.transitions:
                r["Transitions"] = [
                    {
                        k: v
                        for k, v in [
                            ("Days", t.days),
                            ("Date", t.date),
                            ("StorageClass", t.storage_class),
                        ]
                        if v is not None
                    }
                    for t in rule.transitions
                ]

            if (
                rule.expiration_days
                or rule.expiration_date
                or rule.expired_object_delete_marker is not None
            ):
                exp: dict[str, Any] = {}
                if rule.expiration_days:
                    exp["Days"] = rule.expiration_days
                if rule.expiration_date:
                    exp["Date"] = rule.expiration_date
                if rule.expired_object_delete_marker is not None:
                    exp["ExpiredObjectDeleteMarker"] = rule.expired_object_delete_marker
                r["Expiration"] = exp

            if rule.noncurrent_version_transitions:
                r["NoncurrentVersionTransitions"] = [
                    {
                        k: v
                        for k, v in [
                            ("NewerNoncurrentVersions", nvt.newer_versions),
                            ("NoncurrentDays", nvt.days),
                            ("StorageClass", nvt.storage_class),
                        ]
                        if v is not None
                    }
                    for nvt in rule.noncurrent_version_transitions
                ]

            if rule.nve_noncurrent_days:
                r["NoncurrentVersionExpiration"] = {
                    "NoncurrentDays": rule.nve_noncurrent_days
                }

            if rule.aimu_days:
                r["AbortIncompleteMultipartUpload"] = {
                    "DaysAfterInitiation": rule.aimu_days
                }

            lifecycle_rules.append(r)

        result = {
            "Rules": lifecycle_rules,
            "TransitionDefaultMinimumObjectSize": TransitionDefaultMinimumObjectSize.ALL_STORAGE_CLASSES_128K.value,
        }
        return ActionResult(result)

    def get_bucket_location(self) -> ActionResult:
        location: str | None = self.backend.get_bucket_location(self.bucket_name)

        # us-east-1 is different - returns a None location
        if location == DEFAULT_REGION_NAME:
            location = None
        return ActionResult({"LocationConstraint": location})

    def get_bucket_logging(self) -> ActionResult:
        logging = self.backend.get_bucket_logging(self.bucket_name)
        result: dict[str, Any] = {}
        if logging:
            logging_enabled: dict[str, Any] = {
                "TargetBucket": logging["TargetBucket"],
                "TargetPrefix": logging["TargetPrefix"],
            }
            if logging.get("TargetGrants"):
                logging_enabled["TargetGrants"] = [
                    {
                        "Grantee": {
                            "Type": grant.grantees[0].type,
                            "URI": grant.grantees[0].uri,
                            "ID": grant.grantees[0].id,
                            "DisplayName": grant.grantees[0].display_name,
                        },
                        "Permission": grant.permissions[0],
                    }
                    for grant in logging["TargetGrants"]
                ]
            result["LoggingEnabled"] = logging_enabled
        return ActionResult(result)

    def get_bucket_notification_configuration(self) -> ActionResult:
        notification_configuration = self.backend.get_bucket_notification_configuration(
            self.bucket_name
        )
        if not notification_configuration:
            return EmptyResult()

        def _build_config(item: Any, arn_key: str) -> dict[str, Any]:
            config: dict[str, Any] = {
                "Id": item.id,
                arn_key: item.arn,
                "Events": item.events,
            }
            if item.filters:
                config["Filter"] = item.filters
            return config

        result: dict[str, Any] = {
            "TopicConfigurations": [
                _build_config(t, "TopicArn") for t in notification_configuration.topic
            ],
            "QueueConfigurations": [
                _build_config(q, "QueueArn") for q in notification_configuration.queue
            ],
            "LambdaFunctionConfigurations": [
                _build_config(cf, "LambdaFunctionArn")
                for cf in notification_configuration.cloud_function
            ],
        }
        return ActionResult(result)

    def get_bucket_ownership_controls(self) -> ActionResult:
        ownership_rule = self.backend.get_bucket_ownership_controls(self.bucket_name)
        if not ownership_rule:
            raise OwnershipControlsNotFoundError(bucket_name=self.bucket_name)
        return ActionResult(
            {
                "OwnershipControls": {
                    "Rules": [{"ObjectOwnership": ownership_rule}],
                },
            }
        )

    def get_bucket_policy(self) -> ActionResult:
        policy = self.backend.get_bucket_policy(self.bucket_name)
        if not policy:
            raise NoSuchBucketPolicy(bucket_name=self.bucket_name)
        return ActionResult({"Policy": policy})

    def get_bucket_policy_status(self) -> ActionResult:
        policy = self.backend.get_bucket_policy(self.bucket_name)
        if not policy:
            raise NoSuchBucketPolicy(bucket_name=self.bucket_name)
        return ActionResult(
            {"PolicyStatus": {"IsPublic": bucket_policy_is_public(policy)}}
        )

    def get_bucket_replication(self) -> ActionResult:
        replication = self.backend.get_bucket_replication(self.bucket_name)
        if not replication:
            raise ReplicationConfigurationNotFoundError(bucket_name=self.bucket_name)
        rules = [
            {
                "ID": rule["ID"],
                "Priority": rule["Priority"],
                "Status": rule["Status"],
                "DeleteMarkerReplication": rule.get(
                    "DeleteMarkerReplication", {"Status": "Disabled"}
                ),
                "Filter": rule.get("Filter", {"Prefix": ""}),
                "Destination": rule["Destination"],
            }
            for rule in replication["Rules"]
        ]
        return ActionResult(
            {
                "ReplicationConfiguration": {
                    "Role": replication["Role"],
                    "Rules": rules,
                },
            }
        )

    def get_bucket_tagging(self) -> ActionResult:
        tags = self.backend.get_bucket_tagging(self.bucket_name)["Tags"]
        # "Special Error" if no tags:
        if len(tags) == 0:
            raise NoSuchTagSet(bucket_name=self.bucket_name)
        return ActionResult({"TagSet": tags})

    def get_bucket_versioning(self) -> ActionResult:
        versioning = self.backend.get_bucket_versioning(self.bucket_name)
        result = {"Status": versioning}
        return ActionResult(result)

    def get_bucket_website(self) -> ActionResult:
        website_configuration = self.backend.get_bucket_website_configuration(
            self.bucket_name
        )
        if not website_configuration:
            raise NoSuchWebsiteConfiguration(bucket_name=self.bucket_name)
        result = {
            "RedirectAllRequestsTo": website_configuration.get("RedirectAllRequestsTo"),
            "IndexDocument": website_configuration.get("IndexDocument"),
            "ErrorDocument": website_configuration.get("ErrorDocument"),
            "RoutingRules": website_configuration.get("RoutingRules"),
        }
        return ActionResult(result)

    def get_object_acl(self) -> ActionResult:
        key, not_modified = self._get_key()
        self.response_headers.update(self._get_cors_headers_other())
        if key.version_id != "null":
            self.response_headers["x-amz-version-id"] = key.version_id
        if not_modified:
            raise NotModified()

        acl = self.backend.get_object_acl(key)
        return ActionResult(self._acl_to_dict(acl))

    def get_object_lock_configuration(self) -> ActionResult:
        (
            lock_enabled,
            mode,
            days,
            years,
        ) = self.backend.get_object_lock_configuration(self.bucket_name)
        config: dict[str, Any] = {
            "ObjectLockEnabled": "Enabled" if lock_enabled else "Disabled",
        }
        if mode:
            config["Rule"] = {
                "DefaultRetention": {
                    "Mode": mode,
                    "Days": days,
                    "Years": years,
                },
            }
        return ActionResult({"ObjectLockConfiguration": config})

    def get_public_access_block(self) -> ActionResult:
        public_access_block = self.backend.get_public_access_block(self.bucket_name)
        result = {
            "PublicAccessBlockConfiguration": {
                "BlockPublicAcls": public_access_block.block_public_acls,
                "IgnorePublicAcls": public_access_block.ignore_public_acls,
                "BlockPublicPolicy": public_access_block.block_public_policy,
                "RestrictPublicBuckets": public_access_block.restrict_public_buckets,
            }
        }
        return ActionResult(result)

    def get_bucket_inventory_configuration(self) -> ActionResult:
        config_id = self._get_param("Id")
        inventory_configuration = self.backend.get_bucket_inventory_configuration(
            bucket_name=self.bucket_name, id=config_id
        )
        config = {
            "Destination": inventory_configuration.destination,
            "IsEnabled": inventory_configuration.is_enabled,
            "Filter": inventory_configuration.filters,
            "Id": inventory_configuration.id,
            "IncludedObjectVersions": inventory_configuration.included_object_versions,
            "OptionalFields": inventory_configuration.optional_fields,
            "Schedule": inventory_configuration.schedule,
        }
        return ActionResult({"InventoryConfiguration": config})

    def list_bucket_inventory_configurations(self) -> ActionResult:
        inventory_configuration_list = (
            self.backend.list_bucket_inventory_configurations(
                bucket_name=self.bucket_name,
            )
        )
        configs = [
            {
                "Destination": inv.destination,
                "IsEnabled": inv.is_enabled,
                "Filter": inv.filters,
                "Id": inv.id,
                "IncludedObjectVersions": inv.included_object_versions,
                "OptionalFields": inv.optional_fields,
                "Schedule": inv.schedule,
            }
            for inv in inventory_configuration_list
        ]

        # TODO: Add support for pagination/ continuation tokens
        result = {
            "InventoryConfigurationList": configs,
            "IsTruncated": False,
        }
        return ActionResult(result)

    def delete_bucket(self) -> ActionResult:
        removed_bucket = self.backend.delete_bucket(self.bucket_name)
        if removed_bucket:
            # Bucket exists
            return EmptyResult()
        else:
            # Tried to delete a bucket that still has keys
            raise BucketNotEmpty(bucket_name=self.bucket_name)

    def delete_bucket_ownership_controls(self) -> ActionResult:
        self.backend.delete_bucket_ownership_controls(self.bucket_name)
        return EmptyResult()

    def delete_bucket_replication(self) -> ActionResult:
        self.backend.delete_bucket_replication(self.bucket_name)
        return EmptyResult()

    def delete_bucket_encryption(self) -> ActionResult:
        self.backend.delete_bucket_encryption(self.bucket_name)
        return EmptyResult()

    def delete_public_access_block(self) -> ActionResult:
        self.backend.delete_public_access_block(self.bucket_name)
        return EmptyResult()

    def delete_bucket_lifecycle(self) -> ActionResult:
        self.backend.delete_bucket_lifecycle(self.bucket_name)
        return EmptyResult()

    def delete_bucket_cors(self) -> ActionResult:
        self.backend.delete_bucket_cors(self.bucket_name)
        return EmptyResult()

    def delete_bucket_website(self) -> ActionResult:
        self.backend.delete_bucket_website(self.bucket_name)
        return EmptyResult()

    def delete_bucket_tagging(self) -> ActionResult:
        self.backend.delete_bucket_tagging(self.bucket_name)
        return EmptyResult()

    def delete_bucket_policy(self) -> ActionResult:
        self.backend.delete_bucket_policy(self.bucket_name)
        return EmptyResult()

    def post_object(self) -> ActionResult:
        key = self.querystring["key"][0]
        f = self.body

        if "success_action_redirect" in self.querystring:
            redirect = self.querystring["success_action_redirect"][0]
            parts = urlparse(redirect)
            queryargs: dict[str, Any] = parse_qs(parts.query)
            queryargs["key"] = key
            queryargs["bucket"] = self.bucket_name
            redirect_queryargs = urlencode(queryargs, doseq=True)
            newparts = (
                parts.scheme,
                parts.netloc,
                parts.path,
                parts.params,
                redirect_queryargs,
                parts.fragment,
            )
            fixed_redirect = urlunparse(newparts)

            self.response_headers["Location"] = fixed_redirect

        if "success_action_status" in self.querystring:
            status_code = self.querystring["success_action_status"][0]
        elif "success_action_redirect" in self.querystring:
            status_code = 303
        else:
            status_code = 204
        self.response_headers["status"] = str(status_code)

        new_key = self.backend.put_object(
            self.bucket_name, key, f, request_method=self.method
        )

        if self.querystring.get("acl"):
            acl = get_canned_acl(self.querystring["acl"][0])
            new_key.set_acl(acl)

        # Metadata
        metadata = metadata_from_headers(self.form_data)
        new_key.set_metadata(metadata)

        return EmptyResult()

    def delete_objects(self) -> ActionResult:
        bucket_name = self.bucket_name
        body_dict = xmltodict.parse(self.body, strip_whitespace=False)
        bypass_retention = self._get_param("BypassGovernanceRetention", False)

        objects = body_dict["Delete"].get("Object", [])
        if not isinstance(objects, list):
            # We expect a list of objects, but when there is a single <Object> node xmltodict does not
            # return a list.
            objects = [objects]
        if len(objects) == 0:
            raise MalformedXML()

        if not self._delete_objects_access_denied:
            objects_to_delete = []
            errors = []
            for obj in objects:
                from moto.iam.access_control import PermissionResult

                bucket = self.backend.get_bucket(bucket_name)
                perm = bucket.get_permission(
                    "s3:DeleteObject", f"arn:aws:s3:::{bucket_name}/{obj['Key']}"
                )
                if perm == PermissionResult.DENIED:
                    errors.append((obj["Key"], "AccessDenied", "Access Denied"))
                else:
                    objects_to_delete.append(obj)
            deleted, errored = self.backend.delete_objects(
                bucket_name, objects_to_delete, bypass_retention
            )
            errors.extend(
                [
                    (
                        err,
                        "AccessDenied",
                        "Access Denied because object protected by object lock.",
                    )
                    for err in errored
                ]
            )
        else:
            deleted = []
            # [(key_name, errorcode, 'error message'), ..]
            errors = [(o["Key"], "AccessDenied", "Access Denied") for o in objects]

        deleted_objects = [
            {
                "Key": k,
                "VersionId": v or None,
                "DeleteMarkerVersionId": dv or None,
                "DeleteMarker": True if dv else None,
            }
            for k, v, dv in deleted
        ]
        error_objects = [{"Key": k, "Code": c, "Message": m} for k, c, m in errors]
        result = {
            "Deleted": deleted_objects,
            "Errors": error_objects,
        }
        return ActionResult(result)

    def _handle_v4_chunk_signatures(self, body: bytes, content_length: int) -> bytes:
        body_io = io.BytesIO(body)
        new_body = bytearray(content_length)
        pos = 0
        line = body_io.readline()
        while line:
            # https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html#sigv4-chunked-body-definition
            # str(hex(chunk-size)) + ";chunk-signature=" + signature + \r\n + chunk-data + \r\n
            chunk_size = int(line[: line.find(b";")].decode("utf8"), 16)
            if chunk_size == 0:
                # Signature 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER' will have trailing parts that define the checksum
                # We can safely ignore those
                break
            new_body[pos : pos + chunk_size] = body_io.read(chunk_size)
            pos = pos + chunk_size
            body_io.read(2)  # skip trailing \r\n
            line = body_io.readline()
        return bytes(new_body)

    def _handle_encoded_body(self, body: io.BufferedIOBase | bytes) -> bytes:
        decoded_body = b""
        if not body:
            return decoded_body
        if isinstance(body, bytes):
            body = io.BytesIO(body)
        # first line should equal '{content_length}\r\n' while the content_length is a hex number
        content_length = int(body.readline().strip(), 16)
        while content_length > 0:
            # read the content_length of the actual data
            decoded_body += body.read(content_length)
            # next is line with just '\r\n' so we skip it
            body.readline()
            # read another line with '{content_length}\r\n'
            content_length = int(body.readline().strip(), 16)

        return decoded_body

    def get_object(self) -> ActionResult:
        key, not_modified = self._get_key()
        self.response_headers.update(self._get_cors_headers_other())

        if not_modified:
            # Real S3 omits any content-* headers for a 304
            self.response_headers["ETag"] = key.etag
            self.response_headers["Last-Modified"] = key.last_modified_RFC1123
            if key.version_id != "null":
                self.response_headers["x-amz-version-id"] = key.version_id
            raise NotModified()

        metadata = {name.lower(): value for name, value in key.metadata.items()}
        result: dict[str, Any] = {
            "Body": key.value,
            "AcceptRanges": "bytes",
            "Restore": key.restore_status,
            "LastModified": key.last_modified,
            "ContentLength": key.size,
            "ETag": key.etag,
            "VersionId": key.version_id
            if key.version_id != "null" or key.is_versioned
            else None,
            "CacheControl": metadata.get("cache-control"),
            "ContentDisposition": metadata.get("content-disposition"),
            "ContentEncoding": metadata.get("content-encoding"),
            "ContentLanguage": metadata.get("content-language"),
            "ContentType": metadata.get("content-type"),
            "Expires": metadata.get("expires"),
            "WebsiteRedirectLocation": key.website_redirect_location
            or metadata.get("x-amz-website-redirect-location"),
            "ServerSideEncryption": key.encryption,
            "Metadata": {
                name[len("x-amz-meta-") :]: value
                for name, value in metadata.items()
                if name.startswith("x-amz-meta-")
            },
            "SSEKMSKeyId": key.kms_key_id if key.encryption == "aws:kms" else None,
            "BucketKeyEnabled": str(key.bucket_key_enabled).lower() == "true"
            if key.encryption == "aws:kms" and key.bucket_key_enabled is not None
            else None,
            "StorageClass": key.storage_class
            if key.storage_class != "STANDARD"
            else None,
            "TagCount": len(self.backend.get_object_tagging(key)["Tags"]) or None,
            "ObjectLockMode": key.lock_mode,
            "ObjectLockRetainUntilDate": key.lock_until,
            "ObjectLockLegalHoldStatus": key.lock_legal_status,
        }

        # set the checksum after not_modified has been checked
        if self._get_param("ChecksumMode") == "ENABLED" and key.checksum_algorithm:
            qualified_checksum = key.checksum_value
            if key.checksum_type == "COMPOSITE":
                qualified_checksum = f"{key.checksum_value}-{key.checksum_parts}"
            result[f"Checksum{key.checksum_algorithm.upper()}"] = qualified_checksum
            result["ChecksumType"] = key.checksum_type

        for param_name, member_name in ALLOWED_HEADER_OVERRIDES.items():
            if (value := self._get_param(param_name)) is not None:
                if isinstance(value, datetime.datetime):
                    value = rfc_1123_datetime(value)
                result[member_name] = value

        part_number = self._get_param("PartNumber")
        if part_number:
            if key.multipart:
                # TODO
                pass
            else:
                if part_number > 1:
                    raise RangeNotSatisfiable
                result["ContentRange"] = f"bytes 0-{key.size - 1}/{key.size}"
                self.response_headers["status"] = "206"
                return ActionResult(result)

        byte_range = self._get_range(key.size)
        if byte_range:
            begin, end = byte_range
            if begin or end < key.size - 1:
                # range requests do not return the checksum
                for member_name in list(result):
                    if member_name.startswith("Checksum"):
                        result.pop(member_name)
            result["Body"] = key.value[begin : end + 1]
            result["ContentLength"] = len(result["Body"])
            result["ContentRange"] = f"bytes {begin}-{end}/{key.size}"
            self.response_headers["status"] = "206"
        return ActionResult(result)

    def get_object_attributes(self) -> ActionResult:
        # Get the Key, but do not validate StorageClass - we can retrieve the attributes of Glacier-objects
        key, not_modified = self._get_key(validate_storage_class=False)
        self.response_headers.update(self._get_cors_headers_other())
        if not_modified:
            raise NotModified()

        attributes_to_get = self._get_param("ObjectAttributes", [])
        response_keys = self.backend.get_object_attributes(key, attributes_to_get)

        checksum = response_keys["checksum"]
        result: dict[str, Any] = {
            "LastModified": key.last_modified,
            "VersionId": key.version_id if key.version_id != "null" else None,
            "ETag": response_keys["etag"],
            "Checksum": {f"Checksum{algo}": value for algo, value in checksum.items()}
            if checksum
            else None,
            "ObjectSize": response_keys["size"],
            "StorageClass": response_keys["storage_class"],
        }
        if checksum:
            result["Checksum"]["ChecksumType"] = checksum.get("type")
        return ActionResult(result)

    def get_object_legal_hold(self) -> ActionResult:
        key, not_modified = self._get_key()
        self.response_headers.update(self._get_cors_headers_other())
        if not_modified:
            raise NotModified()

        if key.version_id != "null":
            self.response_headers["x-amz-version-id"] = key.version_id

        legal_hold = self.backend.get_object_legal_hold(key)
        return ActionResult({"LegalHold": {"Status": legal_hold}})

    def get_object_tagging(self) -> ActionResult:
        key, not_modified = self._get_key()
        self.response_headers.update(self._get_cors_headers_other())
        if not_modified:
            raise NotModified()

        tags = self.backend.get_object_tagging(key)["Tags"]
        result = {
            "VersionId": key.version_id if key.version_id != "null" else None,
            "TagSet": tags,
        }
        return ActionResult(result)

    def list_parts(self) -> ActionResult:
        response_headers = self._get_cors_headers_other()

        upload_id = self._get_param("UploadId")

        # 0 <= PartNumberMarker <= 2,147,483,647
        part_number_marker = self._get_param("PartNumberMarker", 0)
        if part_number_marker > 2147483647:
            raise NotAnIntegerException(
                name="part-number-marker", value=part_number_marker
            )
        if not (0 <= part_number_marker <= 2147483647):
            raise InvalidMaxPartArgument("part-number-marker", 0, 2147483647)

        # 0 <= MaxParts <= 2,147,483,647 (default is 1,000)
        max_parts = self._get_param("MaxParts", 1000)
        if max_parts > 2147483647:
            raise NotAnIntegerException(name="max-parts", value=max_parts)
        if not (0 <= max_parts <= 2147483647):
            raise InvalidMaxPartArgument("max-parts", 0, 2147483647)

        parts = self.backend.list_parts(
            self.bucket_name,
            upload_id,
            part_number_marker=part_number_marker,
            max_parts=max_parts,
        )
        next_part_number_marker = parts[-1].name if parts else 0
        is_truncated = len(parts) != 0 and self.backend.is_truncated(
            self.bucket_name,
            upload_id,
            next_part_number_marker,  # type: ignore
        )

        key_name = self._get_param("Key")
        parts_list = [
            {
                "PartNumber": part.name,
                "LastModified": part.last_modified_ISO8601,
                "ETag": part.etag,
                "Size": part.size,
            }
            for part in parts
        ]
        result = {
            "Bucket": self.bucket_name,
            "Key": key_name,
            "UploadId": upload_id,
            "StorageClass": "STANDARD",
            "Initiator": {
                "ID": "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a",
                "DisplayName": "webfile",
            },
            "Owner": {
                "ID": "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a",
                "DisplayName": "webfile",
            },
            "PartNumberMarker": part_number_marker,
            "NextPartNumberMarker": next_part_number_marker,
            "MaxParts": max_parts,
            "IsTruncated": is_truncated,
            "Parts": parts_list,
        }
        self.response_headers.update(response_headers)
        return ActionResult(result)

    def _get_key(self, validate_storage_class: bool = True) -> tuple[FakeKey, bool]:
        key_name = self._get_param("Key")
        version_id = self._get_param("VersionId")
        if_modified_since = self._get_param("IfModifiedSince")
        if_match = self._get_param("IfMatch")
        if_none_match = self._get_param("IfNoneMatch")
        if_unmodified_since = self._get_param("IfUnmodifiedSince")

        key = self.backend.get_object(self.bucket_name, key_name, version_id=version_id)
        if key is None and version_id is None:
            raise MissingKey(key=key_name)
        elif key is None:
            raise MissingVersion()
        if validate_storage_class and key.storage_class in ARCHIVE_STORAGE_CLASSES:
            if 'ongoing-request="false"' not in (key.restore_status or ""):
                raise InvalidObjectState(storage_class=key.storage_class)
        if if_unmodified_since:
            if key.last_modified.replace(microsecond=0) > if_unmodified_since:
                raise PreconditionFailed("If-Unmodified-Since")
        if if_match and key.etag not in [if_match, f'"{if_match}"']:
            raise PreconditionFailed("If-Match")
        not_modified = False
        if if_modified_since:
            if key.last_modified.replace(microsecond=0) <= if_modified_since:
                not_modified = True
        if if_none_match and key.etag in [if_none_match, f'"{if_none_match}"']:
            not_modified = True
        return key, not_modified

    def put_object(self) -> ActionResult:
        self._validate_object_lock_headers()
        key_name = self._get_param("Key")
        self.response_headers.update(self._get_cors_headers_other())

        storage_class = self._get_param("StorageClass", "STANDARD")
        encryption = self._get_param("ServerSideEncryption")
        kms_key_id = self._get_param("SSEKMSKeyId")
        if_match = self._get_param("IfMatch")
        if_none_match = self._get_param("IfNoneMatch")
        bucket_key_enabled = self._get_param("BucketKeyEnabled")

        if if_match:
            if not (obj := self.backend.get_object(self.bucket_name, key_name)):
                raise MissingKey
            # Check if the ETags are the same. S3 doesn't seem to care about quotes, so we shouldn't either
            elif if_match.replace('"', "") != obj.etag.replace('"', ""):
                raise PreconditionFailed("If-Match")
        if (
            if_none_match == "*"
            and self.backend.get_object(self.bucket_name, key_name) is not None
        ):
            raise PreconditionFailed("If-None-Match")

        checksum_algorithm, checksum_value = self._get_checksum()

        bucket = self.backend.get_bucket(self.bucket_name)
        lock_enabled = bucket.object_lock_enabled

        legal_hold, lock_mode, lock_until = self._get_lock_details(bucket, lock_enabled)

        acl = self._acl_from_headers()
        if acl is None:
            acl = bucket.acl
        tagging = self._tagging_from_headers()

        new_key = self.backend.put_object(
            self.bucket_name,
            key_name,
            self._get_param("Body", b""),
            storage=storage_class,
            encryption=encryption,
            kms_key_id=kms_key_id,
            bucket_key_enabled=bucket_key_enabled,
            lock_mode=lock_mode,
            lock_legal_status=legal_hold,
            lock_until=lock_until,
            checksum_value=checksum_value,
        )
        metadata = metadata_from_headers(self.headers)
        metadata.update(metadata_from_headers(self.querystring))
        new_key.set_metadata(metadata)
        new_key.set_acl(acl)
        new_key.website_redirect_location = self._get_param("WebsiteRedirectLocation")
        if checksum_algorithm:
            new_key.checksum_algorithm = checksum_algorithm
        self.backend.put_object_tagging(new_key, tagging)

        result: dict[str, Any] = {
            "ETag": new_key.etag,
            "ServerSideEncryption": new_key.encryption,
            "VersionId": new_key.version_id if new_key.is_versioned else None,
            "SSEKMSKeyId": new_key.kms_key_id
            if new_key.encryption == "aws:kms"
            else None,
            "BucketKeyEnabled": str(new_key.bucket_key_enabled).lower() == "true"
            if new_key.encryption == "aws:kms"
            and new_key.bucket_key_enabled is not None
            else None,
        }
        if checksum_algorithm:
            result[f"Checksum{checksum_algorithm.upper()}"] = checksum_value
        return ActionResult(result)

    def _get_checksum(self) -> tuple[str, str | None]:
        checksum_algorithm = self._get_param("ChecksumAlgorithm", "")
        checksum_value = self._get_param(f"Checksum{checksum_algorithm.upper()}")
        if not checksum_value and checksum_algorithm:
            # Extract the checksum-value from the trailer of an aws-chunked body
            search = re.search(rb"x-amz-checksum-\w+:(.+={1,2})", self.raw_body)
            checksum_value = search.group(1) if search else None
        if checksum_value:
            # TODO: AWS computes the provided value and verifies it's the same
            # Afterwards, it should be returned in every subsequent call
            if isinstance(checksum_value, bytes):
                checksum_value = checksum_value.decode("utf-8")
        elif checksum_algorithm:
            # If the value is not provided, we compute it and only return it as part of this request
            checksum_value = compute_checksum(
                self.raw_body, algorithm=checksum_algorithm
            )
            if isinstance(checksum_value, bytes):
                checksum_value = checksum_value.decode("utf-8")
        return checksum_algorithm, checksum_value

    def copy_object(self) -> ActionResult:
        self._validate_object_lock_headers()
        # you can have a quoted ?version=abc with a version Id, so work on
        # we need to parse the unquoted string first
        copy_source_parsed = urlparse(self._get_param("CopySource"))
        src_bucket, src_key = unquote(copy_source_parsed.path).lstrip("/").split("/", 1)
        src_version_id = parse_qs(copy_source_parsed.query).get("versionId", [None])[0]

        key_to_copy = self.backend.get_object(
            src_bucket, src_key, version_id=src_version_id
        )
        key_name = self._get_param("Key")

        bucket = self.backend.get_bucket(self.bucket_name)
        lock_enabled = bucket.object_lock_enabled

        encryption = self._get_param("ServerSideEncryption")
        kms_key_id = self._get_param("SSEKMSKeyId")
        bucket_key_enabled = self._get_param("BucketKeyEnabled")

        legal_hold, lock_mode, lock_until = self._get_lock_details(bucket, lock_enabled)

        if key_to_copy is not None:
            if requested_etag := self._get_param("CopySourceIfNoneMatch"):
                if requested_etag in [key_to_copy.etag, key_to_copy.etag[1:-1]]:
                    raise PreconditionFailed(
                        failed_condition="x-amz-copy-source-If-None-Match"
                    )

            if key_to_copy.storage_class in ARCHIVE_STORAGE_CLASSES:
                if key_to_copy.restore_status is None or (
                    'ongoing-request="true"' in key_to_copy.restore_status
                ):
                    raise ObjectNotInActiveTierError(key_to_copy)

            website_redirect_location = self._get_param("WebsiteRedirectLocation")

            mdirective = self._get_param("MetadataDirective")
            metadata = metadata_from_headers(self.headers)
            self.backend.copy_object(
                key_to_copy,
                self.bucket_name,
                key_name,
                storage=self._get_param("StorageClass"),
                kms_key_id=kms_key_id,
                encryption=encryption,
                bucket_key_enabled=bucket_key_enabled,
                mdirective=mdirective,
                metadata=metadata,
                website_redirect_location=website_redirect_location,
                lock_mode=lock_mode,
                lock_legal_status=legal_hold,
                lock_until=lock_until,
                provided_version_id=src_version_id,
            )
        else:
            if src_version_id:
                raise MissingVersion()
            raise MissingKey(key=src_key)

        new_key: FakeKey = self.backend.get_object(self.bucket_name, key_name)  # type: ignore

        acl = self._acl_from_headers()
        if acl is None:
            acl = bucket.acl
        if acl is not None:
            new_key.set_acl(acl)

        self.response_headers.update(self._get_cors_headers_other())
        if self._get_param("TaggingDirective") == "REPLACE":
            tagging = self._tagging_from_headers()
            self.backend.put_object_tagging(new_key, tagging)

        # checksum stuff, do we need to compute hash of the copied object
        checksum_algorithm = self._get_param("ChecksumAlgorithm")
        if checksum_algorithm:
            checksum_value = compute_checksum(
                new_key.value, algorithm=checksum_algorithm
            ).decode("utf-8")
            # By default, the checksum-details for the copy will be the same as the original
            # But if another algorithm is provided during the copy-operation, we override the values
            new_key.checksum_algorithm = checksum_algorithm
            new_key.checksum_value = checksum_value

        copy_result: dict[str, Any] = {
            "ETag": new_key.etag,
            "LastModified": new_key.last_modified_ISO8601,
        }
        if new_key.checksum_value:
            copy_result[f"Checksum{new_key.checksum_algorithm}"] = (
                new_key.checksum_value
            )

        result: dict[str, Any] = {
            "CopyObjectResult": copy_result,
            "CopySourceVersionId": key_to_copy.version_id
            if key_to_copy.version_id != "null"
            else None,
            "VersionId": new_key.version_id if new_key.is_versioned else None,
            "ServerSideEncryption": new_key.encryption,
            "SSEKMSKeyId": new_key.kms_key_id
            if new_key.encryption == "aws:kms"
            else None,
            "BucketKeyEnabled": str(new_key.bucket_key_enabled).lower() == "true"
            if new_key.encryption == "aws:kms"
            and new_key.bucket_key_enabled is not None
            else None,
        }
        return ActionResult(result)

    def _get_lock_details(
        self, bucket: "FakeBucket", lock_enabled: bool
    ) -> tuple[str | None, str | None, datetime.datetime | None]:
        lock_mode = self._get_param("ObjectLockMode")
        lock_until = self._get_param("ObjectLockRetainUntilDate")
        legal_hold = self._get_param("ObjectLockLegalHoldStatus")
        if lock_mode or lock_until or legal_hold == "ON":
            pass
        elif lock_enabled and bucket.has_default_lock:
            lock_until = bucket.default_retention()
            lock_mode = bucket.default_lock_mode
        return legal_hold, lock_mode, lock_until

    def put_object_tagging(self) -> ActionResult:
        self._validate_object_lock_headers()
        key_name = self._get_param("Key")
        version_id = self._get_param("VersionId")
        key_to_tag = self.backend.get_object(
            self.bucket_name, key_name, version_id=version_id, return_delete_marker=True
        )

        if isinstance(key_to_tag, FakeDeleteMarker):
            raise MethodNotAllowed(method="PUT", resource_type="DeleteMarker")
        elif key_to_tag is None and version_id is None:
            raise MissingKey(key=key_name)
        elif key_to_tag is None:
            raise MissingVersion(key=key_name, version_id=version_id)

        tagging = self._tagging_from_xml()
        self.backend.put_object_tagging(key=key_to_tag, tags=tagging)

        self.response_headers.update(self._get_cors_headers_other())
        return EmptyResult()

    def put_object_acl(self) -> ActionResult:
        self._validate_object_lock_headers()
        acl = self._acl_from_headers()
        if acl is None:
            bucket = self.backend.get_bucket(self.bucket_name)
            acl = bucket.acl
        key_name = self._get_param("Key")
        self.backend.put_object_acl(self.bucket_name, key_name, acl)

        self.response_headers.update(self._get_cors_headers_other())
        return EmptyResult()

    def put_object_legal_hold(self) -> ActionResult:
        self._validate_object_lock_headers()
        version_id = self._get_param("VersionId")
        bucket = self.backend.get_bucket(self.bucket_name)
        lock_enabled = bucket.object_lock_enabled
        key_name = self._get_param("Key")

        if not lock_enabled:
            raise LockNotEnabled
        legal_hold_status = self._get_param("LegalHold.Status")
        self.backend.put_object_legal_hold(
            self.bucket_name, key_name, version_id, legal_hold_status
        )

        self.response_headers.update(self._get_cors_headers_other())
        return EmptyResult()

    def put_object_retention(self) -> ActionResult:
        self._validate_object_lock_headers()
        version_id = self._get_param("VersionId")
        bucket = self.backend.get_bucket(self.bucket_name)
        lock_enabled = bucket.object_lock_enabled
        key_name = self._get_param("Key")

        if not lock_enabled:
            raise LockNotEnabled
        retention = (
            self._get_param("Retention.Mode"),
            self._get_param("Retention.RetainUntilDate"),
        )
        self.backend.put_object_retention(
            self.bucket_name, key_name, version_id=version_id, retention=retention
        )

        self.response_headers.update(self._get_cors_headers_other())
        return EmptyResult()

    def upload_part(self) -> ActionResult:
        upload_id = self._get_param("UploadId")
        part_number = self._get_param("PartNumber")

        if part_number > 10000:
            raise InvalidMaxPartNumberArgument(part_number)
        key = self.backend.upload_part(
            self.bucket_name, upload_id, part_number, self._get_param("Body", b"")
        )

        self.response_headers.update(self._get_cors_headers_other())
        checksum_algorithm, checksum_value = self._get_checksum()
        result: dict[str, Any] = {
            "ETag": key.etag,
            "ServerSideEncryption": key.encryption,
            "SSEKMSKeyId": key.kms_key_id if key.encryption == "aws:kms" else None,
        }
        if checksum_algorithm:
            result[f"Checksum{checksum_algorithm.upper()}"] = checksum_value
        return ActionResult(result)

    def upload_part_copy(self) -> ActionResult:
        self.response_headers.update(self._get_cors_headers_other())

        upload_id = self._get_param("UploadId")
        part_number = self._get_param("PartNumber")

        copy_source_parsed = urlparse(self._get_param("CopySource"))
        src_bucket, src_key = unquote(copy_source_parsed.path).lstrip("/").split("/", 1)
        src_version_id = parse_qs(copy_source_parsed.query).get("versionId", [None])[0]
        src_range = self._get_param("CopySourceRange", "").split("bytes=")[-1]
        try:
            start_byte, end_byte = src_range.split("-")
            start_byte, end_byte = int(start_byte), int(end_byte)
        except ValueError:
            start_byte, end_byte = None, None
        if not self.backend.get_object(src_bucket, src_key, version_id=src_version_id):
            if src_version_id:
                raise MissingVersion()
            raise MissingKey(key=src_key)
        key = self.backend.upload_part_copy(
            self.bucket_name,
            upload_id,
            part_number,
            src_bucket_name=src_bucket,
            src_key_name=src_key,
            src_version_id=src_version_id,
            start_byte=start_byte,
            end_byte=end_byte,
        )
        result = {
            "CopyPartResult": {
                "LastModified": key.last_modified_ISO8601,
                "ETag": key.etag,
            },
            "ServerSideEncryption": key.encryption,
            "SSEKMSKeyId": key.kms_key_id if key.encryption == "aws:kms" else None,
        }
        return ActionResult(result)

    def head_object(self) -> ActionResult:
        bucket_name = self.bucket_name
        key_name = self._get_param("Key")
        version_id = self._get_param("VersionId")
        if version_id and not self.backend.get_bucket(bucket_name).is_versioned:
            raise InvalidArgumentError(
                "Invalid version id specified", "versionId", version_id
            )

        part_number = self._get_param("PartNumber")

        checksum_mode = self._get_param("ChecksumMode") == "ENABLED"
        if_modified_since = self._get_param("IfModifiedSince")
        if_match = self._get_param("IfMatch")
        if_none_match = self._get_param("IfNoneMatch")
        if_unmodified_since = self._get_param("IfUnmodifiedSince")

        try:
            key = self.backend.head_object(
                bucket_name, key_name, version_id=version_id, part_number=part_number
            )
        except HeadOnDeleteMarker as exc:
            self.response_headers["x-amz-delete-marker"] = "true"
            if version_id:
                self.response_headers["x-amz-version-id"] = version_id
                self.response_headers["allow"] = "DELETE"
                raise MethodNotAllowed(method="HEAD", resource_type="DeleteMarker")
            self.response_headers["x-amz-version-id"] = exc.marker.version_id
            raise MissingKey(key=key_name)
        if not key:
            raise MissingKey(key=key_name)

        if if_unmodified_since:
            if key.last_modified.replace(microsecond=0) > if_unmodified_since:
                raise PreconditionFailed("If-Unmodified-Since")
        if if_match and key.etag != if_match:
            raise PreconditionFailed("If-Match")

        not_modified = False
        if if_modified_since:
            if key.last_modified.replace(microsecond=0) <= if_modified_since:
                not_modified = True
        if if_none_match and key.etag == if_none_match:
            not_modified = True
        if not_modified:
            self.response_headers["ETag"] = key.etag
            self.response_headers["Last-Modified"] = key.last_modified_RFC1123
            if key.is_versioned:
                self.response_headers["x-amz-version-id"] = key.version_id
            raise NotModified()

        metadata = {name.lower(): value for name, value in key.metadata.items()}
        result: dict[str, Any] = {
            "AcceptRanges": "bytes",
            "Restore": key.restore_status,
            "LastModified": key.last_modified,
            "ContentLength": key.size,
            "ETag": key.etag,
            "VersionId": key.version_id if key.is_versioned else None,
            "CacheControl": metadata.get("cache-control"),
            "ContentDisposition": metadata.get("content-disposition"),
            "ContentEncoding": metadata.get("content-encoding"),
            "ContentLanguage": metadata.get("content-language"),
            "ContentType": metadata.get("content-type"),
            "Expires": metadata.get("expires"),
            "WebsiteRedirectLocation": key.website_redirect_location
            or metadata.get("x-amz-website-redirect-location"),
            "ServerSideEncryption": key.encryption,
            "Metadata": {
                name[len("x-amz-meta-") :]: value
                for name, value in metadata.items()
                if name.startswith("x-amz-meta-")
            },
            "SSEKMSKeyId": key.kms_key_id if key.encryption == "aws:kms" else None,
            "BucketKeyEnabled": str(key.bucket_key_enabled).lower() == "true"
            if key.encryption == "aws:kms" and key.bucket_key_enabled is not None
            else None,
            "StorageClass": key.storage_class
            if key.storage_class != "STANDARD"
            else None,
            "TagCount": len(self.backend.get_object_tagging(key)["Tags"]) or None,
            "ObjectLockMode": key.lock_mode,
            "ObjectLockRetainUntilDate": key.lock_until,
            "ObjectLockLegalHoldStatus": key.lock_legal_status,
        }

        if checksum_mode and key.checksum_algorithm:
            result[f"Checksum{key.checksum_algorithm.upper()}"] = key.checksum_value

        if part_number:
            full_key: FakeKey = self.backend.head_object(  # type: ignore[assignment]
                bucket_name, key_name, version_id
            )
            if full_key.multipart:
                result["PartsCount"] = len(full_key.multipart.partlist)
            else:
                if part_number > 1:
                    raise RangeNotSatisfiable
                result["ContentRange"] = f"bytes 0-{full_key.size - 1}/{full_key.size}"
                self.response_headers["status"] = "206"
                return ActionResult(result)

        byte_range = self._get_range(key.size)
        if byte_range:
            begin, end = byte_range
            if begin or end < key.size - 1:
                # range requests do not return the checksum
                for member_name in list(result):
                    if member_name.startswith("Checksum"):
                        result.pop(member_name)
            result["ContentLength"] = (end - begin) + 1
            result["ContentRange"] = f"bytes {begin}-{end}/{key.size}"
            self.response_headers["status"] = "206"
        return ActionResult(result)

    def _acl_from_body(self) -> FakeAcl | None:
        parsed_xml = xmltodict.parse(self.body)
        if not parsed_xml.get("AccessControlPolicy"):
            raise MalformedACLError()

        # The owner is needed for some reason...
        if not parsed_xml["AccessControlPolicy"].get("Owner"):
            # TODO: Validate that the Owner is actually correct.
            raise MalformedACLError()

        # If empty, then no ACLs:
        if parsed_xml["AccessControlPolicy"].get("AccessControlList") is None:
            return None

        if not parsed_xml["AccessControlPolicy"]["AccessControlList"].get("Grant"):
            raise MalformedACLError()

        permissions = ["READ", "WRITE", "READ_ACP", "WRITE_ACP", "FULL_CONTROL"]

        if not isinstance(
            parsed_xml["AccessControlPolicy"]["AccessControlList"]["Grant"], list
        ):
            parsed_xml["AccessControlPolicy"]["AccessControlList"]["Grant"] = [
                parsed_xml["AccessControlPolicy"]["AccessControlList"]["Grant"]
            ]

        grants = self._get_grants_from_xml(
            parsed_xml["AccessControlPolicy"]["AccessControlList"]["Grant"],
            MalformedACLError,
            permissions,
        )
        return FakeAcl(grants)

    def _get_grants_from_xml(
        self,
        grant_list: list[dict[str, Any]],
        exception_type: type[S3ClientError],
        permissions: list[str],
    ) -> list[FakeGrant]:
        grants = []
        for grant in grant_list:
            if grant.get("Permission", "") not in permissions:
                raise exception_type()

            if grant["Grantee"].get("@xsi:type", "") not in [
                "CanonicalUser",
                "AmazonCustomerByEmail",
                "Group",
            ]:
                raise exception_type()

            # TODO: Verify that the proper grantee data is supplied based on the type.

            grants.append(
                FakeGrant(
                    [
                        FakeGrantee(
                            grantee_id=grant["Grantee"].get("ID", ""),
                            display_name=grant["Grantee"].get("DisplayName", ""),
                            uri=grant["Grantee"].get("URI", ""),
                        )
                    ],
                    [grant["Permission"]],
                )
            )

        return grants

    def _acl_from_headers(self) -> FakeAcl | None:
        canned_acl = self._get_param("ACL")

        grants = []
        for member_name, permission in [
            ("GrantRead", "READ"),
            ("GrantWrite", "WRITE"),
            ("GrantReadACP", "READ_ACP"),
            ("GrantWriteACP", "WRITE_ACP"),
            ("GrantFullControl", "FULL_CONTROL"),
        ]:
            value = self._get_param(member_name)
            if not value:
                continue

            grantees = []
            for key_and_value in value.split(","):
                key, value = re.match(  # type: ignore
                    '([^=]+)="?([^"]+)"?', key_and_value.strip()
                ).groups()
                if key.lower() == "id":
                    grantees.append(FakeGrantee(grantee_id=value))
                else:
                    grantees.append(FakeGrantee(uri=value))
            grants.append(FakeGrant(grantees, [permission]))

        if canned_acl and grants:
            raise S3AclAndGrantError()
        if canned_acl:
            return get_canned_acl(canned_acl)
        if grants:
            return FakeAcl(grants)
        else:
            return None

    def _tagging_from_headers(self) -> dict[str, str]:
        tags = {}
        if tagging := self._get_param("Tagging"):
            parsed_header = parse_qs(tagging, keep_blank_values=True)
            for tag in parsed_header.items():
                tags[tag[0]] = tag[1][0]
        return tags

    def _tagging_from_xml(self) -> dict[str, str]:
        parsed_xml = xmltodict.parse(self.body, force_list={"Tag": True})

        tags = {}
        for tag in parsed_xml["Tagging"]["TagSet"]["Tag"]:
            tags[tag["Key"]] = tag["Value"] or ""

        return tags

    def _bucket_tagging_from_body(self) -> dict[str, str]:
        parsed_xml = xmltodict.parse(self.body)

        tags = {}
        # Optional if no tags are being sent:
        if parsed_xml["Tagging"].get("TagSet"):
            # If there is only 1 tag, then it's not a list:
            if not isinstance(parsed_xml["Tagging"]["TagSet"]["Tag"], list):
                tags[parsed_xml["Tagging"]["TagSet"]["Tag"]["Key"]] = parsed_xml[
                    "Tagging"
                ]["TagSet"]["Tag"]["Value"]
            else:
                for tag in parsed_xml["Tagging"]["TagSet"]["Tag"]:
                    if tag["Key"] in tags:
                        raise DuplicateTagKeys()
                    tags[tag["Key"]] = tag["Value"]

        # Verify that "aws:" is not in the tags. If so, then this is a problem:
        for key, _ in tags.items():
            if key.startswith("aws:"):
                raise NoSystemTags()

        return tags

    def delete_object(self) -> ActionResult:
        bypass = self._get_param("BypassGovernanceRetention", False)
        key_name = self._get_param("Key")
        version_id = self._get_param("VersionId")
        if_match = self._get_param("IfMatch")

        if if_match:
            if not (obj := self.backend.get_object(self.bucket_name, key_name)):
                raise MissingKey
            # Check if the ETags are the same. S3 doesn't seem to care about quotes, so we shouldn't either
            elif if_match.replace('"', "") != obj.etag.replace('"', ""):
                raise PreconditionFailed("If-Match")

        _, response_meta = self.backend.delete_object(
            self.bucket_name, key_name, version_id=version_id, bypass=bypass
        )
        result = {
            "DeleteMarker": response_meta.get("delete-marker") == "true" or None,
            "VersionId": response_meta.get("version-id"),
        }
        return ActionResult(result)

    def delete_object_tagging(self) -> ActionResult:
        key_name = self._get_param("Key")
        version_id = self._get_param("VersionId")
        self.backend.delete_object_tagging(
            self.bucket_name, key_name, version_id=version_id
        )
        return ActionResult({"VersionId": version_id})

    def _complete_multipart_body(self, body: bytes) -> Iterator[tuple[int, str]]:
        ps = minidom.parseString(body).getElementsByTagName("Part")
        prev = 0
        for p in ps:
            pn = int(p.getElementsByTagName("PartNumber")[0].firstChild.wholeText)  # type: ignore[union-attr]
            if pn <= prev:
                raise InvalidPartOrder()
            yield pn, p.getElementsByTagName("ETag")[0].firstChild.wholeText  # type: ignore[union-attr]

    def create_multipart_upload(self) -> ActionResult:
        key_name = self._get_param("Key")
        encryption = self._get_param("ServerSideEncryption")
        kms_key_id = self._get_param("SSEKMSKeyId")
        metadata = metadata_from_headers(self.headers)
        tagging = self._tagging_from_headers()
        storage_type = self._get_param("StorageClass", "STANDARD")
        acl = self._acl_from_headers()

        multipart = self.backend.create_multipart_upload(
            self.bucket_name,
            key_name,
            metadata,
            storage_type,
            tagging,
            acl,
            encryption,
            kms_key_id,
        )
        result = {
            "Bucket": self.bucket_name,
            "Key": key_name,
            "UploadId": multipart.id,
            "ServerSideEncryption": encryption,
            "SSEKMSKeyId": kms_key_id,
            "ChecksumAlgorithm": multipart.metadata.get("x-amz-checksum-algorithm"),
            "ChecksumType": multipart.metadata.get("x-amz-checksum-type"),
        }
        return ActionResult(result)

    def complete_multipart_upload(self) -> ActionResult:
        key_name = self._get_param("Key")
        existing = self.backend.get_object(self.bucket_name, key_name)

        multipart_id = self._get_param("UploadId")

        if (
            existing is not None
            and existing.multipart
            and existing.multipart.id == multipart_id
        ):
            # Operation is idempotent
            key: FakeKey | None = existing
        else:
            if self._get_param("IfNoneMatch") == "*" and existing is not None:
                raise PreconditionFailed("If-None-Match")

            key = self.backend.complete_multipart_upload(
                self.bucket_name, multipart_id, self._complete_multipart_body(self.body)
            )
        if key is None:
            raise MalformedXML()

        result = {
            "Location": f"http://{self.bucket_name}.s3.amazonaws.com/{key.name}",
            "Bucket": self.bucket_name,
            "Key": key.name,
            "ETag": key.etag,
            "VersionId": key.version_id,
            "ServerSideEncryption": key.encryption,
            "SSEKMSKeyId": key.kms_key_id,
        }
        return ActionResult(result)

    def restore_object(self) -> ActionResult:
        key_name = self._get_param("Key")
        params = xmltodict.parse(self.body)["RestoreRequest"]
        previously_restored = self.backend.restore_object(
            self.bucket_name,
            key_name,
            params.get("Days", None),
            params.get("Type", None),
        )
        if not previously_restored:
            self.response_headers["status"] = "202"
        return EmptyResult()

    def select_object_content(self) -> ActionResult:
        key_name = self._get_param("Key")
        request = xmltodict.parse(self.body)["SelectObjectContentRequest"]
        select_query = request["Expression"]
        input_details = request["InputSerialization"]
        output_details = request["OutputSerialization"]
        results, bytes_scanned = self.backend.select_object_content(
            self.bucket_name, key_name, select_query, input_details, output_details
        )
        bytes_returned = sum(len(line) for line in results)
        event_stream = [
            {"Records": {"Payload": b"".join(results)}},
            {
                "Stats": {
                    "Details": {
                        "BytesScanned": bytes_scanned,
                        "BytesProcessed": bytes_scanned,
                        "BytesReturned": bytes_returned,
                    }
                }
            },
            {"End": {}},
        ]
        return ActionResult({"Payload": event_stream})

    def _invalid_headers(self, url: str, headers: dict[str, str]) -> bool:
        """
        Verify whether the provided metadata in the URL is also present in the headers
        :param url: .../file.txt&content-type=app%2Fjson&Signature=..
        :param headers: Content-Type=app/json
        :return: True or False
        """
        metadata_to_check = {
            "content-disposition": "Content-Disposition",
            "content-encoding": "Content-Encoding",
            "content-language": "Content-Language",
            "content-length": "Content-Length",
            "content-md5": "Content-MD5",
            "content-type": "Content-Type",
        }
        for url_key, header_key in metadata_to_check.items():
            metadata_in_url = re.search(url_key + "=(.+?)(&.+$|$)", url)
            if metadata_in_url:
                url_value = unquote(metadata_in_url.group(1))
                if header_key not in headers or (url_value != headers[header_key]):
                    return True
        return False


S3ResponseInstance = S3Response()
