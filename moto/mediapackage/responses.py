from moto.core.responses import ActionResult, BaseResponse

from .models import MediaPackageBackend, mediapackage_backends


class MediaPackageResponse(BaseResponse):
    def __init__(self) -> None:
        super().__init__(service_name="mediapackage")
        self.automated_parameter_parsing = True

    @property
    def mediapackage_backend(self) -> MediaPackageBackend:
        return mediapackage_backends[self.current_account][self.region]

    @staticmethod
    def _channel_result(channel):  # type: ignore[no-untyped-def]
        return {
            "Arn": channel.arn,
            "Id": channel.channel_id,
            "Description": channel.description,
            "Tags": channel.tags,
        }

    @staticmethod
    def _endpoint_result(endpoint):  # type: ignore[no-untyped-def]
        return {
            "Arn": endpoint.arn,
            "Authorization": endpoint.authorization,
            "ChannelId": endpoint.channel_id,
            "CmafPackage": endpoint.cmaf_package,
            "DashPackage": endpoint.dash_package,
            "Description": endpoint.description,
            "HlsPackage": endpoint.hls_package,
            "Id": endpoint.id,
            "ManifestName": endpoint.manifest_name,
            "MssPackage": endpoint.mss_package,
            "Origination": endpoint.origination,
            "StartoverWindowSeconds": endpoint.startover_window_seconds,
            "Tags": endpoint.tags,
            "TimeDelaySeconds": endpoint.time_delay_seconds,
            "Url": endpoint.url,
            "Whitelist": endpoint.whitelist,
        }

    def create_channel(self) -> ActionResult:
        description = self._get_param("Description")
        channel_id = self._get_param("Id")
        tags = self._get_param("Tags")
        channel = self.mediapackage_backend.create_channel(
            description=description, channel_id=channel_id, tags=tags
        )
        return ActionResult(self._channel_result(channel))

    def list_channels(self) -> ActionResult:
        channels = self.mediapackage_backend.list_channels()
        return ActionResult({"Channels": channels})

    def describe_channel(self) -> ActionResult:
        channel_id = self._get_param("Id")
        channel = self.mediapackage_backend.describe_channel(channel_id=channel_id)
        return ActionResult(self._channel_result(channel))

    def delete_channel(self) -> ActionResult:
        channel_id = self._get_param("Id")
        self.mediapackage_backend.delete_channel(channel_id=channel_id)
        return ActionResult({})

    def create_origin_endpoint(self) -> ActionResult:
        authorization = self._get_param("Authorization")
        channel_id = self._get_param("ChannelId")
        cmaf_package = self._get_param("CmafPackage")
        dash_package = self._get_param("DashPackage")
        description = self._get_param("Description")
        hls_package = self._get_param("HlsPackage")
        endpoint_id = self._get_param("Id")
        manifest_name = self._get_param("ManifestName")
        mss_package = self._get_param("MssPackage")
        origination = self._get_param("Origination")
        startover_window_seconds = self._get_int_param("StartoverWindowSeconds")
        tags = self._get_param("Tags")
        time_delay_seconds = self._get_int_param("TimeDelaySeconds")
        whitelist = self._get_param("Whitelist")
        origin_endpoint = self.mediapackage_backend.create_origin_endpoint(
            authorization=authorization,
            channel_id=channel_id,
            cmaf_package=cmaf_package,
            dash_package=dash_package,
            description=description,
            hls_package=hls_package,
            endpoint_id=endpoint_id,
            manifest_name=manifest_name,
            mss_package=mss_package,
            origination=origination,
            startover_window_seconds=startover_window_seconds,
            tags=tags,
            time_delay_seconds=time_delay_seconds,
            whitelist=whitelist,  # type: ignore[arg-type]
        )
        return ActionResult(self._endpoint_result(origin_endpoint))

    def list_origin_endpoints(self) -> ActionResult:
        origin_endpoints = self.mediapackage_backend.list_origin_endpoints()
        return ActionResult({"OriginEndpoints": origin_endpoints})

    def describe_origin_endpoint(self) -> ActionResult:
        endpoint_id = self._get_param("Id")
        endpoint = self.mediapackage_backend.describe_origin_endpoint(
            endpoint_id=endpoint_id
        )
        return ActionResult(self._endpoint_result(endpoint))

    def delete_origin_endpoint(self) -> ActionResult:
        endpoint_id = self._get_param("Id")
        self.mediapackage_backend.delete_origin_endpoint(endpoint_id=endpoint_id)
        return ActionResult({})

    def update_origin_endpoint(self) -> ActionResult:
        authorization = self._get_param("Authorization")
        cmaf_package = self._get_param("CmafPackage")
        dash_package = self._get_param("DashPackage")
        description = self._get_param("Description")
        hls_package = self._get_param("HlsPackage")
        endpoint_id = self._get_param("Id")
        manifest_name = self._get_param("ManifestName")
        mss_package = self._get_param("MssPackage")
        origination = self._get_param("Origination")
        startover_window_seconds = self._get_int_param("StartoverWindowSeconds")
        time_delay_seconds = self._get_int_param("TimeDelaySeconds")
        whitelist = self._get_param("Whitelist")
        origin_endpoint = self.mediapackage_backend.update_origin_endpoint(
            authorization=authorization,
            cmaf_package=cmaf_package,
            dash_package=dash_package,
            description=description,
            hls_package=hls_package,
            endpoint_id=endpoint_id,
            manifest_name=manifest_name,
            mss_package=mss_package,
            origination=origination,
            startover_window_seconds=startover_window_seconds,
            time_delay_seconds=time_delay_seconds,
            whitelist=whitelist,  # type: ignore[arg-type]
        )
        return ActionResult(self._endpoint_result(origin_endpoint))
