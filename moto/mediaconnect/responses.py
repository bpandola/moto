import json
from typing import Any

from moto.core.responses import BaseResponse

from .models import MediaConnectBackend, mediaconnect_backends


def _to_camel_case(value: Any) -> Any:
    """Recursively convert PascalCase dict keys to camelCase.

    The automated parameter parser produces PascalCase keys (botocore member
    names), but the models layer expects camelCase keys (the JSON wire format).
    """
    if isinstance(value, dict):
        return {
            (k[0].lower() + k[1:] if k else k): _to_camel_case(v)
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [_to_camel_case(item) for item in value]
    return value


class MediaConnectResponse(BaseResponse):
    def __init__(self) -> None:
        super().__init__(service_name="mediaconnect")
        self.automated_parameter_parsing = True

    @property
    def mediaconnect_backend(self) -> MediaConnectBackend:
        return mediaconnect_backends[self.current_account][self.region]

    def create_flow(self) -> str:
        availability_zone = self._get_param("AvailabilityZone")
        entitlements = _to_camel_case(self._get_param("Entitlements", []))
        name = self._get_param("Name")
        outputs = _to_camel_case(self._get_param("Outputs"))
        source = _to_camel_case(self._get_param("Source"))
        source_failover_config = _to_camel_case(self._get_param("SourceFailoverConfig"))
        sources = _to_camel_case(self._get_param("Sources"))
        vpc_interfaces = _to_camel_case(self._get_param("VpcInterfaces"))
        maintenance = _to_camel_case(self._get_param("Maintenance"))
        flow = self.mediaconnect_backend.create_flow(
            availability_zone=availability_zone,
            entitlements=entitlements,
            name=name,
            outputs=outputs,
            source=source,
            source_failover_config=source_failover_config,
            sources=sources,
            vpc_interfaces=vpc_interfaces,
            maintenance=maintenance,
        )
        return json.dumps({"flow": flow.to_dict()})

    def list_flows(self) -> str:
        max_results = self._get_int_param("MaxResults")
        flows = self.mediaconnect_backend.list_flows(max_results=max_results)
        return json.dumps({"flows": flows})

    def describe_flow(self) -> str:
        flow_arn = self._get_param("FlowArn")
        flow = self.mediaconnect_backend.describe_flow(flow_arn=flow_arn)
        return json.dumps({"flow": flow.to_dict()})

    def delete_flow(self) -> str:
        flow_arn = self._get_param("FlowArn")
        flow = self.mediaconnect_backend.delete_flow(flow_arn=flow_arn)
        return json.dumps({"flowArn": flow.flow_arn, "status": flow.status})

    def start_flow(self) -> str:
        flow_arn = self._get_param("FlowArn")
        flow = self.mediaconnect_backend.start_flow(flow_arn=flow_arn)
        return json.dumps({"flowArn": flow.flow_arn, "status": flow.status})

    def stop_flow(self) -> str:
        flow_arn = self._get_param("FlowArn")
        flow = self.mediaconnect_backend.stop_flow(flow_arn=flow_arn)
        return json.dumps({"flowArn": flow.flow_arn, "status": flow.status})

    def tag_resource(self) -> str:
        resource_arn = self._get_param("ResourceArn")
        tags = self._get_param("Tags")
        self.mediaconnect_backend.tag_resource(resource_arn=resource_arn, tags=tags)
        return json.dumps({})

    def list_tags_for_resource(self) -> str:
        resource_arn = self._get_param("ResourceArn")
        tags = self.mediaconnect_backend.list_tags_for_resource(resource_arn)
        return json.dumps({"tags": tags})

    def add_flow_vpc_interfaces(self) -> str:
        flow_arn = self._get_param("FlowArn")
        vpc_interfaces = _to_camel_case(self._get_param("VpcInterfaces"))
        flow = self.mediaconnect_backend.add_flow_vpc_interfaces(
            flow_arn=flow_arn, vpc_interfaces=vpc_interfaces
        )
        return json.dumps(
            {"flow_arn": flow.flow_arn, "vpc_interfaces": flow.vpc_interfaces}
        )

    def remove_flow_vpc_interface(self) -> str:
        flow_arn = self._get_param("FlowArn")
        vpc_interface_name = self._get_param("VpcInterfaceName", "")
        self.mediaconnect_backend.remove_flow_vpc_interface(
            flow_arn=flow_arn, vpc_interface_name=vpc_interface_name
        )
        return json.dumps(
            {"flow_arn": flow_arn, "vpc_interface_name": vpc_interface_name}
        )

    def add_flow_outputs(self) -> str:
        flow_arn = self._get_param("FlowArn")
        outputs = _to_camel_case(self._get_param("Outputs"))
        flow = self.mediaconnect_backend.add_flow_outputs(
            flow_arn=flow_arn, outputs=outputs
        )
        return json.dumps({"flow_arn": flow.flow_arn, "outputs": flow.outputs})

    def remove_flow_output(self) -> str:
        flow_arn = self._get_param("FlowArn")
        output_name = self._get_param("OutputArn", "")
        self.mediaconnect_backend.remove_flow_output(
            flow_arn=flow_arn, output_name=output_name
        )
        return json.dumps({"flow_arn": flow_arn, "output_name": output_name})

    def update_flow_output(self) -> str:
        flow_arn = self._get_param("FlowArn")
        output_arn = self._get_param("OutputArn", "")
        cidr_allow_list = self._get_param("CidrAllowList")
        description = self._get_param("Description")
        destination = self._get_param("Destination")
        encryption = _to_camel_case(self._get_param("Encryption"))
        max_latency = self._get_param("MaxLatency")
        media_stream_output_configuration = _to_camel_case(
            self._get_param("MediaStreamOutputConfigurations")
        )
        min_latency = self._get_param("MinLatency")
        port = self._get_param("Port")
        protocol = self._get_param("Protocol")
        remote_id = self._get_param("RemoteId")
        sender_control_port = self._get_param("SenderControlPort")
        sender_ip_address = self._get_param("SenderIpAddress")
        smoothing_latency = self._get_param("SmoothingLatency")
        stream_id = self._get_param("StreamId")
        vpc_interface_attachment = _to_camel_case(
            self._get_param("VpcInterfaceAttachment")
        )
        output = self.mediaconnect_backend.update_flow_output(
            flow_arn=flow_arn,
            output_arn=output_arn,
            cidr_allow_list=cidr_allow_list,
            description=description,
            destination=destination,
            encryption=encryption,
            max_latency=max_latency,
            media_stream_output_configuration=media_stream_output_configuration,
            min_latency=min_latency,
            port=port,
            protocol=protocol,
            remote_id=remote_id,
            sender_control_port=sender_control_port,
            sender_ip_address=sender_ip_address,
            smoothing_latency=smoothing_latency,
            stream_id=stream_id,
            vpc_interface_attachment=vpc_interface_attachment,
        )
        return json.dumps({"flowArn": flow_arn, "output": output})

    def add_flow_sources(self) -> str:
        flow_arn = self._get_param("FlowArn")
        sources = _to_camel_case(self._get_param("Sources"))
        sources = self.mediaconnect_backend.add_flow_sources(
            flow_arn=flow_arn, sources=sources
        )
        return json.dumps({"flow_arn": flow_arn, "sources": sources})

    def update_flow_source(self) -> str:
        flow_arn = self._get_param("FlowArn")
        source_arn = self._get_param("SourceArn", "")
        description = self._get_param("Description")
        decryption = _to_camel_case(self._get_param("Decryption"))
        entitlement_arn = self._get_param("EntitlementArn", "")
        ingest_port = self._get_param("IngestPort")
        max_bitrate = self._get_param("MaxBitrate")
        max_latency = self._get_param("MaxLatency")
        max_sync_buffer = self._get_param("MaxSyncBuffer")
        media_stream_source_configurations = _to_camel_case(
            self._get_param("MediaStreamSourceConfigurations")
        )
        min_latency = self._get_param("MinLatency")
        protocol = self._get_param("Protocol")
        sender_control_port = self._get_param("SenderControlPort")
        sender_ip_address = self._get_param("SenderIpAddress")
        stream_id = self._get_param("StreamId")
        vpc_interface_name = self._get_param("VpcInterfaceName", "")
        whitelist_cidr = self._get_param("WhitelistCidr")
        source = self.mediaconnect_backend.update_flow_source(
            flow_arn=flow_arn,
            source_arn=source_arn,
            decryption=decryption,
            description=description,
            entitlement_arn=entitlement_arn,
            ingest_port=ingest_port,
            max_bitrate=max_bitrate,
            max_latency=max_latency,
            max_sync_buffer=max_sync_buffer,
            media_stream_source_configurations=media_stream_source_configurations,
            min_latency=min_latency,
            protocol=protocol,
            sender_control_port=sender_control_port,
            sender_ip_address=sender_ip_address,
            stream_id=stream_id,
            vpc_interface_name=vpc_interface_name,
            whitelist_cidr=whitelist_cidr,
        )
        return json.dumps({"flow_arn": flow_arn, "source": source})

    def grant_flow_entitlements(self) -> str:
        flow_arn = self._get_param("FlowArn")
        entitlements = _to_camel_case(self._get_param("Entitlements"))
        entitlements = self.mediaconnect_backend.grant_flow_entitlements(
            flow_arn=flow_arn, entitlements=entitlements
        )
        return json.dumps({"flow_arn": flow_arn, "entitlements": entitlements})

    def revoke_flow_entitlement(self) -> str:
        flow_arn = self._get_param("FlowArn")
        entitlement_arn = self._get_param("EntitlementArn", "")
        self.mediaconnect_backend.revoke_flow_entitlement(
            flow_arn=flow_arn, entitlement_arn=entitlement_arn
        )
        return json.dumps({"flowArn": flow_arn, "entitlementArn": entitlement_arn})

    def update_flow_entitlement(self) -> str:
        flow_arn = self._get_param("FlowArn")
        entitlement_arn = self._get_param("EntitlementArn", "")
        description = self._get_param("Description")
        encryption = _to_camel_case(self._get_param("Encryption"))
        entitlement_status = self._get_param("EntitlementStatus")
        name = self._get_param("Name")
        subscribers = self._get_param("Subscribers")
        entitlement = self.mediaconnect_backend.update_flow_entitlement(
            flow_arn=flow_arn,
            entitlement_arn=entitlement_arn,
            description=description,
            encryption=encryption,
            entitlement_status=entitlement_status,
            name=name,
            subscribers=subscribers,
        )
        return json.dumps({"flowArn": flow_arn, "entitlement": entitlement})
