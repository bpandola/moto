"""Handles Route53 API requests, invokes method and returns response."""

import re
from typing import Any
from urllib.parse import unquote

import xmltodict
from jinja2 import Template

from moto.core.common_types import TYPE_RESPONSE
from moto.core.responses import ActionResult, BaseResponse, EmptyResult
from moto.core.utils import utcnow
from moto.route53.exceptions import InvalidChangeBatch, InvalidInput
from moto.route53.models import Route53Backend, route53_backends

XMLNS = "https://route53.amazonaws.com/doc/2013-04-01/"


class Route53(BaseResponse):
    """Handler for Route53 requests and responses."""

    def __init__(self) -> None:
        super().__init__(service_name="route53")
        self.automated_parameter_parsing = True

    @property
    def backend(self) -> Route53Backend:
        return route53_backends[self.current_account][self.partition]

    def create_hosted_zone(self) -> ActionResult:
        vpcid = None
        vpcregion = None
        comment = self._get_param("HostedZoneConfig.Comment")
        private_zone = self._get_bool_param("HostedZoneConfig.PrivateZone", False)
        # It is possible to create a Private Hosted Zone without
        # associating VPC at the time of creation.
        if private_zone:
            vpcid = self._get_param("VPC.VPCId")
            vpcregion = self._get_param("VPC.VPCRegion")
        name = self._get_param("Name")
        caller_reference = self._get_param("CallerReference")
        if name[-1] != ".":
            name += "."
        delegation_set_id = self._get_param("DelegationSetId")
        new_zone = self.backend.create_hosted_zone(
            name,
            comment=comment,
            private_zone=private_zone,
            caller_reference=caller_reference,
            vpcid=vpcid,
            vpcregion=vpcregion,
            delegation_set_id=delegation_set_id,
        )
        result = {
            "HostedZone": new_zone,
            "ChangeInfo": {
                "Id": "/change/C1PA6795UKMFR9",
                "Status": "INSYNC",
                "SubmittedAt": utcnow(),
            },
            "DelegationSet": new_zone.delegation_set if not private_zone else None,
            "VPC": {
                "VPCRegion": new_zone.vpcs[0].get("vpc_region"),
                "VPCId": new_zone.vpcs[0].get("vpc_id"),
            }
            if new_zone.private_zone and new_zone.vpcs
            else None,
            "Location": f"https://route53.amazonaws.com/2013-04-01/hostedzone/{new_zone.resource_id}",
        }
        return ActionResult(result)

    def list_hosted_zones(self) -> ActionResult:
        max_items = self._get_int_param("MaxItems", 100)
        marker = self._get_param("Marker")
        zone_page, next_marker = self.backend.list_hosted_zones(
            marker=marker, max_size=max_items
        )
        result = {
            "HostedZones": zone_page,
            "Marker": marker,
            "IsTruncated": True if next_marker else False,
            "NextMarker": next_marker,
            "MaxItems": max_items,
        }
        return ActionResult(result)

    def list_hosted_zones_by_name(self) -> ActionResult:
        dnsname = self._get_param("DNSName")
        dnsname, zones = self.backend.list_hosted_zones_by_name(dnsname)
        result = {"DNSName": dnsname, "HostedZones": zones}
        return ActionResult(result)

    def list_hosted_zones_by_vpc(self) -> ActionResult:
        vpc_id = self._get_param("VPCId")
        zones = self.backend.list_hosted_zones_by_vpc(vpc_id)
        result = {"HostedZoneSummaries": zones}
        return ActionResult(result)

    def get_hosted_zone_count(self) -> ActionResult:
        num_zones = self.backend.get_hosted_zone_count()
        result = {"HostedZoneCount": num_zones}
        return ActionResult(result)

    def get_hosted_zone(self) -> ActionResult:
        zoneid = self._get_param("Id")
        the_zone = self.backend.get_hosted_zone(zoneid)
        result = {
            "HostedZone": the_zone,
            "DelegationSet": the_zone.delegation_set
            if not the_zone.private_zone
            else None,
            "VPCs": the_zone.vpcs if the_zone.private_zone else None,
        }
        return ActionResult(result)

    def delete_hosted_zone(self) -> ActionResult:
        zoneid = self._get_param("Id")
        self.backend.delete_hosted_zone(zoneid)
        result = {
            "ChangeInfo": {
                "Id": "/change/C2682N5HXP0BZ4",
                "Status": "INSYNC",
                "SubmittedAt": utcnow(),
            }
        }
        return ActionResult(result)

    def update_hosted_zone_comment(self) -> ActionResult:
        zoneid = self._get_param("Id")
        comment = self._get_param("Comment")
        zone = self.backend.update_hosted_zone_comment(zoneid, comment)
        result = {"HostedZone": zone}
        return ActionResult(result)

    def get_dnssec(self) -> ActionResult:
        # TODO: implement enable/disable dnssec apis
        zoneid = self._get_param("HostedZoneId")
        self.backend.get_dnssec(zoneid)
        result = {"Status": {"ServeSignature": "NOT_SIGNING"}}
        return ActionResult(result)

    def associate_vpc_with_hosted_zone(self) -> ActionResult:
        zoneid = self._get_param("HostedZoneId")
        comment = self._get_param("Comment")
        vpcid = self._get_param("VPC.VPCId")
        vpcregion = self._get_param("VPC.VPCRegion")
        self.backend.associate_vpc_with_hosted_zone(zoneid, vpcid, vpcregion)
        result = {
            "ChangeInfo": {
                "Id": "/change/a1b2c3d4",
                "Status": "INSYNC",
                "SubmittedAt": utcnow(),
                "Comment": comment,
            }
        }
        return ActionResult(result)

    def disassociate_vpc_from_hosted_zone(self) -> ActionResult:
        zoneid = self._get_param("HostedZoneId")
        comment = self._get_param("Comment")
        vpcid = self._get_param("VPC.VPCId")
        self.backend.disassociate_vpc_from_hosted_zone(zoneid, vpcid)
        result = {
            "ChangeInfo": {
                "Id": "/change/a1b2c3d4",
                "Status": "INSYNC",
                "SubmittedAt": utcnow(),
                "Comment": comment,
            }
        }
        return ActionResult(result)

    def change_resource_record_sets(self) -> ActionResult:
        zoneid = self._get_param("HostedZoneId")
        change_list = self._get_param("ChangeBatch.Changes")
        # Enforce quotas https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/DNSLimitations.html#limits-api-requests-changeresourcerecordsets
        #  - A request cannot contain more than 1,000 ResourceRecord elements. When the value of the Action element is UPSERT, each ResourceRecord element is counted twice.
        effective_rr_count = 0
        for value in change_list:
            record_set = value["ResourceRecordSet"]
            if "ResourceRecords" not in record_set or not record_set["ResourceRecords"]:
                continue
            resource_records = record_set["ResourceRecords"]
            effective_rr_count += len(resource_records)
            if value["Action"] == "UPSERT":
                effective_rr_count += len(resource_records)
        if effective_rr_count > 1000:
            raise InvalidChangeBatch
        self.backend.change_resource_record_sets(zoneid, change_list)
        result = {
            "ChangeInfo": {
                "Id": "/change/C2682N5HXP0BZ4",
                "Status": "INSYNC",
                "SubmittedAt": utcnow(),
            }
        }
        return ActionResult(result)

    def list_resource_record_sets(self) -> ActionResult:
        zoneid = self._get_param("HostedZoneId")
        start_type = self._get_param("StartRecordType")
        start_name = self._get_param("StartRecordName")
        max_items = self._get_int_param("MaxItems", 300)

        if start_type and not start_name:
            raise InvalidInput("The input is not valid")

        (
            record_sets,
            next_name,
            next_type,
            is_truncated,
        ) = self.backend.list_resource_record_sets(
            zoneid,
            start_type=start_type,  # type: ignore
            start_name=start_name,  # type: ignore
            max_items=max_items,
        )
        result = {
            "ResourceRecordSets": record_sets,
            "IsTruncated": is_truncated,
            "NextRecordName": next_name if is_truncated else None,
            "NextRecordType": next_type if is_truncated else None,
            "MaxItems": max_items,
        }
        return ActionResult(result)

    def create_health_check(self) -> ActionResult:
        caller_reference = self._get_param("CallerReference")
        config = self._get_param("HealthCheckConfig")
        health_check_args = {
            "ip_address": config.get("IPAddress"),
            "port": config.get("Port"),
            "type": config["Type"],
            "resource_path": config.get("ResourcePath"),
            "fqdn": config.get("FullyQualifiedDomainName"),
            "search_string": config.get("SearchString"),
            "request_interval": config.get("RequestInterval"),
            "failure_threshold": config.get("FailureThreshold"),
            "health_threshold": config.get("HealthThreshold"),
            "measure_latency": config.get("MeasureLatency"),
            "inverted": config.get("Inverted"),
            "disabled": config.get("Disabled"),
            "enable_sni": config.get("EnableSNI"),
            "children": config.get("ChildHealthChecks", []),
            "regions": config.get("Regions", []),
        }
        health_check = self.backend.create_health_check(
            caller_reference, health_check_args
        )
        result = {"HealthCheck": health_check}
        return ActionResult(result)

    def list_health_checks(self) -> ActionResult:
        health_checks = self.backend.list_health_checks()
        result = {"HealthChecks": health_checks}
        return ActionResult(result)

    def get_health_check(self) -> ActionResult:
        health_check_id = self._get_param("HealthCheckId")
        health_check = self.backend.get_health_check(health_check_id)
        result = {"HealthCheck": health_check}
        return ActionResult(result)

    def delete_health_check(self) -> ActionResult:
        health_check_id = self._get_param("HealthCheckId")
        self.backend.delete_health_check(health_check_id)
        return EmptyResult()

    def update_health_check(self) -> ActionResult:
        health_check_id = self._get_param("HealthCheckId")
        health_check_args = {
            "ip_address": self._get_param("IPAddress"),
            "port": self._get_param("Port"),
            "resource_path": self._get_param("ResourcePath"),
            "fqdn": self._get_param("FullyQualifiedDomainName"),
            "search_string": self._get_param("SearchString"),
            "failure_threshold": self._get_param("FailureThreshold"),
            "health_threshold": self._get_param("HealthThreshold"),
            "inverted": self._get_param("Inverted"),
            "disabled": self._get_param("Disabled"),
            "enable_sni": self._get_param("EnableSNI"),
            "children": self._get_param("ChildHealthChecks", []),
            "regions": self._get_param("Regions", []),
        }
        health_check = self.backend.update_health_check(
            health_check_id, health_check_args
        )
        result = {"HealthCheck": health_check}
        return ActionResult(result)

    def get_health_check_status(self) -> ActionResult:
        health_check_id = self._get_param("HealthCheckId")
        self.backend.get_health_check(health_check_id)
        result = {
            "HealthCheckObservations": [
                {
                    "Region": "us-east-1",
                    "IPAddress": "127.0.13.37",
                    "StatusReport": {
                        "Status": "Success: HTTP Status Code: 200. Resolved IP: 127.0.13.37. OK",
                        "CheckedTime": utcnow(),
                    },
                },
            ]
        }
        return ActionResult(result)

    def not_implemented_response(
        self, request: Any, full_url: str, headers: Any
    ) -> TYPE_RESPONSE:
        self.setup_class(request, full_url, headers)

        action = ""
        if "tags" in full_url:
            action = "tags"
        elif "trafficpolicyinstances" in full_url:
            action = "policies"
        raise NotImplementedError(
            f"The action for {action} has not been implemented for route 53"
        )

    def list_or_change_tags_for_resource_request(  # type: ignore[return]
        self, request: Any, full_url: str, headers: Any
    ) -> TYPE_RESPONSE:
        self.setup_class(request, full_url, headers)

        id_matcher = re.search(r"/tags/([-a-z]+)/(.+)", self.parsed_url.path)
        assert id_matcher
        type_ = id_matcher.group(1)
        id_ = unquote(id_matcher.group(2))
        if type_ == "hostedzone" and len(id_) > 32:
            # From testing, it looks like Route53 creates ID's that are either 21 or 22 characters long
            # In practice, this error will typically appear when passing in the full ID: `/hostedzone/{id}`
            # Users should pass in {id} instead
            # NOTE: we don't know (yet) what kind of validation (if any) is in place for type_==healthcheck.
            raise InvalidInput(
                f"1 validation error detected: Value '{id_}' at 'resourceId' failed to satisfy constraint: Member must have length less than or equal to 32"
            )

        if request.method == "GET":
            tags = self.backend.list_tags_for_resource(id_)
            template = Template(LIST_TAGS_FOR_RESOURCE_RESPONSE)
            return (
                200,
                headers,
                template.render(resource_type=type_, resource_id=id_, tags=tags),
            )

        if request.method == "POST":
            tags = xmltodict.parse(self.body)["ChangeTagsForResourceRequest"]

            if "AddTags" in tags:
                tags = tags["AddTags"]  # type: ignore
            elif "RemoveTagKeys" in tags:
                tags = tags["RemoveTagKeys"]  # type: ignore

            self.backend.change_tags_for_resource(type_, id_, tags)
            template = Template(CHANGE_TAGS_FOR_RESOURCE_RESPONSE)
            return 200, headers, template.render()

    def list_tags_for_resources(self) -> ActionResult:
        resource_type = self._get_param("ResourceType")
        resource_ids = self._get_param("ResourceIds")
        tag_sets = self.backend.list_tags_for_resources(resource_type, resource_ids)
        result = {"ResourceTagSets": tag_sets}
        return ActionResult(result)

    def get_change(self) -> ActionResult:
        change_id = self._get_param("Id")
        result = {
            "ChangeInfo": {
                "Id": change_id,
                "Status": "INSYNC",
                "SubmittedAt": utcnow(),
            }
        }
        return ActionResult(result)

    def create_query_logging_config(self) -> ActionResult:
        hosted_zone_id = self._get_param("HostedZoneId")
        log_group_arn = self._get_param("CloudWatchLogsLogGroupArn")
        query_logging_config = self.backend.create_query_logging_config(
            hosted_zone_id, log_group_arn
        )
        result = {
            "QueryLoggingConfig": query_logging_config,
            "Location": query_logging_config.location,
        }
        return ActionResult(result)

    def list_query_logging_configs(self) -> ActionResult:
        hosted_zone_id = self._get_param("HostedZoneId")
        next_token = self._get_param("NextToken")
        max_results = self._get_int_param("MaxResults", 100)
        all_configs, next_token = self.backend.list_query_logging_configs(
            hosted_zone_id=hosted_zone_id,
            next_token=next_token,
            max_results=max_results,
        )
        result = {"QueryLoggingConfigs": all_configs, "NextToken": next_token}
        return ActionResult(result)

    def get_query_logging_config(self) -> ActionResult:
        query_logging_config_id = self._get_param("Id")
        query_logging_config = self.backend.get_query_logging_config(
            query_logging_config_id
        )
        result = {"QueryLoggingConfig": query_logging_config}
        return ActionResult(result)

    def delete_query_logging_config(self) -> ActionResult:
        query_logging_config_id = self._get_param("Id")
        self.backend.delete_query_logging_config(query_logging_config_id)
        return EmptyResult()

    def list_reusable_delegation_sets(self) -> ActionResult:
        marker = self._get_param("Marker")
        delegation_sets = self.backend.list_reusable_delegation_sets()
        result = {
            "DelegationSets": delegation_sets,
            "Marker": marker,
            "IsTruncated": False,
            "NextMarker": None,
            "MaxItems": 100,
        }
        return ActionResult(result)

    def create_reusable_delegation_set(self) -> ActionResult:
        caller_reference = self._get_param("CallerReference")
        hosted_zone_id = self._get_param("HostedZoneId")
        delegation_set = self.backend.create_reusable_delegation_set(
            caller_reference=caller_reference, hosted_zone_id=hosted_zone_id
        )
        result = {"DelegationSet": delegation_set, "Location": delegation_set.location}
        return ActionResult(result)

    def get_reusable_delegation_set(self) -> ActionResult:
        ds_id = self._get_param("Id")
        delegation_set = self.backend.get_reusable_delegation_set(
            delegation_set_id=ds_id
        )
        result = {"DelegationSet": delegation_set}
        return ActionResult(result)

    def delete_reusable_delegation_set(self) -> ActionResult:
        ds_id = self._get_param("Id")
        self.backend.delete_reusable_delegation_set(delegation_set_id=ds_id)
        return EmptyResult()


LIST_TAGS_FOR_RESOURCE_RESPONSE = """
<ListTagsForResourceResponse xmlns="https://route53.amazonaws.com/doc/2015-01-01/">
    <ResourceTagSet>
        <ResourceType>{{resource_type}}</ResourceType>
        <ResourceId>{{resource_id}}</ResourceId>
        <Tags>
            {% for key, value in tags.items() %}
            <Tag>
                <Key>{{key}}</Key>
                <Value>{{value}}</Value>
            </Tag>
            {% endfor %}
        </Tags>
    </ResourceTagSet>
</ListTagsForResourceResponse>
"""

LIST_TAGS_FOR_RESOURCES_RESPONSE = """
<ListTagsForResourcesResponse xmlns="https://route53.amazonaws.com/doc/2015-01-01/">
    <ResourceTagSets>
        {% for set in tag_sets %}
        <ResourceTagSet>
            <ResourceType>{{resource_type}}</ResourceType>
            <ResourceId>{{set["ResourceId"]}}</ResourceId>
            <Tags>
                {% for key, value in set["Tags"].items() %}
                <Tag>
                    <Key>{{key}}</Key>
                    <Value>{{value}}</Value>
                </Tag>
                {% endfor %}
            </Tags>
        </ResourceTagSet>
        {% endfor %}
    </ResourceTagSets>
</ListTagsForResourcesResponse>
"""

CHANGE_TAGS_FOR_RESOURCE_RESPONSE = """<ChangeTagsForResourceResponse xmlns="https://route53.amazonaws.com/doc/2015-01-01/">
</ChangeTagsForResourceResponse>
"""

LIST_RRSET_RESPONSE = """<?xml version="1.0" encoding="UTF-8"?>
<ListResourceRecordSetsResponse xmlns="https://route53.amazonaws.com/doc/2012-12-12/">
   <ResourceRecordSets>
       {% for record in record_sets %}
       <ResourceRecordSet>
           <Name>{{ record.name }}</Name>
           <Type>{{ record.type_ }}</Type>
           {% if record.set_identifier %}
               <SetIdentifier>{{ record.set_identifier }}</SetIdentifier>
           {% endif %}
           {% if record.weight %}
               <Weight>{{ record.weight }}</Weight>
           {% endif %}
           {% if record.region %}
               <Region>{{ record.region }}</Region>
           {% endif %}
           {% if record.ttl %}
               <TTL>{{ record.ttl }}</TTL>
           {% endif %}
           {% if record.failover %}
               <Failover>{{ record.failover }}</Failover>
           {% endif %}
           {% if record.multi_value %}
               <MultiValueAnswer>{{ record.multi_value }}</MultiValueAnswer>
           {% endif %}
           {% if record.geo_location %}
           <GeoLocation>
           {% for geo_key in ['ContinentCode','CountryCode','SubdivisionCode'] %}
             {% if record.geo_location[geo_key] %}<{{ geo_key }}>{{ record.geo_location[geo_key] }}</{{ geo_key }}>{% endif %}
           {% endfor %}
           </GeoLocation>
           {% endif %}
           {% if record.alias_target %}
           <AliasTarget>
               <HostedZoneId>{{ record.alias_target['HostedZoneId'] }}</HostedZoneId>
               <DNSName>{{ record.alias_target['DNSName'] }}</DNSName>
               <EvaluateTargetHealth>{{ record.alias_target['EvaluateTargetHealth'] }}</EvaluateTargetHealth>
           </AliasTarget>
           {% else %}
           <ResourceRecords>
               {% for resource in record.records %}
               <ResourceRecord>
                   <Value><![CDATA[{{ resource }}]]></Value>
               </ResourceRecord>
               {% endfor %}
           </ResourceRecords>
           {% endif %}
           {% if record.health_check %}
               <HealthCheckId>{{ record.health_check }}</HealthCheckId>
           {% endif %}
        </ResourceRecordSet>
       {% endfor %}
   </ResourceRecordSets>
   {% if is_truncated %}<NextRecordName>{{ next_name }}</NextRecordName>{% endif %}
   {% if is_truncated %}<NextRecordType>{{ next_type }}</NextRecordType>{% endif %}
   <MaxItems>{{ max_items }}</MaxItems>
   <IsTruncated>{{ 'true' if is_truncated else 'false' }}</IsTruncated>
</ListResourceRecordSetsResponse>"""

CHANGE_RRSET_RESPONSE = """<ChangeResourceRecordSetsResponse xmlns="https://route53.amazonaws.com/doc/2012-12-12/">
   <ChangeInfo>
      <Status>INSYNC</Status>
      <SubmittedAt>2010-09-10T01:36:41.958Z</SubmittedAt>
      <Id>/change/C2682N5HXP0BZ4</Id>
   </ChangeInfo>
</ChangeResourceRecordSetsResponse>"""

DELETE_HOSTED_ZONE_RESPONSE = """<DeleteHostedZoneResponse xmlns="https://route53.amazonaws.com/doc/2012-12-12/">
    <ChangeInfo>
      <Status>INSYNC</Status>
      <SubmittedAt>2010-09-10T01:36:41.958Z</SubmittedAt>
      <Id>/change/C2682N5HXP0BZ4</Id>
   </ChangeInfo>
</DeleteHostedZoneResponse>"""

GET_HOSTED_ZONE_COUNT_RESPONSE = """<GetHostedZoneCountResponse> xmlns="https://route53.amazonaws.com/doc/2012-12-12/">
   <HostedZoneCount>{{ zone_count }}</HostedZoneCount>
</GetHostedZoneCountResponse>"""


GET_HOSTED_ZONE_RESPONSE = """<GetHostedZoneResponse xmlns="https://route53.amazonaws.com/doc/2012-12-12/">
   <HostedZone>
      <Id>/hostedzone/{{ zone.id }}</Id>
      <Name>{{ zone.name }}</Name>
        <CallerReference>{{ zone.caller_reference }}</CallerReference>
      <ResourceRecordSetCount>{{ zone.rrsets|count }}</ResourceRecordSetCount>
      <Config>
        {% if zone.comment %}
            <Comment>{{ zone.comment }}</Comment>
        {% endif %}
        <PrivateZone>{{ 'true' if zone.private_zone else 'false' }}</PrivateZone>
      </Config>
   </HostedZone>
   {% if not zone.private_zone %}
   <DelegationSet>
      <Id>{{ zone.delegation_set.id }}</Id>
      <NameServers>
        {% for name in zone.delegation_set.name_servers %}<NameServer>{{ name }}</NameServer>{% endfor %}
      </NameServers>
   </DelegationSet>
   {% endif %}
   {% if zone.private_zone %}
   <VPCs>
      {% for vpc in zone.vpcs %}
      <VPC>
         <VPCId>{{vpc.vpc_id}}</VPCId>
         <VPCRegion>{{vpc.vpc_region}}</VPCRegion>
      </VPC>
      {% endfor %}
   </VPCs>
   {% endif %}
</GetHostedZoneResponse>"""

CREATE_HOSTED_ZONE_RESPONSE = """<CreateHostedZoneResponse xmlns="https://route53.amazonaws.com/doc/2012-12-12/">
    {% if zone.private_zone %}
    <VPC>
      <VPCId>{{zone.vpcid}}</VPCId>
      <VPCRegion>{{zone.vpcregion}}</VPCRegion>
    </VPC>
    {% endif %}
   <HostedZone>
      <Id>/hostedzone/{{ zone.id }}</Id>
      <Name>{{ zone.name }}</Name>
      <CallerReference>{{ zone.caller_reference }}</CallerReference>
      <ResourceRecordSetCount>{{ zone.rrsets|count }}</ResourceRecordSetCount>
      <Config>
        {% if zone.comment %}
            <Comment>{{ zone.comment }}</Comment>
        {% endif %}
        <PrivateZone>{{ 'true' if zone.private_zone else 'false' }}</PrivateZone>
      </Config>
   </HostedZone>
   {% if not zone.private_zone %}
   <DelegationSet>
      <Id>{{ zone.delegation_set.id }}</Id>
      <NameServers>
         {% for name in zone.delegation_set.name_servers %}<NameServer>{{ name }}</NameServer>{% endfor %}
      </NameServers>
   </DelegationSet>
   {% endif %}
   <ChangeInfo>
      <Id>/change/C1PA6795UKMFR9</Id>
      <Status>INSYNC</Status>
      <SubmittedAt>2017-03-15T01:36:41.958Z</SubmittedAt>
   </ChangeInfo>
</CreateHostedZoneResponse>"""

LIST_HOSTED_ZONES_RESPONSE = """<ListHostedZonesResponse xmlns="https://route53.amazonaws.com/doc/2012-12-12/">
   <HostedZones>
      {% for zone in zones %}
      <HostedZone>
         <Id>/hostedzone/{{ zone.id }}</Id>
         <Name>{{ zone.name }}</Name>
         <CallerReference>{{ zone.caller_reference }}</CallerReference>
         <Config>
            {% if zone.comment %}
                <Comment>{{ zone.comment }}</Comment>
            {% endif %}
           <PrivateZone>{{ 'true' if zone.private_zone else 'false' }}</PrivateZone>
         </Config>
         <ResourceRecordSetCount>{{ zone.rrsets|count  }}</ResourceRecordSetCount>
      </HostedZone>
      {% endfor %}
   </HostedZones>
   {% if marker %}<Marker>{{ marker }}</Marker>{% endif %}
   {%if next_marker %}<NextMarker>{{ next_marker }}</NextMarker>{% endif %}
   {%if max_items %}<MaxItems>{{ max_items }}</MaxItems>{% endif %}
   <IsTruncated>{{ 'true' if next_marker else 'false'}}</IsTruncated>
</ListHostedZonesResponse>"""

LIST_HOSTED_ZONES_BY_NAME_RESPONSE = """<ListHostedZonesByNameResponse xmlns="{{ xmlns }}">
  {% if dnsname %}
  <DNSName>{{ dnsname }}</DNSName>
  {% endif %}
  <HostedZones>
      {% for zone in zones %}
      <HostedZone>
         <Id>/hostedzone/{{ zone.id }}</Id>
         <Name>{{ zone.name }}</Name>
         <CallerReference>{{ zone.caller_reference }}</CallerReference>
         <Config>
            {% if zone.comment %}
                <Comment>{{ zone.comment }}</Comment>
            {% endif %}
           <PrivateZone>{{ 'true' if zone.private_zone else 'false' }}</PrivateZone>
         </Config>
         <ResourceRecordSetCount>{{ zone.rrsets|count  }}</ResourceRecordSetCount>
      </HostedZone>
      {% endfor %}
   </HostedZones>
   <IsTruncated>false</IsTruncated>
</ListHostedZonesByNameResponse>"""

LIST_HOSTED_ZONES_BY_VPC_RESPONSE = """<ListHostedZonesByVpcResponse xmlns="{{xmlns}}">
   <HostedZoneSummaries>
       {% for zone in zones -%}
       <HostedZoneSummary>
           <HostedZoneId>{{zone["HostedZoneId"]}}</HostedZoneId>
           <Name>{{zone["Name"]}}</Name>
           <Owner>
               {% if zone["Owner"]["OwningAccount"] -%}
               <OwningAccount>{{zone["Owner"]["OwningAccount"]}}</OwningAccount>
               {% endif -%}
               {% if zone["Owner"]["OwningService"] -%}
               <OwningService>zone["Owner"]["OwningService"]</OwningService>
               {% endif -%}
           </Owner>
       </HostedZoneSummary>
       {% endfor -%}
   </HostedZoneSummaries>
</ListHostedZonesByVpcResponse>"""

CREATE_HEALTH_CHECK_RESPONSE = """<?xml version="1.0" encoding="UTF-8"?>
<CreateHealthCheckResponse xmlns="{{ xmlns }}">
  {{ health_check.to_xml() }}
</CreateHealthCheckResponse>"""

UPDATE_HEALTH_CHECK_RESPONSE = """<?xml version="1.0" encoding="UTF-8"?>
<UpdateHealthCheckResponse>
  {{ health_check.to_xml() }}
</UpdateHealthCheckResponse>
"""

LIST_HEALTH_CHECKS_RESPONSE = """<?xml version="1.0" encoding="UTF-8"?>
<ListHealthChecksResponse xmlns="{{ xmlns }}">
   <HealthChecks>
   {% for health_check in health_checks %}
      {{ health_check.to_xml() }}
    {% endfor %}
   </HealthChecks>
   <IsTruncated>false</IsTruncated>
   <MaxItems>{{ health_checks|length }}</MaxItems>
</ListHealthChecksResponse>"""

DELETE_HEALTH_CHECK_RESPONSE = """<?xml version="1.0" encoding="UTF-8"?>
    <DeleteHealthCheckResponse xmlns="{{ xmlns }}">
</DeleteHealthCheckResponse>"""

GET_CHANGE_RESPONSE = """<?xml version="1.0" encoding="UTF-8"?>
<GetChangeResponse xmlns="{{ xmlns }}">
   <ChangeInfo>
      <Status>INSYNC</Status>
      <SubmittedAt>2010-09-10T01:36:41.958Z</SubmittedAt>
      <Id>{{ change_id }}</Id>
   </ChangeInfo>
</GetChangeResponse>"""

CREATE_QUERY_LOGGING_CONFIG_RESPONSE = """<?xml version="1.0" encoding="UTF-8"?>
<CreateQueryLoggingConfigResponse xmlns="{{ xmlns }}">
  {{ query_logging_config.to_xml() }}
</CreateQueryLoggingConfigResponse>"""

GET_QUERY_LOGGING_CONFIG_RESPONSE = """<?xml version="1.0" encoding="UTF-8"?>
<CreateQueryLoggingConfigResponse xmlns="{{ xmlns }}">
  {{ query_logging_config.to_xml() }}
</CreateQueryLoggingConfigResponse>"""

LIST_QUERY_LOGGING_CONFIGS_RESPONSE = """<?xml version="1.0" encoding="UTF-8"?>
<ListQueryLoggingConfigsResponse xmlns="{{ xmlns }}">
   <QueryLoggingConfigs>
      {% for query_logging_config in query_logging_configs %}
         {{ query_logging_config.to_xml() }}
      {% endfor %}
   </QueryLoggingConfigs>
   {% if next_token %}
      <NextToken>{{ next_token }}</NextToken>
   {% endif %}
</ListQueryLoggingConfigsResponse>"""


CREATE_REUSABLE_DELEGATION_SET_TEMPLATE = """<CreateReusableDelegationSetResponse>
  <DelegationSet>
      <Id>{{ delegation_set.id }}</Id>
      <CallerReference>{{ delegation_set.caller_reference }}</CallerReference>
      <NameServers>
        {% for name in delegation_set.name_servers %}<NameServer>{{ name }}</NameServer>{% endfor %}
      </NameServers>
  </DelegationSet>
</CreateReusableDelegationSetResponse>
"""


LIST_REUSABLE_DELEGATION_SETS_TEMPLATE = """<ListReusableDelegationSetsResponse>
  <DelegationSets>
    {% for delegation in delegation_sets %}
    <DelegationSet>
  <Id>{{ delegation.id }}</Id>
  <CallerReference>{{ delegation.caller_reference }}</CallerReference>
  <NameServers>
    {% for name in delegation.name_servers %}<NameServer>{{ name }}</NameServer>{% endfor %}
  </NameServers>
</DelegationSet>
    {% endfor %}
  </DelegationSets>
  <Marker>{{ marker }}</Marker>
  <IsTruncated>{{ is_truncated }}</IsTruncated>
  <MaxItems>{{ max_items }}</MaxItems>
</ListReusableDelegationSetsResponse>
"""


DELETE_REUSABLE_DELEGATION_SET_TEMPLATE = """<DeleteReusableDelegationSetResponse>
  <DeleteReusableDelegationSetResponse/>
</DeleteReusableDelegationSetResponse>
"""

GET_REUSABLE_DELEGATION_SET_TEMPLATE = """<GetReusableDelegationSetResponse>
<DelegationSet>
  <Id>{{ delegation_set.id }}</Id>
  <CallerReference>{{ delegation_set.caller_reference }}</CallerReference>
  <NameServers>
    {% for name in delegation_set.name_servers %}<NameServer>{{ name }}</NameServer>{% endfor %}
  </NameServers>
</DelegationSet>
</GetReusableDelegationSetResponse>
"""

GET_DNSSEC = """<?xml version="1.0"?>
<GetDNSSECResponse>
    <Status>
        <ServeSignature>NOT_SIGNING</ServeSignature>
    </Status>
    <KeySigningKeys/>
</GetDNSSECResponse>
"""

GET_HEALTH_CHECK_RESPONSE = """<?xml version="1.0"?>
<GetHealthCheckResponse>
    {{ health_check.to_xml() }}
</GetHealthCheckResponse>
"""

GET_HEALTH_CHECK_STATUS_RESPONSE = """<?xml version="1.0" encoding="UTF-8"?>
<GetHealthCheckStatusResponse>
   <HealthCheckObservations>
      <HealthCheckObservation>
         <IPAddress>127.0.13.37</IPAddress>
         <Region>us-east-1</Region>
         <StatusReport>
            <CheckedTime>{{ timestamp }}</CheckedTime>
            <Status>Success: HTTP Status Code: 200. Resolved IP: 127.0.13.37. OK</Status>
         </StatusReport>
      </HealthCheckObservation>
   </HealthCheckObservations>
</GetHealthCheckStatusResponse>
"""

UPDATE_HOSTED_ZONE_COMMENT_RESPONSE = """<?xml version="1.0" encoding="UTF-8"?>
<UpdateHostedZoneCommentResponse>
   <HostedZone>
      <Config>
         {% if zone.comment %}
         <Comment>{{ zone.comment }}</Comment>
         {% endif %}
         <PrivateZone>{{ 'true' if zone.private_zone else 'false' }}</PrivateZone>
      </Config>
      <Id>/hostedzone/{{ zone.id }}</Id>
      <Name>{{ zone.name }}</Name>
      <ResourceRecordSetCount>{{ zone.rrsets|count }}</ResourceRecordSetCount>
   </HostedZone>
</UpdateHostedZoneCommentResponse>
"""

ASSOCIATE_VPC_RESPONSE = """<?xml version="1.0" encoding="UTF-8"?>
<AssociateVPCWithHostedZoneResponse>
   <ChangeInfo>
      <Comment>{{ comment or "" }}</Comment>
      <Id>/change/a1b2c3d4</Id>
      <Status>INSYNC</Status>
      <SubmittedAt>2017-03-31T01:36:41.958Z</SubmittedAt>
   </ChangeInfo>
</AssociateVPCWithHostedZoneResponse>
"""

DISASSOCIATE_VPC_RESPONSE = """<?xml version="1.0" encoding="UTF-8"?>
<DisassociateVPCFromHostedZoneResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
   <ChangeInfo>
      <Comment>{{ comment or "" }}</Comment>
      <Id>/change/a1b2c3d4</Id>
      <Status>INSYNC</Status>
      <SubmittedAt>2017-03-31T01:36:41.958Z</SubmittedAt>
   </ChangeInfo>
</DisassociateVPCFromHostedZoneResponse>
"""
