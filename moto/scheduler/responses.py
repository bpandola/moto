"""Handles incoming scheduler requests, invokes methods, returns responses."""

import calendar
import datetime
import json
from typing import Any, Optional

from moto.core.responses import BaseResponse

from .models import EventBridgeSchedulerBackend, scheduler_backends


class EventBridgeSchedulerResponse(BaseResponse):
    """Handler for EventBridgeScheduler requests and responses."""

    def __init__(self) -> None:
        super().__init__(service_name="scheduler")
        self.automated_parameter_parsing = True

    @property
    def scheduler_backend(self) -> EventBridgeSchedulerBackend:
        """Return backend instance specific for this region."""
        return scheduler_backends[self.current_account][self.region]

    @staticmethod
    def _to_timestamp(value: Optional[Any]) -> Optional[Any]:
        """Convert a datetime to a Unix timestamp if needed."""
        if isinstance(value, datetime.datetime):
            return calendar.timegm(value.utctimetuple())
        return value

    def create_schedule(self) -> str:
        description = self._get_param("Description")
        end_date = self._to_timestamp(self._get_param("EndDate"))
        flexible_time_window = self._get_param("FlexibleTimeWindow")
        group_name = self._get_param("GroupName")
        kms_key_arn = self._get_param("KmsKeyArn")
        name = self._get_param("Name")
        schedule_expression = self._get_param("ScheduleExpression")
        schedule_expression_timezone = self._get_param("ScheduleExpressionTimezone")
        start_date = self._to_timestamp(self._get_param("StartDate"))
        state = self._get_param("State")
        target = self._get_param("Target")
        action_after_completion = self._get_param("ActionAfterCompletion")
        schedule = self.scheduler_backend.create_schedule(
            description=description,
            end_date=end_date,
            flexible_time_window=flexible_time_window,
            group_name=group_name,
            kms_key_arn=kms_key_arn,
            name=name,
            schedule_expression=schedule_expression,
            schedule_expression_timezone=schedule_expression_timezone,
            start_date=start_date,
            state=state,
            target=target,
            action_after_completion=action_after_completion,
        )
        return json.dumps({"ScheduleArn": schedule.arn})

    def get_schedule(self) -> str:
        group_name = self._get_param("GroupName")
        name = self._get_param("Name")
        schedule = self.scheduler_backend.get_schedule(group_name, name)
        return json.dumps(schedule.to_dict())

    def delete_schedule(self) -> str:
        group_name = self._get_param("GroupName")
        name = self._get_param("Name")
        self.scheduler_backend.delete_schedule(group_name, name)
        return "{}"

    def update_schedule(self) -> str:
        group_name = self._get_param("GroupName")
        name = self._get_param("Name")
        description = self._get_param("Description")
        end_date = self._to_timestamp(self._get_param("EndDate"))
        flexible_time_window = self._get_param("FlexibleTimeWindow")
        kms_key_arn = self._get_param("KmsKeyArn")
        schedule_expression = self._get_param("ScheduleExpression")
        schedule_expression_timezone = self._get_param("ScheduleExpressionTimezone")
        start_date = self._to_timestamp(self._get_param("StartDate"))
        state = self._get_param("State")
        target = self._get_param("Target")
        schedule = self.scheduler_backend.update_schedule(
            description=description,
            end_date=end_date,
            flexible_time_window=flexible_time_window,
            group_name=group_name,
            kms_key_arn=kms_key_arn,
            name=name,
            schedule_expression=schedule_expression,
            schedule_expression_timezone=schedule_expression_timezone,
            start_date=start_date,
            state=state,
            target=target,
        )
        return json.dumps({"ScheduleArn": schedule.arn})

    def list_schedules(self) -> str:
        group_names = self._get_param("GroupName")
        state = self._get_param("State")
        name_prefix = self._get_param("NamePrefix")
        next_token = self._get_param("NextToken")
        max_results = self._get_int_param("MaxResults")
        schedules, next_token = self.scheduler_backend.list_schedules(
            group_names,
            state,
            name_prefix,
            max_results=max_results,
            next_token=next_token,
        )
        result = {
            "Schedules": [sch.to_dict(short=True) for sch in schedules],
            "NextToken": next_token,
        }
        return json.dumps(result)

    def create_schedule_group(self) -> str:
        name = self._get_param("Name")
        tags = self._get_param("Tags")
        schedule_group = self.scheduler_backend.create_schedule_group(
            name=name,
            tags=tags,
        )
        return json.dumps({"ScheduleGroupArn": schedule_group.arn})

    def get_schedule_group(self) -> str:
        group_name = self._get_param("Name")
        group = self.scheduler_backend.get_schedule_group(group_name)
        return json.dumps(group.to_dict())

    def delete_schedule_group(self) -> str:
        group_name = self._get_param("Name")
        self.scheduler_backend.delete_schedule_group(group_name)
        return "{}"

    def list_schedule_groups(self) -> str:
        name_prefix = self._get_param("NamePrefix")
        next_token = self._get_param("NextToken")
        max_results = self._get_int_param("MaxResults")
        schedule_groups, next_token = self.scheduler_backend.list_schedule_groups(
            name_prefix, max_results=max_results, next_token=next_token
        )
        result = {
            "ScheduleGroups": [sg.to_dict() for sg in schedule_groups],
            "NextToken": next_token,
        }
        return json.dumps(result)

    def list_tags_for_resource(self) -> str:
        resource_arn = self._get_param("ResourceArn")
        tags = self.scheduler_backend.list_tags_for_resource(resource_arn)
        return json.dumps(tags)

    def tag_resource(self) -> str:
        resource_arn = self._get_param("ResourceArn")
        tags = self._get_param("Tags")
        self.scheduler_backend.tag_resource(resource_arn, tags)
        return "{}"

    def untag_resource(self) -> str:
        resource_arn = self._get_param("ResourceArn")
        tag_keys = self._get_param("TagKeys")
        self.scheduler_backend.untag_resource(resource_arn, tag_keys)  # type: ignore
        return "{}"
