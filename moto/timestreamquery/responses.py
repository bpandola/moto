import json

from moto.core.responses import BaseResponse

from .models import TimestreamQueryBackend, timestreamquery_backends


class TimestreamQueryResponse(BaseResponse):
    def __init__(self) -> None:
        super().__init__(service_name="timestream-query")

    @property
    def timestreamquery_backend(self) -> TimestreamQueryBackend:
        return timestreamquery_backends[self.current_account][self.region]

    def describe_endpoints(self) -> str:
        endpoints = self.timestreamquery_backend.describe_endpoints()
        return json.dumps({"Endpoints": endpoints})

    def create_scheduled_query(self) -> str:
        name = self._get_param("Name")
        query_string = self._get_param("QueryString")
        schedule_configuration = self._get_param("ScheduleConfiguration")
        notification_configuration = self._get_param("NotificationConfiguration")
        target_configuration = self._get_param("TargetConfiguration")
        scheduled_query_execution_role_arn = self._get_param(
            "ScheduledQueryExecutionRoleArn"
        )
        tags = self._get_param("Tags")
        kms_key_id = self._get_param("KmsKeyId")
        error_report_configuration = self._get_param("ErrorReportConfiguration")
        scheduled_query = self.timestreamquery_backend.create_scheduled_query(
            name=name,
            query_string=query_string,
            schedule_configuration=schedule_configuration,
            notification_configuration=notification_configuration,
            target_configuration=target_configuration,
            scheduled_query_execution_role_arn=scheduled_query_execution_role_arn,
            tags=tags,
            kms_key_id=kms_key_id,
            error_report_configuration=error_report_configuration,
        )
        return json.dumps({"Arn": scheduled_query.arn})

    def delete_scheduled_query(self) -> str:
        scheduled_query_arn = self._get_param("ScheduledQueryArn")
        self.timestreamquery_backend.delete_scheduled_query(
            scheduled_query_arn=scheduled_query_arn,
        )
        return "{}"

    def update_scheduled_query(self) -> str:
        scheduled_query_arn = self._get_param("ScheduledQueryArn")
        state = self._get_param("State")
        self.timestreamquery_backend.update_scheduled_query(
            scheduled_query_arn=scheduled_query_arn,
            state=state,
        )
        return "{}"

    def query(self) -> str:
        query_string = self._get_param("QueryString")
        result = self.timestreamquery_backend.query(query_string=query_string)
        return json.dumps(result)

    def describe_scheduled_query(self) -> str:
        scheduled_query_arn = self._get_param("ScheduledQueryArn")
        scheduled_query = self.timestreamquery_backend.describe_scheduled_query(
            scheduled_query_arn=scheduled_query_arn,
        )
        return json.dumps({"ScheduledQuery": scheduled_query.description()})
