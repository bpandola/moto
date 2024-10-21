from typing import Any, Callable, Optional, Set

import boto3
from botocore.exceptions import ClientError

from moto.stepfunctions.parser.api import HistoryEventType, TaskFailedEventDetails
from moto.stepfunctions.parser.asl.component.common.error_name.custom_error_name import (
    CustomErrorName,
)
from moto.stepfunctions.parser.asl.component.common.error_name.failure_event import (
    FailureEvent,
    FailureEventException,
)
from moto.stepfunctions.parser.asl.component.common.error_name.states_error_name import (
    StatesErrorName,
)
from moto.stepfunctions.parser.asl.component.common.error_name.states_error_name_type import (
    StatesErrorNameType,
)
from moto.stepfunctions.parser.asl.component.state.exec.state_task.credentials import (
    ComputedCredentials,
)
from moto.stepfunctions.parser.asl.component.state.exec.state_task.service.resource import (
    ResourceCondition,
    ResourceRuntimePart,
)
from moto.stepfunctions.parser.asl.component.state.exec.state_task.service.state_task_service_callback import (
    StateTaskServiceCallback,
)
from moto.stepfunctions.parser.asl.eval.environment import Environment
from moto.stepfunctions.parser.asl.eval.event.event_detail import EventDetails
from moto.stepfunctions.parser.asl.utils.boto_client import boto_client_for
from moto.stepfunctions.parser.asl.utils.encoding import to_json_str

_SUPPORTED_INTEGRATION_PATTERNS: Set[ResourceCondition] = {
    ResourceCondition.Sync,
}

# Set of JobRunState value that indicate the JobRun had terminated in an abnormal state.
_JOB_RUN_STATE_ABNORMAL_TERMINAL_VALUE: Set[str] = {"FAILED", "TIMEOUT", "ERROR"}

# Set of JobRunState values that indicate the JobRun has terminated.
_JOB_RUN_STATE_TERMINAL_VALUES: Set[str] = {
    "STOPPED",
    "SUCCEEDED",
    *_JOB_RUN_STATE_ABNORMAL_TERMINAL_VALUE,
}

# The handler function name prefix for StateTaskServiceGlue objects.
_HANDLER_REFLECTION_PREFIX: str = "_handle_"
# The sync handler function name prefix for StateTaskServiceGlue objects.
_SYNC_HANDLER_REFLECTION_PREFIX: str = "_sync_to_"
# The type of (sync)handler function for StateTaskServiceGlue objects.
_API_ACTION_HANDLER_TYPE = Callable[
    [Environment, ResourceRuntimePart, dict, ComputedCredentials], None
]
# The type of (sync)handler builder function for StateTaskServiceGlue objects.
_API_ACTION_HANDLER_BUILDER_TYPE = Callable[
    [Environment, ResourceRuntimePart, dict, ComputedCredentials],
    Callable[[], Optional[Any]],
]


class StateTaskServiceGlue(StateTaskServiceCallback):
    def __init__(self):
        super().__init__(supported_integration_patterns=_SUPPORTED_INTEGRATION_PATTERNS)

    def _get_api_action_handler(self) -> _API_ACTION_HANDLER_TYPE:
        api_action = self._get_boto_service_action()
        handler_name = _HANDLER_REFLECTION_PREFIX + api_action
        resolver_handler = getattr(self, handler_name)
        if resolver_handler is None:
            raise ValueError(f"Unknown or unsupported glue action '{api_action}'.")
        return resolver_handler

    def _get_api_action_sync_builder_handler(self) -> _API_ACTION_HANDLER_BUILDER_TYPE:
        api_action = self._get_boto_service_action()
        handler_name = _SYNC_HANDLER_REFLECTION_PREFIX + api_action
        resolver_handler = getattr(self, handler_name)
        if resolver_handler is None:
            raise ValueError(f"Unknown or unsupported glue action '{api_action}'.")
        return resolver_handler

    @staticmethod
    def _get_glue_client(
        resource_runtime_part: ResourceRuntimePart,
        task_credentials: ComputedCredentials,
    ) -> boto3.client:
        return boto_client_for(
            region=resource_runtime_part.region,
            account=resource_runtime_part.account,
            service="glue",
            credentials=task_credentials,
        )

    def _from_error(self, env: Environment, ex: Exception) -> FailureEvent:
        if isinstance(ex, ClientError):
            error_code = ex.response["Error"]["Code"]
            error_name: str = f"Glue.{error_code}"
            return FailureEvent(
                env=env,
                error_name=CustomErrorName(error_name),
                event_type=HistoryEventType.TaskFailed,
                event_details=EventDetails(
                    taskFailedEventDetails=TaskFailedEventDetails(
                        error=error_name,
                        cause=ex.response["Error"]["Message"],
                        resource=self._get_sfn_resource(),
                        resourceType=self._get_sfn_resource_type(),
                    )
                ),
            )
        return super()._from_error(env=env, ex=ex)

    def _wait_for_task_token(
        self,
        env: Environment,
        resource_runtime_part: ResourceRuntimePart,
        normalised_parameters: dict,
    ) -> None:
        raise RuntimeError(
            f"Unsupported .waitForTaskToken callback procedure in resource {self.resource.resource_arn}"
        )

    def _handle_start_job_run(
        self,
        env: Environment,
        resource_runtime_part: ResourceRuntimePart,
        normalised_parameters: dict,
        computed_credentials: ComputedCredentials,
    ):
        glue_client = self._get_glue_client(
            resource_runtime_part=resource_runtime_part,
            task_credentials=computed_credentials,
        )
        response = glue_client.start_job_run(**normalised_parameters)
        response.pop("ResponseMetadata", None)
        # AWS StepFunctions extracts the JobName from the request and inserts it into the response, which
        # normally only contains JobRunID; as this is a required field for start_job_run, the access at
        # this depth is safe.
        response["JobName"] = normalised_parameters.get("JobName")
        env.stack.append(response)

    def _eval_service_task(
        self,
        env: Environment,
        resource_runtime_part: ResourceRuntimePart,
        normalised_parameters: dict,
        task_credentials: ComputedCredentials,
    ):
        # Source the action handler and delegate the evaluation.
        api_action_handler = self._get_api_action_handler()
        api_action_handler(
            env, resource_runtime_part, normalised_parameters, task_credentials
        )

    def _sync_to_start_job_run(
        self,
        env: Environment,
        resource_runtime_part: ResourceRuntimePart,
        normalised_parameters: dict,
        task_credentials: ComputedCredentials,
    ) -> Callable[[], Optional[Any]]:
        # Poll the job run state from glue, using GetJobRun until the job has terminated. Hence, append the output
        # of GetJobRun to the state.

        # Access the JobName and the JobRunId from the StartJobRun output call that must
        # have occurred before this point.
        start_job_run_output: dict = env.stack.pop()
        job_name: str = start_job_run_output["JobName"]
        job_run_id: str = start_job_run_output["JobRunId"]

        glue_client = self._get_glue_client(
            resource_runtime_part=resource_runtime_part,
            task_credentials=task_credentials,
        )

        def _sync_resolver() -> Optional[Any]:
            # Sample GetJobRun until completion.
            get_job_run_response: dict = glue_client.get_job_run(
                JobName=job_name, RunId=job_run_id
            )
            job_run: dict = get_job_run_response["JobRun"]
            job_run_state: str = job_run["JobRunState"]

            # If the job run has not terminated, continue and check later.
            is_terminated: bool = job_run_state in _JOB_RUN_STATE_TERMINAL_VALUES
            if not is_terminated:
                return None

            # AWS StepFunctions appears to append attach the JobName to the output both in case of error or success.
            job_run["JobName"] = job_name

            # If the job run terminated in a normal state, return the result.
            is_abnormal_termination = (
                job_run_state in _JOB_RUN_STATE_ABNORMAL_TERMINAL_VALUE
            )
            if not is_abnormal_termination:
                return job_run

            # If the job run has terminated with an abnormal state, raise the error in stepfunctions.
            raise FailureEventException(
                FailureEvent(
                    env=env,
                    error_name=StatesErrorName(
                        typ=StatesErrorNameType.StatesTaskFailed
                    ),
                    event_type=HistoryEventType.TaskFailed,
                    event_details=EventDetails(
                        taskFailedEventDetails=TaskFailedEventDetails(
                            resource=self._get_sfn_resource(),
                            resourceType=self._get_sfn_resource_type(),
                            error=StatesErrorNameType.StatesTaskFailed.to_name(),
                            cause=to_json_str(job_run),
                        )
                    ),
                )
            )

        return _sync_resolver

    def _build_sync_resolver(
        self,
        env: Environment,
        resource_runtime_part: ResourceRuntimePart,
        normalised_parameters: dict,
        task_credentials: ComputedCredentials,
    ) -> Callable[[], Optional[Any]]:
        sync_resolver_builder = self._get_api_action_sync_builder_handler()
        sync_resolver = sync_resolver_builder(
            env, resource_runtime_part, normalised_parameters, task_credentials
        )
        return sync_resolver