from moto.core.exceptions import ServiceException


class OpensearchIngestionExceptions(ServiceException):
    pass


class PipelineAlreadyExistsException(OpensearchIngestionExceptions):
    code = "ResourceAlreadyExistsException"

    def __init__(self, pipeline_name: str):
        super().__init__(f"Pipeline with given name {pipeline_name} already exists")


class PipelineInvalidStateException(OpensearchIngestionExceptions):
    code = "ConflictException"

    def __init__(self, action: str, valid_states: list[str], current_state: str | None):
        super().__init__(
            f"Only pipelines with one of the following statuses are eligible for {action}: {valid_states}. The current status is {current_state}."
        )


class PipelineNotFoundException(OpensearchIngestionExceptions):
    code = "ResourceNotFoundException"

    def __init__(self, pipeline_name: str):
        super().__init__(f"Pipeline {pipeline_name} could not be found.")


class PipelineValidationException(OpensearchIngestionExceptions):
    code = "ValidationException"


class InvalidVPCOptionsException(OpensearchIngestionExceptions):
    code = "ValidationException"

    def __init__(self, message: str) -> None:
        super().__init__(f"Invalid VpcOptions: {message}")


class SubnetNotFoundException(InvalidVPCOptionsException):
    def __init__(self, subnet_id: str) -> None:
        super().__init__(f"The subnet ID {subnet_id} does not exist")


class SecurityGroupNotFoundException(InvalidVPCOptionsException):
    def __init__(self, sg_id: str) -> None:
        super().__init__(f"The security group {sg_id} does not exist")


class ResourceNotFoundException(OpensearchIngestionExceptions):
    code = "ResourceNotFoundException"

    def __init__(self, resource_arn: str):
        super().__init__(f"Resource {resource_arn} could not be found.")


class ValidationException(OpensearchIngestionExceptions):
    code = "ValidationException"
