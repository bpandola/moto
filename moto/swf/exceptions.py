from typing import TYPE_CHECKING, Any

from moto.core.exceptions import ServiceException

if TYPE_CHECKING:
    from .models.generic_type import GenericType


class SWFClientError(ServiceException):
    pass


class SWFUnknownResourceFault(SWFClientError):
    code = "UnknownResourceFault"

    def __init__(self, resource_type: str, resource_name: str | None = None):
        if resource_name:
            message = f"Unknown {resource_type}: {resource_name}"
        else:
            message = f"Unknown {resource_type}"
        super().__init__(message)


class SWFDomainAlreadyExistsFault(SWFClientError):
    code = "DomainAlreadyExistsFault"

    def __init__(self, domain_name: str):
        super().__init__(domain_name)


class SWFDomainDeprecatedFault(SWFClientError):
    code = "DomainDeprecatedFault"

    def __init__(self, domain_name: str):
        super().__init__(domain_name)


class SWFSerializationException(SWFClientError):
    code = "SerializationException"

    def __init__(self, value: Any):
        message = "class java.lang.Foo can not be converted to an String "
        message += f" (not a real SWF exception ; happened on: {value})"
        super().__init__(message)


class SWFTypeAlreadyExistsFault(SWFClientError):
    code = "TypeAlreadyExistsFault"

    def __init__(self, _type: "GenericType"):
        super().__init__(
            f"{_type.__class__.__name__}=[name={_type.name}, version={_type.version}]"
        )


class SWFTypeDeprecatedFault(SWFClientError):
    code = "TypeDeprecatedFault"

    def __init__(self, _type: "GenericType"):
        super().__init__(
            f"{_type.__class__.__name__}=[name={_type.name}, version={_type.version}]"
        )


class SWFWorkflowExecutionAlreadyStartedFault(SWFClientError):
    code = "WorkflowExecutionAlreadyStartedFault"
    message = "Already Started"


class SWFDefaultUndefinedFault(SWFClientError):
    code = "DefaultUndefinedFault"

    def __init__(self, key: str):
        # TODO: move that into moto.core.utils maybe?
        words = key.split("_")
        key_camel_case = words.pop(0)
        for word in words:
            key_camel_case += word.capitalize()
        super().__init__(key_camel_case)


class SWFValidationException(SWFClientError):
    code = "ValidationException"


class SWFDecisionValidationException(SWFClientError):
    code = "ValidationException"

    def __init__(self, problems: list[dict[str, Any]]):
        # messages
        messages = []
        for pb in problems:
            if pb["type"] == "null_value":
                messages.append(
                    f"Value null at '{pb['where']}' failed to satisfy constraint: Member must not be null"
                )
            elif pb["type"] == "bad_decision_type":
                messages.append(
                    f"Value '{pb['value']}' at '{pb['where']}' failed to satisfy constraint: "
                    f"Member must satisfy enum value set: [{pb['possible_values']}]"
                )
            else:
                raise ValueError(f"Unhandled decision constraint type: {pb['type']}")
        # prefix
        count = len(problems)
        if count < 2:
            prefix = "{0} validation error detected: "
        else:
            prefix = "{0} validation errors detected: "
        super().__init__(prefix.format(count) + "; ".join(messages))


class SWFWorkflowExecutionClosedError(Exception):
    def __str__(self) -> str:
        return repr("Cannot change this object because the WorkflowExecution is closed")
