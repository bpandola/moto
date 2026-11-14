from moto.core.exceptions import ServiceException


class Route53ResolverException(ServiceException):
    pass


class RRValidationException(Route53ResolverException):
    code = "ValidationException"

    def __init__(self, error_tuples: list[tuple[str, str, str]]):
        """Validation errors are concatenated into one exception message.

        error_tuples is a list of tuples.  Each tuple contains:

          - name of invalid parameter,
          - value of invalid parameter,
          - string describing the constraints for that parameter.
        """
        msg_leader = (
            f"{len(error_tuples)} "
            f"validation error{'s' if len(error_tuples) > 1 else ''} detected: "
        )
        msgs = []
        for arg_name, arg_value, constraint in error_tuples:
            msgs.append(
                f"Value '{arg_value}' at '{arg_name}' failed to satisfy "
                f"constraint: Member must {constraint}"
            )
        super().__init__(msg_leader + "; ".join(msgs))


class InvalidNextTokenException(Route53ResolverException):
    code = "InvalidNextTokenException"
    message = "Invalid value passed for the NextToken parameter"


class InvalidParameterException(Route53ResolverException):
    code = "InvalidParameterException"


class InvalidRequestException(Route53ResolverException):
    code = "InvalidRequestException"


class LimitExceededException(Route53ResolverException):
    code = "LimitExceededException"


class ResourceExistsException(Route53ResolverException):
    code = "ResourceExistsException"


class ResourceInUseException(Route53ResolverException):
    code = "ResourceInUseException"


class ResourceNotFoundException(Route53ResolverException):
    code = "ResourceNotFoundException"


class TagValidationException(Route53ResolverException):
    code = "ValidationException"
