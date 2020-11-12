"""User input parameter validation.

This module handles user input parameter validation
against a provided input model.

Note that the objects in this module do *not* mutate any
arguments.  No type conversions happens here.  It is up to another
layer to properly convert arguments to any required types.

Validation Errors
-----------------


"""

import botocore.session
from botocore.compat import six
from botocore.validate import ParamValidator, type_check, ValidationErrors

from moto.core.utils import pascal_to_camelcase

SERVICE_MODEL = (
    botocore.session.get_session().create_client("dynamodb").meta.service_model
)


class ValidationAbortedError(RuntimeError):
    """Error when a validation has been aborted (error limit reached)."""


def validate_parameters(params, operation_name, max_errors=1):
    """Validates input parameters against a schema.

    This is a convenience function that validates parameters against a schema.
    You can also instantiate and use the ParamValidator class directly if you
    want more control.

    If there are any validation errors then a ParamValidationError
    will be raised.  If there are no validation errors than no exception
    is raised and a value of None is returned.

    :param params: The user provided input parameters.

    :param operation_name: The schema which the input parameters should
        adhere to.

    :param max_errors:

    :raise: ParamValidationError

    """
    operation_model = SERVICE_MODEL.operation_model(operation_name)
    input_shape = operation_model.input_shape
    if input_shape is None:
        return
    validator = DDBParamValidator()
    errors = DDBValidationErrors(max_errors)
    try:
        validator.do_validation(params, input_shape, errors)
    except ValidationAbortedError:
        pass
    if errors.has_errors():
        return errors.generate_report()


def range_check_max(name, value, length, shape, error_type, errors):
    failed = False
    max_allowed = float("inf")
    if "max" in shape.metadata:
        max_allowed = shape.metadata["max"]
        if length > max_allowed:
            failed = True
    if failed:
        errors.report(name, error_type, param=value, valid_max=max_allowed)


def enum_check(name, value, shape, error_type, errors):
    failed = False
    if shape.enum:
        if value not in shape.enum:
            failed = True
    if failed:
        errors.report(name, error_type, param=value, valid_values=shape.enum)


class DDBValidationErrors(ValidationErrors):
    def __init__(self, max_errors):
        super(DDBValidationErrors, self).__init__()
        self.max_errors = max_errors

    # def has_errors(self):
    #     if self._errors:
    #         return True
    #     return False

    def generate_report(self):
        num_errors = len(self._errors)
        report_header = "%s validation %s detected: " % (
            num_errors,
            "error" if num_errors == 1 else "errors",
        )
        report = super(DDBValidationErrors, self).generate_report()
        return report_header + report

    def _format_error(self, error):
        error_type, name, additional = error
        name = self._get_name(name)
        if error_type == "missing required field":
            return 'Missing required parameter in %s: "%s"' % (
                name,
                additional["required_name"],
            )
        elif error_type == "unknown field":
            return 'Unknown parameter in %s: "%s", must be one of: %s' % (
                name,
                additional["unknown_param"],
                ", ".join(additional["valid_names"]),
            )
        elif error_type == "invalid type":
            return (
                "Invalid type for parameter %s, value: %s, type: %s, "
                "valid types: %s"
                % (
                    name,
                    additional["param"],
                    str(type(additional["param"])),
                    ", ".join(additional["valid_types"]),
                )
            )
        elif error_type == "invalid range":
            min_allowed = additional["valid_range"][0]
            max_allowed = additional["valid_range"][1]
            return (
                "Invalid range for parameter %s, value: %s, valid range: "
                "%s-%s" % (name, additional["param"], min_allowed, max_allowed)
            )
        elif error_type == "invalid length":
            min_allowed = additional["valid_range"][0]
            max_allowed = additional["valid_range"][1]
            return (
                "Invalid length for parameter %s, value: %s, valid range: "
                "%s-%s" % (name, additional["param"], min_allowed, max_allowed)
            )
        elif error_type == "unable to encode to json":
            return "Invalid parameter %s must be json serializable: %s" % (
                name,
                additional["type_error"],
            )
        elif error_type == "invalid enum":
            return (
                "Value '%s' at '%s' failed to satisfy constraint: Member must satisfy enum value set: [%s]"
                % (additional["param"], name, ", ".join(additional["valid_values"]))
            )
        elif error_type == "invalid max":
            return (
                "Value '%s' at '%s' failed to satisfy constraint: Member must have length less than or equal to %s"
                % (additional["param"], name, additional["valid_max"])
            )
        else:
            return super(DDBValidationErrors, self)._format_error(error)

    def _get_name(self, name):
        if not name:
            name = "input"
        elif name.startswith("."):
            name = name[1:]
        return ".".join([pascal_to_camelcase(name) for name in name.split(".")])

    def report(self, name, reason, **kwargs):
        self._errors.append((reason, name, kwargs))
        if len(self._errors) >= self.max_errors:
            raise ValidationAbortedError()


class DDBParamValidator(ParamValidator):
    """Validates parameters against a shape model."""

    def do_validation(self, params, shape, errors):
        """Validate parameters against a shape model.

        This method will validate the parameters against a provided shape model.
        All errors will be collected before returning to the caller.  This means
        that this method will not stop at the first error, it will return all
        possible errors.

        :param params: User provided dict of parameters
        :param shape: A shape model describing the expected input.
        :param errors:
        :return: A list of errors.

        """
        self._validate(params, shape, errors, name="")
        return errors

    #
    # def _check_special_validation_cases(self, shape):
    #     if is_json_value_header(shape):
    #         return self._validate_jsonvalue_string
    #
    # def _validate(self, params, shape, errors, name):
    #     special_validator = self._check_special_validation_cases(shape)
    #     if special_validator:
    #         special_validator(params, shape, errors, name)
    #     else:
    #         getattr(self, '_validate_%s' % shape.type_name)(
    #             params, shape, errors, name)
    #
    # def _validate_jsonvalue_string(self, params, shape, errors, name):
    #     # Check to see if a value marked as a jsonvalue can be dumped to
    #     # a json string.
    #     try:
    #         json.dumps(params)
    #     except (ValueError, TypeError) as e:
    #         errors.report(name, 'unable to encode to json', type_error=e)
    #
    # @type_check(valid_types=(dict,))
    # def _validate_structure(self, params, shape, errors, name):
    #     # Validate required fields.
    #     for required_member in shape.metadata.get('required', []):
    #         if required_member not in params:
    #             errors.report(name, 'missing required field',
    #                           required_name=required_member, user_params=params)
    #     members = shape.members
    #     known_params = []
    #     # Validate known params.
    #     for param in params:
    #         if param not in members:
    #             errors.report(name, 'unknown field', unknown_param=param,
    #                           valid_names=list(members))
    #         else:
    #             known_params.append(param)
    #     # Validate structure members.
    #     for param in known_params:
    #         self._validate(params[param], shape.members[param],
    #                        errors, '%s.%s' % (name, param))

    @type_check(valid_types=six.string_types)
    def _validate_string(self, param, shape, errors, name):
        # Super call handles range_check_min
        super(DDBParamValidator, self)._validate_string(param, shape, errors, name)
        range_check_max(name, param, len(param), shape, "invalid length", errors)
        enum_check(name, param, shape, "invalid enum", errors)

    @type_check(valid_types=(list, tuple))
    def _validate_list(self, param, shape, errors, name):
        # Super call handles range_check_min
        super(DDBParamValidator, self)._validate_list(param, shape, errors, name)
        range_check_max(name, param, len(param), shape, "invalid max", errors)

    @type_check(valid_types=(dict,))
    def _validate_map(self, param, shape, errors, name):
        key_shape = shape.key
        value_shape = shape.value
        for key, value in param.items():
            self._validate(key, key_shape, errors, "%s (key: %s)" % (name, key))
            self._validate(value, value_shape, errors, "%s.%s.member" % (name, key))

    #
    # @type_check(valid_types=six.integer_types)
    # def _validate_integer(self, param, shape, errors, name):
    #     range_check(name, param, shape, 'invalid range', errors)
    #
    # def _validate_blob(self, param, shape, errors, name):
    #     if isinstance(param, (bytes, bytearray, six.text_type)):
    #         return
    #     elif hasattr(param, 'read'):
    #         # File like objects are also allowed for blob types.
    #         return
    #     else:
    #         errors.report(name, 'invalid type', param=param,
    #                       valid_types=[str(bytes), str(bytearray),
    #                                    'file-like object'])
    #
    # @type_check(valid_types=(bool,))
    # def _validate_boolean(self, param, shape, errors, name):
    #     pass
    #
    # @type_check(valid_types=(float, decimal.Decimal) + six.integer_types)
    # def _validate_double(self, param, shape, errors, name):
    #     range_check(name, param, shape, 'invalid range', errors)
    #
    # _validate_float = _validate_double
    #
    # @type_check(valid_types=six.integer_types)
    # def _validate_long(self, param, shape, errors, name):
    #     range_check(name, param, shape, 'invalid range', errors)
    #
    # def _validate_timestamp(self, param, shape, errors, name):
    #     # We don't use @type_check because datetimes are a bit
    #     # more flexible.  You can either provide a datetime
    #     # object, or a string that parses to a datetime.
    #     is_valid_type = self._type_check_datetime(param)
    #     if not is_valid_type:
    #         valid_type_names = [six.text_type(datetime), 'timestamp-string']
    #         errors.report(name, 'invalid type', param=param,
    #                       valid_types=valid_type_names)
    #
    # def _type_check_datetime(self, value):
    #     try:
    #         parse_to_aware_datetime(value)
    #         return True
    #     except (TypeError, ValueError, AttributeError):
    #         # Yes, dateutil can sometimes raise an AttributeError
    #         # when parsing timestamps.
    #         return False
