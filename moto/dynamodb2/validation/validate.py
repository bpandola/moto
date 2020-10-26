from .visitor import ParallelVisitor, visit
from .validation_context import ValidationContext
from .rules import item_rules
from ..exceptions import ValidationException


class ValidationAbortedError(RuntimeError):
    """Error when a validation has been aborted (error limit reached)."""


def validate(
    schema, document_ast, rules=None, max_errors=None,
):
    """Implements the "Validation" section of the spec.

    Validation runs synchronously, returning a list of encountered errors, or an empty
    list if no errors were encountered and the document is valid.

    A list of specific validation rules may be provided. If not provided, the default
    list of rules defined by the GraphQL specification will be used.

    Each validation rule is a ValidationRule object which is a visitor object that holds
    a ValidationContext (see the language/visitor API). Visitor methods are expected to
    return GraphQLErrors, or lists of GraphQLErrors when invalid.

    Optionally a custom TypeInfo instance may be provided. If not provided, one will be
    created from the provided schema.
    """
    # if not document_ast or not isinstance(document_ast, DocumentNode):
    #     raise TypeError("Must provide document.")
    # If the schema used for validation is invalid, throw an error.
    # assert_valid_schema(schema)
    # if type_info is None:
    #     type_info = TypeInfo(schema)
    # elif not isinstance(type_info, TypeInfo):
    #     raise TypeError(f"Not a TypeInfo object: {inspect(type_info)}.")
    if rules is None:
        rules = item_rules
    # elif not is_collection(rules) or not all(
    #     isinstance(rule, type) and issubclass(rule, ASTValidationRule) for rule in rules
    # ):
    #     raise TypeError(
    #         "Rules must be specified as a collection of ASTValidationRule subclasses."
    #     )
    if max_errors is not None and not isinstance(max_errors, int):
        raise TypeError("The maximum number of errors must be passed as an int.")

    errors = []

    def on_error(error):
        if max_errors is not None and len(errors) >= max_errors:
            errors.append(
                "Too many validation errors, error limit reached."
                " Validation aborted."
            )
            raise ValidationAbortedError
        errors.append(error)

    context = ValidationContext(schema, document_ast, on_error)

    # This uses a specialized visitor which runs multiple visitors in parallel,
    # while maintaining the visitor skip and break API.
    visitors = [rule(context) for rule in rules]

    # Visit the whole document with each instance of all provided rules.
    try:
        visit(document_ast, ParallelVisitor(visitors))
    except ValidationAbortedError:
        pass
    return errors
