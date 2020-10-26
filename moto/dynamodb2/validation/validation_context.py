class ASTValidationContext:
    """Utility class providing a context for validation of an AST.

    An instance of this class is passed as the context attribute to all Validators,
    allowing access to commonly useful contextual information from within a validation
    rule.
    """

    def __init__(self, ast, on_error):
        self.document = ast
        self.on_error = on_error  # type: ignore

    def on_error(self, error):
        pass

    def report_error(self, error):
        self.on_error(error)


class ValidationContext(ASTValidationContext):
    """Utility class providing a context for validation using a GraphQL schema.

    An instance of this class is passed as the context attribute to all Validators,
    allowing access to commonly useful contextual information from within a validation
    rule.
    """

    def __init__(
        self, schema, ast, on_error,
    ):
        super().__init__(ast, on_error)
        self.schema = schema
