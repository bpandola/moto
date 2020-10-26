from .visitor import SKIP, Visitor
from .ast import ItemNode


class ASTValidationRule(Visitor):
    """Visitor for validation of an AST."""

    def __init__(self, context):
        self.context = context

    def report_error(self, error):
        self.context.report_error(error)


class ValidationRule(ASTValidationRule):
    """Visitor for validation using a GraphQL schema."""

    def __init__(self, context):
        super().__init__(context)


class ValuesOfCorrectTypeRule(ValidationRule):
    def enter_number_value(self, node, *_args):
        def parse():
            try:
                return int(node.value)
            except ValueError:
                return float(node.value)

        try:
            parse()
        except ValueError:
            self.report_error("The parameter cannot be converted to a numeric value")


class ItemRule(ValidationRule):
    """Unique operation names

    A GraphQL document is only valid if all defined operations have unique names.
    """

    def __init__(self, context):
        super().__init__(context)
        # self.known_operation_names: Dict[str, NameNode] = {}

    def enter_item(self, node, *_args):
        # operation_name = node.name
        # if operation_name:
        #     known_operation_names = self.known_operation_names
        #     if operation_name.value in known_operation_names:
        #         self.report_error(
        #             GraphQLError(
        #                 "There can be only one operation"
        #                 f" named '{operation_name.value}'.",
        #                 [known_operation_names[operation_name.value], operation_name],
        #             )
        #         )
        #     else:
        #         known_operation_names[operation_name.value] = operation_name
        return None

    def enter_attribute(self, node, *_args):
        return None

    def enter_attribute_name(self, node, *_args):
        return None

    # @staticmethod
    # def enter_fragment_definition(*_args: Any) -> VisitorAction:
    #     return SKIP


class KeysMustBePresent(ASTValidationRule):
    """All of the table's primary key attributes must be specified"""

    def __init__(self, context):
        super().__init__(context)
        self.attr_names = []
        self.key_names = [i["AttributeName"] for i in context.schema["key_schema"]]

    # TODO: Can probably put this in a base class...
    def is_item_attribute(self, ancestors):
        return len(ancestors) == 2 and isinstance(ancestors[0], ItemNode)

    def leave_item(self, *_args):
        for key in self.key_names:
            if key not in self.attr_names:
                self.report_error("Missing the key {} in the item".format(key))

    def enter_attribute_name(self, node, _key, _parent, _path, ancestors):
        if self.is_item_attribute(ancestors):
            self.attr_names.append(node.value)


class KeysMustBeOfCorrectType(ASTValidationRule):
    """All of the table's primary key attributes data types
    must match those of the table's key schema
    """

    def __init__(self, context):
        super().__init__(context)
        self.attr_definitions_stack = []
        self.attr_types = {}
        self.key_names = [
            i["AttributeName"] for i in context.schema["attribute_definitions"]
        ]
        self.key_definitions = context.schema["attribute_definitions"]

    def leave_item(self, *_args):
        for definition in self.key_definitions:
            key = definition["AttributeName"]
            type_expected = definition["AttributeType"]
            type_actual = self.attr_types.get(key, type_expected)
            if type_actual != type_expected:
                self.report_error(
                    "Type mismatch for key {key} expected: {expected} actual: {actual}".format(
                        key=key, expected=type_expected, actual=type_actual
                    )
                )

    def enter_map_attribute(self, *_args):
        self.attr_definitions_stack.append(self.attr_types)
        self.attr_types = {}

    def leave_map_attribute(self, *_args):
        self.attr_types = self.attr_definitions_stack.pop()

    def record_type(self, node, *_args):
        self.attr_types[node.name.value] = node.value.type

    # WE only have to enter the attributes that can be a key: S, N, and B
    enter_attribute = (
        enter_number_attribute
    ) = enter_string_attribute = enter_binary_attribute = record_type


item_rules = [
    ItemRule,
    ValuesOfCorrectTypeRule,
    KeysMustBePresent,
    KeysMustBeOfCorrectType,
]
