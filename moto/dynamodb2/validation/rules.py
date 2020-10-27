from .ast import ItemNode
from .visitor import Visitor


class ASTValidationRule(Visitor):
    """Visitor for validation of an AST."""

    def __init__(self, context):
        super(ASTValidationRule, self).__init__()
        self.context = context

    def report_error(self, error):
        self.context.report_error(error)


class ValidationRule(ASTValidationRule):
    """Visitor for validation using a GraphQL schema."""

    def __init__(self, context):
        super(ValidationRule, self).__init__(context)


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


class KeysMustBePresent(ASTValidationRule):
    """All of the table's primary key attributes must be specified"""

    def __init__(self, context):
        super(KeysMustBePresent, self).__init__(context)
        self.attr_names = []
        self.key_names = [i["AttributeName"] for i in context.schema["key_schema"]]

    # TODO: Can probably put this in a base class... or even outside the class.
    @staticmethod
    def is_item_attribute(ancestors):
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
        super(KeysMustBeOfCorrectType, self).__init__(context)
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


# Order matters here.  They run in parallel, but failures get reported in order.
item_rules = [
    ValuesOfCorrectTypeRule,
    KeysMustBePresent,
    KeysMustBeOfCorrectType,
]
