from .visitor import Visitor

# ERROR MESSAGES
ERR_MSG_PREFIX = "One or more parameter values were invalid: "
ERR_MSG_PREFIX_ALT = "One or more parameter values are not valid. "

KEY_MISSING = ERR_MSG_PREFIX + "Missing the key {key} in the item"
KEY_TYPE_MISMATCH = (
    ERR_MSG_PREFIX + "Type mismatch for key {key} expected: {expected} actual: {actual}"
)
KEY_EMPTY_STRING = (
    ERR_MSG_PREFIX_ALT
    + "The AttributeValue for a key attribute cannot contain an empty string value. Key: {key}"
)

CANNOT_CONVERT_TO_NUMERIC_VALUE = (
    "The parameter cannot be converted to a numeric value: {value}"
)
COLLECTION_CONTAINS_DUPLICATES = "Input collection [, ] contains duplicates."


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
            self.report_error(CANNOT_CONVERT_TO_NUMERIC_VALUE.format(value=node.value))

    def enter_string_set_value(self, node, *_args):
        if len(node.values) > len(set(node.values)):
            self.report_error(COLLECTION_CONTAINS_DUPLICATES)


class KeysMustBePresent(ValidationRule):
    """All of the table's primary key attributes must be specified"""

    def __init__(self, context):
        super(KeysMustBePresent, self).__init__(context)
        self.key_names = [i["AttributeName"] for i in context.schema["key_schema"]]

    def enter_item(self, item, *_args):
        item_attribute_names = [attr.name.value for attr in item.attributes]
        for key in self.key_names:
            if key not in item_attribute_names:
                self.report_error(KEY_MISSING.format(key=key))

    def attribute_value(self, node, key, parent, path, ancestors):
        print(path)


class KeysMustBeOfCorrectType(ValidationRule):
    """All of the table's primary key attributes data types
    must match those of the table's key schema.
    """

    def __init__(self, context):
        super(KeysMustBeOfCorrectType, self).__init__(context)
        self.key_to_type_map = {
            i["AttributeName"]: i["AttributeType"]
            for i in context.schema["attribute_definitions"]
        }

    def enter_item(self, item, *_args):
        for attr in item.attributes:
            if attr.name.value in self.key_to_type_map:
                type_expected = self.key_to_type_map[attr.name.value]
                type_actual = attr.value.type
                if type_actual != type_expected:
                    self.report_error(
                        KEY_TYPE_MISMATCH.format(
                            key=attr.name.value,
                            expected=type_expected,
                            actual=type_actual,
                        )
                    )


class KeysMustBeNonEmpty(ValidationRule):
    def __init__(self, context):
        super(KeysMustBeNonEmpty, self).__init__(context)
        self.key_names = [
            i["AttributeName"] for i in context.schema["attribute_definitions"]
        ]

    def enter_item(self, item, *_args):
        for attr in item.attributes:
            if attr.name.value in self.key_names:
                if getattr(attr.value.data, "value", None) == "":
                    self.report_error(KEY_EMPTY_STRING.format(key=attr.name.value))

class PathTest(ValidationRule):

    def enter_attribute_value(self, node, key, parent, path, ancestors):
        test = 'fuck'
        print(path)
# Order matters here.  They run in parallel, but failures get reported in order.
item_rules = [
    # ValuesOfCorrectTypeRule,
    # KeysMustBePresent,
    # KeysMustBeOfCorrectType,
    # KeysMustBeNonEmpty,
    PathTest,
]
