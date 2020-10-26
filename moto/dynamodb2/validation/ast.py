from copy import deepcopy
from moto.core.utils import camelcase_to_underscores

# Base AST Node


class Node:
    """AST nodes"""

    # allow custom attributes and weak references (not used internally)
    __slots__ = "__dict__", "__weakref__"

    kind: str = "ast"  # the kind of the node as a snake_case string
    keys = []  # the names of the attributes of this node

    def __init__(self, **kwargs):
        """Initialize the node with the given keyword arguments."""
        for key in self.keys:
            value = kwargs.get(key)
            # if isinstance(value, list) and not isinstance(value, FrozenList):
            #     value = FrozenList(value)
            setattr(self, key, value)

    def __repr__(self):
        """Get a simple representation of the node."""
        name, loc = self.__class__.__name__, getattr(self, "loc", None)
        return f"{name} at {loc}" if loc else name

    def __eq__(self, other):
        """Test whether two nodes are equal (recursively)."""
        return (
            isinstance(other, Node)
            and self.__class__ == other.__class__
            and all(getattr(self, key) == getattr(other, key) for key in self.keys)
        )

    def __hash__(self):
        return hash(tuple(getattr(self, key) for key in self.keys))

    def __copy__(self):
        """Create a shallow copy of the node."""
        return self.__class__(**{key: getattr(self, key) for key in self.keys})

    def __deepcopy__(self, memo):
        """Create a deep copy of the node"""
        # noinspection PyArgumentList
        return self.__class__(
            **{key: deepcopy(getattr(self, key), memo) for key in self.keys}
        )

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        name = cls.__name__
        if name.endswith("Node"):
            name = name[:-4]
        cls.kind = camelcase_to_underscores(name)
        keys = []
        for base in cls.__bases__:
            # noinspection PyUnresolvedReferences
            keys.extend(base.keys)  # type: ignore
        keys.extend(cls.__slots__)
        cls.keys = keys


class ItemNode(Node):
    __slots__ = ("attributes",)


class AttributeNode(Node):
    __slots__ = ("name", "value")


class NumberAttributeNode(Node):
    __slots__ = ("name", "value")


class StringAttributeNode(Node):
    __slots__ = ("name", "value")


class BinaryAttributeNode(Node):
    __slots__ = ("name", "value")


class MapAttributeNode(Node):
    __slots__ = (
        "name",
        "attributes",
    )


class AttributeValueNode(Node):
    __slots__ = ("type", "data")


class AttributeNameNode(Node):
    __slots__ = ("value",)


class ValueNode(Node):
    __slots__ = ("value",)


class NumberValueNode(ValueNode):
    __slots__ = ("value",)


class StringValueNode(ValueNode):
    __slots__ = ("value",)


class ListValueNode(ValueNode):
    __slots__ = ("values",)


class StringSetValueNode(ValueNode):
    __slots__ = ("values",)
