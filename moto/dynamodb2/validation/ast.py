from copy import deepcopy

import six

from moto.core.utils import camelcase_to_underscores


# Base AST Node
class Node(object):
    """AST nodes"""

    # allow custom attributes and weak references (not used internally)
    # BP: I'm dropping this because it's breaking Python2...
    # Ref: https://github.com/graphql-python/graphql-core/pull/82/files
    __slots__ = ()  # "__dict__", "__weakref__"

    kind = "ast"  # the kind of the node as a snake_case string
    keys = []  # the names of the attributes of this node

    def __init__(self, **kwargs):
        """Initialize the node with the given keyword arguments."""
        super(Node, self).__init__()
        if six.PY2:
            self.__class__.__init_subclass__()
        for key in self.keys:
            value = kwargs.get(key)
            setattr(self, key, value)

    def __repr__(self):
        """Get a simple representation of the node."""
        name = self.__class__.__name__
        return "{name}".format(name=name)

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

    # Python 3 only.
    @classmethod
    def __init_subclass__(cls):
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
