from .ast import (
    ItemNode,
    AttributeValueNode,
    MapAttributeNode,
    AttributeNameNode,
    NumberAttributeNode,
    AttributeNode,
    ListValueNode,
    StringValueNode,
    StringSetValueNode,
    NumberValueNode,
    StringAttributeNode,
)

# This is obviously horrible...but it works.   Might want to model it after
# the botocore parsers, although we won't have access to shape data, so might
# have to do a lot of isinstance...


def item_parser(item):
    def parse(item):
        if isinstance(item, dict):
            parsed = {
                k: (list(v.keys())[0], list(v.values())[0],) for k, v in item.items()
            }
            attributes = []
            for k, v in parsed.items():
                type_ = v[0]
                data = v[1]
                name_node = AttributeNameNode(value=k)
                if type_ == "M":
                    attribute = MapAttributeNode(name=name_node, attributes=parse(data))
                elif type_ == "N":
                    attribute = NumberAttributeNode(name=name_node, value=parse(v))
                elif type_ == "S":
                    attribute = StringAttributeNode(name=name_node, value=parse(v))
                else:
                    attribute = AttributeNode(name=name_node, value=parse(v))
                attributes.append(attribute)
            parsed = attributes
        elif isinstance(item, tuple):
            type_ = item[0]
            data = item[1]
            if type_ == "L":
                data = [(k, v,) for d in data for k, v in d.items()]
                parsed = AttributeValueNode(
                    type="L", data=ListValueNode(values=[parse(i) for i in data])
                )
            elif type_ == "SS":
                parsed = AttributeValueNode(
                    type="SS", data=StringSetValueNode(values=[str(i) for i in data]),
                )
            elif type_ == "M":
                attributes = parse(data)
                parsed = MapAttributeNode(attributes=attributes)
            elif type_ == "N":
                parsed = AttributeValueNode(
                    type=type_, data=NumberValueNode(value=data)
                )
            else:
                parsed = AttributeValueNode(type=type_, data=parse(data))
        elif isinstance(item, list):
            parsed = [parse(i) for i in item]
        else:
            # Scalar
            parsed = StringValueNode(value=item)
        return parsed

    parsed = ItemNode(attributes=parse(item))
    return parsed
