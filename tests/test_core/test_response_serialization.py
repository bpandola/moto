from moto.core.model import ServiceModel
from moto.core.responses import ActionContext, ActionResult, BaseResponse, EmptyResult
from moto.core.serialize import QuerySerializer, never_return

TEST_MODEL = {
    "metadata": {"protocol": "query", "apiVersion": "2014-01-01"},
    "documentation": "",
    "operations": {
        "TestOperation": {
            "name": "TestOperation",
            "http": {
                "method": "POST",
                "requestUri": "/",
            },
            "output": {"shape": "OutputShape"},
        }
    },
    "shapes": {
        "OutputShape": {
            "type": "structure",
            "members": {
                "string": {"shape": "StringType"},
                "TransformerTest": {"shape": "StringType"},
                "Tags": {"shape": "TagList"},
            },
        },
        "StringType": {
            "type": "string",
        },
        "Tag": {
            "type": "structure",
            "members": {
                "Key": {
                    "shape": "StringType",
                },
                "Value": {
                    "shape": "StringType",
                },
            },
        },
        "TagList": {
            "type": "list",
            "member": {"shape": "Tag", "locationName": "Tag"},
        },
    },
}


def transform_tags(tags: dict[str, str]) -> list[dict[str, str]]:
    """Transform a dictionary of tags into a list of dictionaries."""
    return [{"Key": key, "Value": value} for key, value in tags.items()]


def test_response_result_execution() -> None:
    """Exercise various response result execution paths."""

    class TestResponseClass(BaseResponse):
        RESPONSE_KEY_PATH_TO_TRANSFORMER = {
            "OutputShape.TransformerTest": never_return,
            #"OutputShape.Tags": transform_tags,
            "TagList": transform_tags,
        }

    class TestResponseObject:
        string = "test-string"
        TransformerTest = "some data"
        Tags = {"tag1": "value1", "tag2": "value2"}

    service_model = ServiceModel(TEST_MODEL)
    operation_model = service_model.operation_model("TestOperation")
    serializer_class = QuerySerializer
    response_class = TestResponseClass

    context = ActionContext(
        service_model, operation_model, serializer_class, response_class
    )

    result = ActionResult(TestResponseObject())
    _, __, body = result.execute_result(context)
    assert "test-string" in body
    assert "TransformerTest" not in body
    # Assert that the tags are transformed correctly
    assert "<Key>tag1</Key>" in body
    assert "<Value>value1</Value>" in body
    assert "<Key>tag2</Key>" in body
    assert "<Value>value2</Value>" in body

    result = EmptyResult()
    _, __, body = result.execute_result(context)
    assert "<OutputShapeResult/>" in body
