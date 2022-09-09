from moto.motocore.loaders import _load_service_model
from moto.ec2 import ec2_backends
from moto.ec2.models.amis import Ami
from moto.motocore.serialize import create_serializer


def test_query_protocol():
    service_model = _load_service_model('rds')
    operation_model = service_model.operation_model("CreateDBInstance")
    serialized = {
        "uri": "/",
        "headers": {
            "Content-Type": "application/x-www-form-urlencoded; charset=utf-8"
        },
        "body": "Action=CreateDBInstance&Version=2014-01-01&Foo=val1&Bar=val2"
    }

def test_rds_model_error():
#     <ErrorResponse xmlns="http://rds.amazonaws.com/doc/2014-10-31/">
#   <Error>
#     <Type>Sender</Type>
#     <Code>DBInstanceAlreadyExists</Code>
#     <Message>DB instance already exists</Message>
#   </Error>
#   <RequestId>ab7402f8-b684-44ff-8282-7420610c91ff</RequestId>
# </ErrorResponse>
    service_model = _load_service_model('rds')
    operation_model = service_model.operation_model("CreateDBInstance")
    # botocore.errorfactory.DBInstanceAlreadyExistsFault: An error occurred (DBInstanceAlreadyExists) when calling the CreateDBInstance operation: DB instance already exists
    error = service_model.shape_for_error_code('DBInstanceAlreadyExists')
    print('')

def test_ami_serialization():
    model = _load_service_model("ec2")
    backend = ec2_backends["us-east-1"]
    ami = Ami(
        backend,
        ami_id="ami-1234",
        architecture="i386",
        virtualization_type="hvm",
    )
    operation_model = model.operation_model("DescribeImages")
    result_dict = {"Images": [ami, ami]}
    serializer = create_serializer("query")
    serializer.ALIASES.update({"Ami": "Image"})
    serialized = serializer.serialize_object(result_dict, operation_model)
    serialized = serializer.serialize_to_response(result_dict, operation_model)
    print(serialized)


# TODO:
# Test the various ways of getting an attribute
# ClassnameAttribute, camel, snake, etc.
# Should test that if the model wants a string we don't give it a list or whatever.
# Basically type checking the response model.
# https://github.com/spulec/moto/blob/master/moto/ec2/responses/launch_templates.py
