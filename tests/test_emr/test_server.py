import moto.server as server

"""
Test the different server responses
"""


def test_describe_jobflows():
    backend = server.create_backend_app("emr")
    test_client = backend.test_client()
    headers = {
        "Content-Type": "application/x-amz-json-1.1",
        "X-Amz-Target": "ElasticMapReduce.DescribeJobFlows",
    }
    res = test_client.post("/", headers=headers)
    assert b"JobFlows" in res.data
