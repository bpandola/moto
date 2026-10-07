"""Round-trip tests for server_pagination.ServerPaginator.

Each test wires a ServerPaginator in as the operation method behind botocore's
client-side Paginator and checks that build_full_result() reproduces the full,
unpaginated data.
"""

import random
from typing import Any, Optional

import botocore.session
import pytest
from botocore.paginate import Paginator

from moto.core.paginate import (
    PaginationServerError,
    ServerPaginationConfig,
    ServerPaginator,
)

SESSION = botocore.session.get_session()


def client_cfg(service: str, version: str, op: str) -> Any:
    # Loaded through botocore so sdk-extras files are merged in.
    loader = SESSION.get_component("data_loader")
    model = loader.load_service_model(service, "paginators-1", version)
    return model["pagination"][op]


def drive(
    service: str,
    version: str,
    op: str,
    full: dict[str, Any],
    server_config: Optional[dict[str, Any]] = None,
    model: Any = None,
    **pconf: Any,
) -> tuple[Any, list[dict[str, Any]]]:
    cfg = client_cfg(service, version, op)
    server = ServerPaginator(
        ServerPaginationConfig.from_botocore(op, cfg, server_config, model)
    )
    calls: list[dict[str, Any]] = []

    def method(**kwargs: Any) -> dict[str, Any]:
        calls.append(dict(kwargs))
        return server.paginate(full, kwargs)

    op_model = SESSION.get_service_model(service).operation_model(op)
    paginator: Paginator[Any] = Paginator(method, cfg, op_model)  # type: ignore[arg-type]
    return paginator.paginate(PaginationConfig=pconf), calls


def keys(n: int) -> list[str]:
    return [f"k{i:04d}" for i in range(n)]


@pytest.mark.parametrize("page_size", [1, 3, 7, 50, 1000])
def test_opaque_next_token_roundtrip(page_size: int) -> None:
    items = [{"Arn": k} for k in keys(23)]
    full = {"Items": items}
    result = drive(
        "oam",
        "2022-06-10",
        "ListAttachedLinks",
        full,
        PageSize=page_size,
    )[0].build_full_result()
    assert result["Items"] == items


def test_max_items_resume_with_starting_token() -> None:
    items = [{"Arn": k} for k in keys(23)]
    full = {"Items": items}
    seen: list[Any] = []
    token = None
    while True:
        pages, _ = drive(
            "oam",
            "2022-06-10",
            "ListAttachedLinks",
            full,
            PageSize=4,
            MaxItems=5,
            StartingToken=token,
        )
        result = pages.build_full_result()
        seen.extend(result["Items"])
        token = result.get("NextToken")
        if not token:
            break
    assert seen == items


def test_bound_params_reject_changed_filter() -> None:
    full = {"Items": [{"Arn": k} for k in keys(10)]}
    config = ServerPaginationConfig.from_botocore(
        "ListAttachedLinks",
        client_cfg("oam", "2022-06-10", "ListAttachedLinks"),
        {"bound_params": ["SinkIdentifier"]},
    )
    server = ServerPaginator(config)
    page = server.paginate(full, {"SinkIdentifier": "a", "MaxResults": 3})
    with pytest.raises(PaginationServerError):
        server.paginate(full, {"SinkIdentifier": "b", "NextToken": page["NextToken"]})
    with pytest.raises(PaginationServerError):
        server.paginate(full, {"NextToken": "garbage"})


def test_page_size_derived_from_model() -> None:
    model = SESSION.get_service_model("oam").operation_model("ListAttachedLinks")
    config = ServerPaginationConfig.from_botocore(
        "ListAttachedLinks",
        client_cfg("oam", "2022-06-10", "ListAttachedLinks"),
        {"page_size_policy": "error"},
        model,
    )
    assert config.max_page_size == 1000
    assert config.invalid_token_error["code"] == "InvalidNextTokenException"
    with pytest.raises(PaginationServerError):
        ServerPaginator(config).paginate({"Items": []}, {"MaxResults": 5000})


def test_s3_list_objects_merged_marker() -> None:
    contents = [{"Key": f"file{i:03d}"} for i in range(0, 40, 2)]
    prefixes = [{"Prefix": f"file{i:03d}/"} for i in range(1, 40, 6)]
    full = {"Contents": contents, "CommonPrefixes": prefixes, "Name": "b"}
    server_config = {
        "multi_result_mode": "merged",
        "result_key_attributes": {
            "Contents": "Key",
            "CommonPrefixes": "Prefix",
        },
        "echo_params": ["Marker", "MaxKeys"],
    }
    pages, calls = drive(
        "s3", "2006-03-01", "ListObjects", full, server_config, PageSize=4
    )
    result = pages.build_full_result()
    assert result["Contents"] == contents
    assert result["CommonPrefixes"] == prefixes
    # Contents and CommonPrefixes share the MaxKeys budget.
    assert len(calls) == -(-(len(contents) + len(prefixes)) // 4)


def test_s3_list_objects_v2_opaque_merged() -> None:
    contents = [{"Key": k} for k in keys(9)]
    prefixes = [{"Prefix": k + "/"} for k in keys(5)]
    full = {"Contents": contents, "CommonPrefixes": prefixes}
    server_config = {
        "multi_result_mode": "merged",
        "result_key_attributes": {
            "Contents": "Key",
            "CommonPrefixes": "Prefix",
        },
    }
    result = drive(
        "s3", "2006-03-01", "ListObjectsV2", full, server_config, PageSize=3
    )[0].build_full_result()
    assert result["Contents"] == contents
    assert result["CommonPrefixes"] == prefixes


def test_route53_three_part_token_at_next() -> None:
    records: list[dict[str, str]] = []
    for name in ["a.example.com.", "b.example.com.", "c.example.com."]:
        for rtype in ["A", "AAAA", "TXT"]:
            for ident in [None, "east", "west"]:
                rec = {"Name": name, "Type": rtype}
                if ident:
                    rec["SetIdentifier"] = ident
                records.append(rec)
    random.Random(0).shuffle(records)
    full = {"ResourceRecordSets": records, "MaxItems": "5"}
    server_config = {
        "token_strategy": "marker",
        "token_attributes": ["Name", "Type", "SetIdentifier"],
        "token_position": "at_next",
    }
    result = drive(
        "route53",
        "2013-04-01",
        "ListResourceRecordSets",
        full,
        server_config,
        PageSize=4,
    )[0].build_full_result()

    def key(record: dict[str, str]) -> tuple[str, str, str]:
        return (
            record["Name"],
            record["Type"],
            record.get("SetIdentifier", ""),
        )

    assert result["ResourceRecordSets"] == sorted(records, key=key)


def test_kinesis_derived_token_and_nested_paths() -> None:
    shards = [{"ShardId": f"shardId-{i:012d}"} for i in range(11)]
    full = {
        "StreamDescription": {
            "StreamName": "s",
            "StreamStatus": "ACTIVE",
            "Shards": shards,
        }
    }
    pages, calls = drive("kinesis", "2013-12-02", "DescribeStream", full, PageSize=3)
    result = pages.build_full_result()
    assert result["StreamDescription"]["Shards"] == shards
    assert result["StreamDescription"]["StreamName"] == "s"
    assert calls[1]["ExclusiveStartShardId"] == shards[2]["ShardId"]


def test_cloudfront_nested_tokens_and_more_results() -> None:
    items = [{"Id": k} for k in keys(10)]
    full = {"DistributionList": {"Items": items, "Quantity": 10}}
    pages, calls = drive(
        "cloudfront", "2020-05-31", "ListDistributions", full, PageSize=4
    )
    result = pages.build_full_result()
    assert result["DistributionList"]["Items"] == items
    assert len(calls) == 3


def test_dynamodb_map_token_and_count_keys() -> None:
    items = [{"pk": {"S": f"p{i % 3}"}, "sk": {"N": str(i)}} for i in range(14)]
    full = {"Items": items, "ConsumedCapacity": {"TableName": "t"}}
    server_config: dict[str, Any] = {
        "token_strategy": "marker",
        "token_attributes": [["pk", "sk"]],
        "marker_match": "exact",
        "count_keys": {"Count": "Items", "ScannedCount": "Items"},
    }
    result = drive("dynamodb", "2012-08-10", "Scan", full, server_config, PageSize=4)[
        0
    ].build_full_result()
    assert result["Items"] == items
    assert result["Count"] == 14
    assert result["ScannedCount"] == 14
    assert result["ConsumedCapacity"] == {"TableName": "t"}


def test_s3_list_parts_integer_marker() -> None:
    parts = [{"PartNumber": n, "Size": 5} for n in range(1, 12)]
    full = {"Parts": parts, "Bucket": "b", "Owner": {"ID": "x"}}
    server_config = {
        "token_strategy": "marker",
        "token_attributes": ["PartNumber"],
    }
    result = drive("s3", "2006-03-01", "ListParts", full, server_config, PageSize=3)[
        0
    ].build_full_result()
    assert result["Parts"] == parts
    assert result["Owner"] == {"ID": "x"}


def test_every_shipped_config_builds() -> None:
    loader = SESSION.get_component("data_loader")
    built = 0
    for service in SESSION.get_available_services():
        try:
            pagination = loader.load_service_model(service, "paginators-1")
        except Exception:
            continue
        for op, cfg in pagination["pagination"].items():
            ServerPaginationConfig.from_botocore(op, cfg)
            built += 1
    assert built > 3000
