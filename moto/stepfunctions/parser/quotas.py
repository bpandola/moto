from typing import Any

from moto.stepfunctions.parser.asl.utils.encoding import to_json_str

MAX_STATE_SIZE_UTF8_BYTES: int = 256 * 1024  # 256 KB of data as a UTF-8 encoded string.


def is_within_size_quota(value: Any) -> bool:
    item_str = value if isinstance(value, str) else to_json_str(value)
    item_bytes = item_str.encode("utf-8")
    len_item_bytes = len(item_bytes)
    return len_item_bytes < MAX_STATE_SIZE_UTF8_BYTES
