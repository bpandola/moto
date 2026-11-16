---
name: migrate-moto-parser-serializer
description: Migrate a Moto service backend to the automated parser/serializer pattern. Use when asked to refactor a Moto service's responses.py to use automated_parameter_parsing + self._get_param() for input, ActionResult/EmptyResult/PaginatedResult for output, and to move/remove model to_json() methods. Covers the botocore-model introspection needed to get member names and types right.
---

# Migrate a Moto backend to the parser/serializer pattern

This skill captures the end-to-end process for moving one Moto service from
hand-rolled request parsing (`json.loads(self.body)`, `self.path.split("/")`,
`self.querystring.get(...)`) and hand-rolled responses (`json.dumps(...)`) to
the automated botocore-model-driven parser and serializer.

The migration has three independent phases. Each phase is a clean commit and
should be verified before moving on. Do them in order; later phases assume
earlier ones.

1. **Input parsing** — enable `automated_parameter_parsing`, replace all manual
   parsing with `self._get_param()`.
2. **Output serialization** — return `ActionResult` / `EmptyResult` /
   `PaginatedResult` instead of `json.dumps(...)`; move `to_json()` dict-building
   out of `models.py`.
3. **Type fidelity** — make typed members (bool, timestamp, blob) use the right
   Python types end-to-end.

Scope each request to what the user asked. Phase 1 alone is a valid task; don't
do phase 2/3 unless asked.

## Before you start: introspect the botocore model

Everything keys off the botocore service model. The member names you pass to
`_get_param()` and the keys in your result dicts are **botocore member names**,
not the regex group names in `urls.py`. Get them from the model, don't guess.

`boto3_service_name` may differ from the moto folder name (e.g. `emr-containers`
vs `emrcontainers`). Use the boto3 name in the snippets below.

List every operation's HTTP method, URI, members, and where each member lives
(uri / querystring / body / header):

```python
from botocore.session import Session
m = Session().get_service_model("<boto3-service-name>")
for op_name in m.operation_names:
    op = m.operation_model(op_name)
    inp = op.input_shape
    locs = {}
    if inp:
        for n, s in inp.members.items():
            loc = s.serialization.get("location")
            if loc:
                locs[n] = (loc, s.serialization.get("name"))
    print(op.http["method"], op.http["requestUri"], op_name)
    print("   members:", list(inp.members.keys()) if inp else [])
    print("   located:", locs)  # uri / querystring / header members
```

Key facts this reveals:
- **Path params**: members with `location == "uri"`. Their member name is what
  you pass to `_get_param()` (e.g. `apiId`, `id`, `resourceArn`). The parser
  URL-decodes them automatically.
- **Querystring params**: `location == "querystring"`. `_get_param()` returns
  the parsed value; list-typed querystrings come back as a list.
- **Body params**: no `location`. Standard `_get_param("memberName")`.

## Phase 1 — Input parsing

In `responses.py`:

1. In `__init__`, after `super().__init__(...)`, add:
   ```python
   self.automated_parameter_parsing = True
   ```
2. Replace every parsing idiom with `self._get_param("<botocoreMemberName>")`:
   - `params = json.loads(self.body)` + `params.get("x")` → `self._get_param("x")`
   - `self.path.split("/")[-1]` → `self._get_param("<uriMemberName>")`
   - `self.querystring.get("x")[0]` → `self._get_param("x")`
   - Custom ARN/path extraction helpers → `self._get_param("<uriMemberName>")`
     (drop the helper; the parser handles slash-containing greedy uri params).
3. Preserve existing defaults: `self._get_param("tags", {})`,
   `self._get_param("xrayEnabled", False)`, etc.
4. Remove now-unused imports (`unquote`, custom helpers). Keep `json` if a
   non-dispatch static handler still uses it.

How it works: with `automated_parameter_parsing = True`, `setup_class` calls
`parse_parameters`, which uses the botocore parser to populate `self.params`
from the body, uri params, querystring, and headers. `_get_param(name)` then
reads `self.params` by member name. `self.uri_match` is set from botocore's URI
pattern (via `_get_method_urls`), **not** from the service's `urls.py` — so the
`urls.py` regex group names are irrelevant to parsing and you don't touch that
file.

### Blob gotcha (catches you in phase 1)

A member typed `blob` is **base64-decoded to `bytes` by the parser**. If the old
code passed the raw base64 string to the model (which then `base64.b64decode`d
it), that double-decode now breaks. Fix the model to consume the decoded bytes
(e.g. `definition.decode("utf-8")`) and update its type hint to `bytes`. Drop the
now-unused `base64` import.

Find blob members:
```python
for op_name in m.operation_names:
    op = m.operation_model(op_name)
    if op.input_shape:
        for n, s in op.input_shape.members.items():
            if s.type_name == "blob":
                print(op_name, n)
```

## Phase 2 — Output serialization

Return result wrappers instead of dumping JSON. `call_action` serializes them
via the operation's **output shape**, picking values from your result by member
name.

- `ActionResult({...})` — a dict whose top-level keys are the output shape's
  member names. Nested structures are dicts keyed by their member names too.
- `EmptyResult()` — for actions that returned `"{}"`.
- `PaginatedResult({...})` — only if you're wiring up pagination (separate
  concern; don't add it unless asked — Moto list ops are often unpaginated).

Import: `from moto.core.responses import ActionResult, BaseResponse, EmptyResult`

How value-picking works: the serializer walks the output shape and, for each
member, looks up the key in your result dict. A plain dict with **camelCase keys
matching the member names** works directly (this is exactly what the old
`to_json()` produced). For model *objects*, the `XFormedAttributePicker` also
tries snake_case attributes — but per the pattern here we build dicts, so keep
keys as the literal output member names.

### Moving to_json() out of models.py

The user's preference (follow it): **inline** the dict in each response method
into a `result` variable, then `return ActionResult(result)`. Delete the
`to_json()` methods from `models.py`. Build tag lookups on the response side
(`self.<svc>_backend.list_tags_for_resource(arn)`).

```python
def get_thing(self) -> ActionResult:
    thing_id = self._get_param("thingId")
    thing = self.<svc>_backend.get_thing(thing_id=thing_id)
    result = {
        "thing": {
            "thingId": thing.id,
            "name": thing.name,
            "tags": self.<svc>_backend.list_tags_for_resource(thing.arn),
        }
    }
    return ActionResult(result)
```

For list endpoints with conditional keys, build the item dict (annotate
`dict[str, Any]`), conditionally add optional keys, then append — a list
comprehension can't do conditional keys.

Verify which output members exist (the serializer ignores extra dict keys and
omits members whose value is `None`):
```python
op = m.operation_model("<Operation>")
out = op.output_shape
for n, s in out.members.items():
    print(n, s.type_name)
    if s.type_name == "structure":
        for nn, ss in s.members.items():
            print("   ", nn, ss.type_name)
```

### Payload members (streaming body)

If `op.output_shape.serialization.get("payload")` names a `blob`/`string`
member, that member is delivered as the **raw HTTP body** (boto3 reads it via
`StreamingBody.read()`), not wrapped in JSON. Just put the value under that
member key in the result; the serializer emits it raw:
```python
return ActionResult({"schema": schema_text})  # GetIntrospectionSchema
```

## Phase 3 — Type fidelity

Make typed members use the right Python types so the serializer round-trips
them correctly and signatures are honest.

### Booleans

Read boolean *input* members with `self._get_bool_param("name", default)` and
type the corresponding model fields/params as `bool` (not `str`). Find them:
```python
def walk(shape, path, depth=0, seen=None):
    seen = seen or set()
    if id(shape) in seen or depth > 6:
        return
    seen.add(id(shape))
    if shape.type_name == "structure":
        for n, s in shape.members.items():
            if s.type_name == "boolean":
                print(f"{path}.{n}")
            walk(s, f"{path}.{n}", depth + 1, seen)
    elif shape.type_name == "list":
        walk(shape.member, path + "[]", depth + 1, seen)
    elif shape.type_name == "map":
        walk(shape.value, path + "{}", depth + 1, seen)
for op_name in m.operation_names:
    op = m.operation_model(op_name)
    if op.input_shape:
        walk(op.input_shape, op_name)
```
Only top-level members are read individually; booleans nested inside a struct
member (e.g. `logConfig.excludeVerboseContent`) ride along inside the struct dict
from `_get_param("logConfig")` and need no special handling. Ignore members of
unimplemented operations.

### Timestamps vs longs (important distinction)

Run the same `walk` looking for `s.type_name == "timestamp"`.

- A **`timestamp`** member: store it as a native `datetime.datetime` in the
  model (not an isoformat string). The serializer converts datetime → wire format
  via `parse_to_aware_datetime`, so just place the `datetime` in the result dict.
  Type model signatures `datetime.datetime | None`.
- A **`long`** member that *represents* a time (e.g. API key `expires`/`deletes`,
  epoch seconds) is **not** a `timestamp`. The `long` serializer does
  `int(value)`, which raises on a `datetime`. Confirm the type — don't assume a
  time-ish name means `timestamp`.

When the team's convention is "store times as datetime in models" even for these
epoch `long` fields:
- Type the model param `datetime.datetime | None`, store a native (naive UTC)
  `datetime`.
- Convert at the `responses.py` boundary: incoming epoch → datetime with
  `utcfromtimestamp(...)`; outgoing datetime → epoch int in the result dict with
  `int(unix_time(dt))`.
- Keep `datetime`s naive UTC: `unix_time(dt)` subtracts a naive epoch and raises
  on aware datetimes; `utcfromtimestamp` already returns naive UTC. This keeps
  the integer round-trip exact (`int(unix_time(utcfromtimestamp(x))) == x`).
- Remove `unix_time` usage from `models.py`; it now lives only in `responses.py`
  (result-dict conversions + any current-time validation checks).

## Phase 4 — Exceptions

Only `ServiceException` subclasses go through the serializer
(`BaseResponse.call_action`). `JsonRESTError`/`AWSError` are werkzeug
`HTTPException`s with a hand-built JSON body, so they bypass it (and would never
be CBOR-encoded).

- Give each backend one base class, then subclass it per error code:

  ```python
  from moto.core.exceptions import ServiceException


  class ShieldException(ServiceException):
      pass


  class ResourceNotFoundException(ShieldException):
      code = "ResourceNotFoundException"
  ```

- Base class naming: if `exceptions.py` already has a generic base class (e.g.
  `CodeDeployException`, `MemoryDBClientError`), keep its name and just
  reparent it to `ServiceException`. If it has none, name the new one
  `<ServiceName>Exception` (not `<ServiceName>Error`).
- Keep the diff small: keep existing concrete class names and call sites;
  fix only `code` when it's wrong.
- `code` must be an error shape in the botocore model (unmodeled codes still
  serialize but log a warning). Fix invented codes (e.g. memorydb
  `InvalidSubnetError` → `InvalidSubnet`) and update the tests that assert them.
- HTTP status comes from the model's error shape; drop hardcoded `code = 400`.
- **Silent message loss:** `ServiceException.__init__` treats kwargs as
  `message.format(**kwargs)` fields. `raise X(msg="...")` doesn't fail; it sends
  "An unspecified service error occurred". Pass the message positionally, and
  check multi-line `raise X(\n    msg=...)` calls too. Tests rarely catch this,
  so compare raw error responses (status, `X-Amzn-Errortype`, body) against
  master for a handful of errors.
- Extra error members (e.g. `ResourceId`) are set as attributes on the
  exception; the serializer picks them up from the error shape.
- Exceptions raised bare (`raise X` / `raise X()`) need a class-level
  `message = "..."`, or they send the generic unspecified message.
- Generic codes such as `ValidationException` are fine without a model shape
  if they're in `COMMON_ERROR_CODES` (moto/core/errors.py).
- Services with a catch-all client error (e.g. directconnect's
  `DirectConnectClientException`) should use it instead of invented codes.

## Verify (every phase)

```bash
python -m pytest tests/test_<service>/ -q
ruff check moto/<service>/
ruff format moto/<service>/
python -m mypy moto/<service>/
```

All four must be clean. If `ruff format` reformats, re-run the check. Inspect the
diff before committing; commit each phase separately with a focused message.

## Quick gotcha checklist

- Member names come from the **botocore model**, not `urls.py`. Don't touch
  `urls.py`.
- `blob` input → decoded `bytes` (fix double-decode in the model).
- `blob`/`string` payload output → raw body, not JSON-wrapped.
- Result dict keys = output **member names** (camelCase); extra keys ignored,
  `None` values omitted.
- `_get_bool_param` for booleans; model fields typed `bool`.
- `timestamp` ≠ `long`. Check the actual type before treating something as a
  datetime on the wire.
- Keep model `datetime`s naive UTC for `unix_time`/`utcfromtimestamp` round-trips.
- Don't add pagination unless asked.
- A standalone static handler (mapped directly in `urls.py`, not via
  `dispatch`/action) is out of scope — leave its `json.loads`/`json.dumps`.

## Reference

A fully migrated example: `moto/emrcontainers/responses.py` and
`moto/medialive/responses.py` (note: medialive passes model objects to
`ActionResult` and relies on attribute-picking; this skill prefers explicit
dicts). The AppSync migration (`moto/appsync/`) exercised all three phases
including the blob, payload, bool, and timestamp-vs-long gotchas.
