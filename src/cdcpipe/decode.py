"""Turns Debezium change events (JSON converter, schemas enabled) into typed rows.

A Debezium value looks like
    {"schema": {...}, "payload": {"before": {...}, "after": {...}, "op": "c|u|d|r",
                                  "source": {"file": "mysql-bin.000003", "pos": 990, "row": 0,
                                             "ts_us": ..., "gtid": ..., "snapshot": ...}}}

The Kafka Connect schema travels with every message, so the sink always knows the exact column
list and types of the row version it is applying, including columns added after it started.

Source position
---------------
(binlog file number, pos, row) strictly increases through the binlog: each row event inside a
transaction has its own `pos`, and rows within one multi-row event are numbered by `row`. The
sink stores it on every target row and only applies an event whose position is greater than the
stored one. That makes replays (after a crash, a rebalance or a Debezium restart) no-ops, and
stops an older event from overwriting a newer one.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import orjson

EPOCH = datetime(1970, 1, 1)
EPOCH_DATE = date(1970, 1, 1)

# Connect logical type name -> PostgreSQL type
_LOGICAL = {
    "io.debezium.time.Date": "date",
    "io.debezium.time.Timestamp": "timestamp(3)",
    "io.debezium.time.MicroTimestamp": "timestamp(6)",
    "io.debezium.time.NanoTimestamp": "timestamp(6)",
    "io.debezium.time.ZonedTimestamp": "timestamptz",
    "io.debezium.time.MicroTime": "time(6)",
    "io.debezium.time.Year": "integer",
    "io.debezium.data.Json": "jsonb",
    "io.debezium.data.Enum": "text",
}
_PRIMITIVE = {
    "int8": "smallint",
    "int16": "smallint",
    "int32": "integer",
    "int64": "bigint",
    "float32": "real",
    "float64": "double precision",
    "boolean": "boolean",
    "string": "text",
    "bytes": "bytea",
}


@dataclass(frozen=True)
class Field:
    name: str
    pg_type: str
    logical: str | None
    params: tuple[tuple[str, str], ...] = ()

    def convert(self, v: Any) -> Any:
        if v is None:
            return None
        lt = self.logical
        if lt == "org.apache.kafka.connect.data.Decimal":
            unscaled = int.from_bytes(base64.b64decode(v), "big", signed=True)
            return Decimal(unscaled).scaleb(-int(dict(self.params)["scale"]))
        if lt == "io.debezium.time.MicroTimestamp":
            return EPOCH + timedelta(microseconds=v)
        if lt == "io.debezium.time.Timestamp":
            return EPOCH + timedelta(milliseconds=v)
        if lt == "io.debezium.time.NanoTimestamp":
            return EPOCH + timedelta(microseconds=v // 1000)
        if lt == "io.debezium.time.Date":
            return EPOCH_DATE + timedelta(days=v)
        if lt == "io.debezium.time.MicroTime":
            return (EPOCH + timedelta(microseconds=v)).time()
        if self.pg_type == "bytea":
            return base64.b64decode(v)
        return v


def field_from_schema(f: dict[str, Any]) -> Field:
    name = f.get("name")
    params = tuple(sorted((f.get("parameters") or {}).items()))
    if name == "org.apache.kafka.connect.data.Decimal":
        p = dict(params)
        prec = p.get("connect.decimal.precision")
        pg = f"numeric({prec},{p['scale']})" if prec else "numeric"
    elif name in _LOGICAL:
        pg = _LOGICAL[name]
    else:
        pg = _PRIMITIVE[f["type"]]
    return Field(f["field"], pg, name, params)


@dataclass(frozen=True)
class TableSchema:
    fields: tuple[Field, ...]
    key: tuple[str, ...]

    @property
    def names(self) -> list[str]:
        return [f.name for f in self.fields]


@dataclass
class Event:
    table: str
    op: str  # c, u, d, r (snapshot read)
    key: tuple[Any, ...]
    row: dict[str, Any]  # after image, or before image for deletes
    position: tuple[int, int, int]  # (binlog file number, pos, row)
    commit_us: int  # source commit time, microseconds since epoch
    schema: TableSchema
    partition: int
    offset: int

    @property
    def deleted(self) -> bool:
        return self.op == "d"


_schema_cache: dict[bytes, TableSchema] = {}


def _table_schema(value_schema: dict[str, Any], key_fields: tuple[str, ...]) -> TableSchema:
    after = next(f for f in value_schema["fields"] if f["field"] == "after")
    return TableSchema(tuple(field_from_schema(f) for f in after["fields"]), key_fields)


def table_of(topic: str) -> str:
    return topic.rsplit(".", 1)[1]


def decode(
    topic: str, key: bytes | None, value: bytes | None, partition: int, offset: int
) -> Event | None:
    """Returns None for tombstones (the null-valued message Debezium sends after a delete so that
    log compaction can drop the key): the delete event before it already carried the change."""
    if value is None:
        return None
    msg = orjson.loads(value)
    payload = msg["payload"]
    op = payload["op"]
    if op not in ("c", "u", "d", "r"):
        raise ValueError(f"unsupported op {op!r} at {topic}:{partition}:{offset}")
    key_msg = orjson.loads(key) if key else None
    key_fields = tuple(f["field"] for f in key_msg["schema"]["fields"]) if key_msg else ()

    # The schema is identical for every message of a table version: build it once, keyed on the
    # raw bytes of the schema part (the JSON converter always writes "schema" before "payload").
    cut = value.find(b',"payload":')
    cache_key = value[:cut] + repr(key_fields).encode()
    ts = _schema_cache.get(cache_key)
    if ts is None:
        ts = _schema_cache[cache_key] = _table_schema(msg["schema"], key_fields)

    image = payload["before"] if op == "d" else payload["after"]
    row = {f.name: f.convert(image.get(f.name)) for f in ts.fields}
    src = payload["source"]
    file_no = int(src["file"].rsplit(".", 1)[1])
    return Event(
        table=table_of(topic),
        op=op,
        key=tuple(row[k] for k in ts.key),
        row=row,
        position=(file_no, int(src["pos"]), int(src.get("row") or 0)),
        commit_us=int(src.get("ts_us") or src["ts_ms"] * 1000),
        schema=ts,
        partition=partition,
        offset=offset,
    )


def utc_from_us(us: int) -> datetime:
    return datetime.fromtimestamp(us / 1e6, tz=UTC)
