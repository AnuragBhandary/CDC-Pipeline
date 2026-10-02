"""Builds Debezium-shaped messages (JSON converter, schemas enabled) for tests, in exactly the
shape the real connector produced in the probe recorded in docs/DESIGN.md."""

from __future__ import annotations

import base64
from decimal import Decimal
from typing import Any

import orjson

ID = {"type": "int64", "optional": False, "field": "id"}
NAME = {"type": "string", "optional": False, "field": "name"}
PRICE = {
    "type": "bytes",
    "optional": False,
    "name": "org.apache.kafka.connect.data.Decimal",
    "parameters": {"scale": "2", "connect.decimal.precision": "10"},
    "field": "price",
}
UPDATED = {
    "type": "int64",
    "optional": False,
    "name": "io.debezium.time.MicroTimestamp",
    "field": "updated_at",
}
BORN = {"type": "int32", "optional": True, "name": "io.debezium.time.Date", "field": "born"}
TIER = {"type": "string", "optional": True, "field": "tier"}
FIELDS = [ID, NAME, PRICE, UPDATED, BORN]


def dec(v: str, scale: int = 2) -> str:
    unscaled = int(Decimal(v).scaleb(scale))
    n = max(1, (unscaled.bit_length() + 8) // 8)
    return base64.b64encode(unscaled.to_bytes(n, "big", signed=True)).decode()


def row(
    i: int, name: str = "a", price: str = "1.00", tier: str | None = None, with_tier: bool = False
) -> dict[str, Any]:
    r: dict[str, Any] = {
        "id": i,
        "name": name,
        "price": dec(price),
        "updated_at": 1_790_000_000_000_000 + i,
        "born": 20455,
    }
    if with_tier:
        r["tier"] = tier
    return r


def message(
    table: str,
    op: str,
    before: dict[str, Any] | None,
    after: dict[str, Any] | None,
    pos: int,
    row_no: int = 0,
    file_no: int = 3,
    fields: list[dict[str, Any]] | None = None,
    commit_us: int = 1_790_000_000_000_000,
) -> tuple[str, bytes, bytes]:
    fields = fields or FIELDS
    struct = {"type": "struct", "fields": fields, "optional": True, "name": f"x.shop.{table}.Value"}
    value = {
        "schema": {
            "type": "struct",
            "fields": [
                {**struct, "field": "before"},
                {**struct, "field": "after"},
                {"type": "struct", "fields": [], "optional": False, "field": "source"},
                {"type": "string", "optional": False, "field": "op"},
            ],
            "optional": False,
            "name": f"x.shop.{table}.Envelope",
        },
        "payload": {
            "before": before,
            "after": after,
            "op": op,
            "source": {
                "file": f"mysql-bin.{file_no:06d}",
                "pos": pos,
                "row": row_no,
                "ts_us": commit_us,
                "ts_ms": commit_us // 1000,
            },
        },
    }
    rid = (after or before or {})["id"]
    key = {"schema": {"type": "struct", "fields": [ID], "optional": False}, "payload": {"id": rid}}
    return f"x.shop.{table}", orjson.dumps(key), orjson.dumps(value)
