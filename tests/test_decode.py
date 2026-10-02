from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest

from cdcpipe.decode import decode, field_from_schema

from .debezium import message, row


def test_decodes_types_and_position() -> None:
    e = decode(
        *message("items", "c", None, row(7, price="-12.30"), pos=990, row_no=2),
        partition=1,
        offset=42,
    )
    assert e is not None
    assert e.table == "items" and e.op == "c" and e.key == (7,)
    assert e.row["price"] == Decimal("-12.30")
    assert e.row["updated_at"] == datetime(2026, 9, 21, 14, 13, 20, 7)
    assert e.row["born"] == date(2026, 1, 2)
    assert e.position == (3, 990, 2)
    assert (e.partition, e.offset) == (1, 42)


def test_delete_uses_before_image() -> None:
    e = decode(*message("items", "d", row(5, name="gone"), None, pos=10), partition=0, offset=0)
    assert e is not None and e.deleted and e.row["name"] == "gone"


def test_tombstone_is_skipped() -> None:
    topic, key, _ = message("items", "d", row(5), None, pos=10)
    assert decode(topic, key, None, 0, 1) is None


def test_positions_order_across_binlog_files() -> None:
    a = decode(*message("t", "u", None, row(1), pos=99999, file_no=3), partition=0, offset=0)
    b = decode(*message("t", "u", None, row(1), pos=4, file_no=4), partition=0, offset=1)
    assert a is not None and b is not None and a.position < b.position


def test_schema_parsed_once_per_version() -> None:
    a = decode(*message("t", "c", None, row(1), pos=1), partition=0, offset=0)
    b = decode(*message("t", "c", None, row(2), pos=2), partition=0, offset=1)
    assert a is not None and b is not None and a.schema is b.schema


@pytest.mark.parametrize(
    ("schema", "pg"),
    [
        ({"type": "int16", "field": "x"}, "smallint"),
        ({"type": "int64", "field": "x"}, "bigint"),
        ({"type": "float64", "field": "x"}, "double precision"),
        ({"type": "boolean", "field": "x"}, "boolean"),
        (
            {"type": "string", "name": "io.debezium.time.ZonedTimestamp", "field": "x"},
            "timestamptz",
        ),
        ({"type": "string", "name": "io.debezium.data.Json", "field": "x"}, "jsonb"),
        (
            {
                "type": "bytes",
                "name": "org.apache.kafka.connect.data.Decimal",
                "parameters": {"scale": "3"},
                "field": "x",
            },
            "numeric",
        ),
    ],
)
def test_type_mapping(schema: dict[str, object], pg: str) -> None:
    assert field_from_schema(schema).pg_type == pg  # type: ignore[arg-type]


def test_rejects_unknown_ops() -> None:
    topic, key, value = message("t", "c", None, row(1), pos=1)
    with pytest.raises(ValueError, match="unsupported op"):
        decode(topic, key, value.replace(b'"op":"c"', b'"op":"t"'), 0, 0)
