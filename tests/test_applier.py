"""The sink's apply logic against a real PostgreSQL (no Kafka needed)."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import psycopg
import pytest

from cdcpipe.config import Settings
from cdcpipe.decode import Event, decode
from cdcpipe.sink import Applier, SchemaConflict

from .debezium import FIELDS, TIER, message, row

SCHEMA = "applier_test"


@pytest.fixture
def applier() -> Iterator[Applier]:
    s = Settings.from_env()
    try:
        conn = s.pg()
    except psycopg.OperationalError as e:  # pragma: no cover
        pytest.skip(f"postgres not reachable: {e}")
    conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    conn.commit()
    yield Applier(conn, SCHEMA)
    conn.rollback()
    conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    conn.commit()
    conn.close()


def ev(op: str, before: Any, after: Any, pos: int, offset: int = 0, **kw: Any) -> Event:
    e = decode(*message("items", op, before, after, pos, **kw), partition=0, offset=offset)
    assert e is not None
    return e


def apply(a: Applier, *events: Event) -> tuple[int, int, int]:
    r = a.apply(list(events))
    a.conn.commit()
    return r.applied, r.stale, r.deduped


def table(a: Applier) -> dict[int, tuple[Any, ...]]:
    rows = a.conn.execute(
        f"SELECT id, name, price::text, _deleted, _src_pos FROM {SCHEMA}.items ORDER BY id"
    ).fetchall()
    a.conn.commit()
    return {r[0]: r[1:] for r in rows}


def test_creates_table_and_inserts(applier: Applier) -> None:
    assert apply(applier, ev("r", None, row(1), 100), ev("c", None, row(2), 200)) == (2, 0, 0)
    assert table(applier) == {1: ("a", "1.00", False, 100), 2: ("a", "1.00", False, 200)}
    pk = applier.conn.execute(
        "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid"
        " AND a.attnum = ANY(i.indkey) WHERE i.indrelid = %s::regclass AND i.indisprimary",
        (f"{SCHEMA}.items",),
    ).fetchall()
    applier.conn.commit()
    assert pk == [("id",)]


def test_newest_event_per_key_wins_within_a_batch(applier: Applier) -> None:
    assert apply(
        applier,
        ev("c", None, row(1, "v1"), 100),
        ev("u", row(1, "v1"), row(1, "v3"), 300),
        ev("u", row(1, "v1"), row(1, "v2"), 200),  # arrives late inside the batch
    ) == (1, 0, 2)
    assert table(applier)[1][0] == "v3"


def test_replayed_events_are_noops(applier: Applier) -> None:
    batch = [ev("c", None, row(1, "v1"), 100), ev("u", None, row(1, "v2"), 200)]
    apply(applier, *batch)
    assert apply(applier, *batch) == (0, 1, 1)  # one deduped, the survivor is stale
    assert table(applier)[1] == ("v2", "1.00", False, 200)


def test_older_event_never_overwrites_newer(applier: Applier) -> None:
    apply(applier, ev("u", None, row(1, "new"), 500))
    assert apply(applier, ev("u", None, row(1, "old"), 400)) == (0, 1, 0)
    assert table(applier)[1][0] == "new"


def test_soft_delete_blocks_resurrection(applier: Applier) -> None:
    apply(applier, ev("c", None, row(1), 100))
    apply(applier, ev("d", row(1), None, 200))
    assert table(applier)[1][2] is True
    # The insert is replayed after the delete (e.g. a consumer restarted from an old offset):
    assert apply(applier, ev("c", None, row(1), 100)) == (0, 1, 0)
    assert table(applier)[1][2] is True


def test_same_pos_different_row_index(applier: Applier) -> None:
    """Rows of one multi-row binlog event share pos and differ by row index."""
    apply(applier, ev("u", None, row(1, "first"), 900, row_no=0))
    apply(applier, ev("u", None, row(1, "second"), 900, row_no=1))
    assert table(applier)[1][0] == "second"


def test_added_column_without_restart(applier: Applier) -> None:
    apply(applier, ev("c", None, row(1), 100))
    v2 = [*FIELDS, TIER]
    apply(
        applier,
        ev("u", None, row(1, tier="gold", with_tier=True), 200, fields=v2),
        ev("c", None, row(2, tier="silver", with_tier=True), 300, fields=v2),
    )
    rows = applier.conn.execute(f"SELECT id, tier FROM {SCHEMA}.items ORDER BY id").fetchall()
    applier.conn.commit()
    assert rows == [(1, "gold"), (2, "silver")]


def test_mixed_schema_versions_in_one_batch(applier: Applier) -> None:
    v2 = [*FIELDS, TIER]
    assert apply(
        applier,
        ev("c", None, row(1), 100),
        ev("c", None, row(2, tier="gold", with_tier=True), 200, fields=v2),
    ) == (2, 0, 0)


def test_columns_missing_from_a_newer_event_become_null(applier: Applier) -> None:
    """Full row images: a column the event doesn't carry doesn't exist in the source then."""
    v2 = [*FIELDS, TIER]
    apply(applier, ev("c", None, row(1, tier="gold", with_tier=True), 100, fields=v2))
    apply(applier, ev("u", None, row(1), 200))  # table recreated without the column
    tier = applier.conn.execute(f"SELECT tier FROM {SCHEMA}.items").fetchone()
    applier.conn.commit()
    assert tier == (None,)


def test_decimal_widening_follows_source(applier: Applier) -> None:
    apply(applier, ev("c", None, row(1), 100))
    wide = [
        f
        if f["field"] != "price"
        else {**f, "parameters": {"scale": "2", "connect.decimal.precision": "14"}}
        for f in FIELDS
    ]
    apply(applier, ev("u", None, row(1, price="123456789012.34"), 200, fields=wide))
    assert table(applier)[1][1] == "123456789012.34"


def test_incompatible_type_change_stops_the_sink(applier: Applier) -> None:
    apply(applier, ev("c", None, row(1), 100))
    bad = [
        f if f["field"] != "name" else {"type": "int32", "optional": False, "field": "name"}
        for f in FIELDS
    ]
    r = row(1)
    r["name"] = 5
    with pytest.raises(SchemaConflict, match=r"items\.name"):
        applier.apply([ev("u", None, r, 200, fields=bad)])
