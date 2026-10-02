"""Proves the replica equals the source.

1. Catch-up barrier: write a marker row into MySQL, wait until the sink has applied it, then wait
   until the consumer group has no lag on any partition. The binlog is one ordered stream and
   Debezium emits in binlog order, so once the marker and every partition are drained, every
   change before the marker has been applied.
2. Compare every table, row by row: the source rows against the target's live rows
   (_deleted = false). Values are normalised to one text form on both sides (exact decimals,
   microsecond timestamps, NULL as a sentinel), and a SHA-256 checksum is computed per table over
   the sorted rows.

    missing     in the source, not live in the target (lost change or resurrected delete)
    extra       live in the target, gone from the source (lost delete)
    mismatched  same key, different values (lost or misordered update)
"""

from __future__ import annotations

import hashlib
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime
from datetime import time as dtime
from decimal import Decimal
from typing import Any

from confluent_kafka import Consumer, TopicPartition
from psycopg import sql

from .config import Settings

NULL = "\\N"


def norm(v: Any) -> str:
    if v is None:
        return NULL
    if isinstance(v, datetime):
        return v.replace(tzinfo=None).isoformat(sep=" ", timespec="microseconds")
    if isinstance(v, date | dtime):
        return v.isoformat()
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, bool):
        return str(int(v))
    if isinstance(v, bytes | bytearray | memoryview):
        return bytes(v).hex()
    return str(v)


@dataclass
class TableReport:
    table: str
    source_rows: int
    target_rows: int
    missing: int
    extra: int
    mismatched: int
    source_sha256: str
    target_sha256: str
    examples: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return (self.missing, self.extra, self.mismatched) == (0, 0, 0) and (
            self.source_sha256 == self.target_sha256
        )


def consumer_lag(s: Settings, group: str) -> int:
    c = Consumer({"bootstrap.servers": s.kafka, "group.id": group, "enable.auto.commit": False})
    try:
        md = c.list_topics(timeout=10)
        prefix = f"{s.topic_prefix}.{s.mysql_db}."
        tps = [
            TopicPartition(t, p)
            for t, meta in md.topics.items()
            if t.startswith(prefix)
            for p in meta.partitions
        ]
        total = 0
        for tp in c.committed(tps, timeout=10):
            _, high = c.get_watermark_offsets(tp, timeout=10)
            committed = tp.offset if tp.offset >= 0 else 0
            total += max(0, high - committed)
        return total
    finally:
        c.close()


def wait_caught_up(s: Settings, group: str, timeout_s: float = 300) -> float:
    """Returns seconds waited after the marker was written."""
    marker = str(uuid.uuid4())
    my = s.mysql()
    with my.cursor() as cur:
        cur.execute("INSERT INTO cdc_markers (id) VALUES (%s)", (marker,))
    my.commit()
    my.close()
    t0 = time.time()
    pg = s.pg(autocommit=True)
    q = sql.SQL("SELECT 1 FROM {} WHERE id = %s").format(
        sql.Identifier(s.target_schema, "cdc_markers")
    )
    try:
        while True:
            if time.time() - t0 > timeout_s:
                raise TimeoutError("sink did not catch up")
            try:
                seen = pg.execute(q, (marker,)).fetchone()
            except Exception:  # table not created yet
                seen = None
            if seen and consumer_lag(s, group) == 0:
                return time.time() - t0
            time.sleep(0.3)
    finally:
        pg.close()


def compare(s: Settings) -> list[TableReport]:
    my = s.mysql()
    pg = s.pg(autocommit=True)
    reports = []
    try:
        with my.cursor() as cur:
            cur.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
            cur.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = %s"
                " AND table_type = 'BASE TABLE' ORDER BY table_name",
                (s.mysql_db,),
            )
            tables = [r[0] for r in cur.fetchall()]
            for t in tables:
                cur.execute(
                    "SELECT column_name, column_key = 'PRI' FROM information_schema.columns"
                    " WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
                    (s.mysql_db, t),
                )
                meta = cur.fetchall()
                cols = [m[0] for m in meta]
                npk = [i for i, m in enumerate(meta) if m[1]]
                cur.execute(f"SELECT {', '.join(f'`{c}`' for c in cols)} FROM `{t}`")
                src = {tuple(norm(r[i]) for i in npk): tuple(map(norm, r)) for r in cur.fetchall()}
                try:
                    rows = pg.execute(
                        sql.SQL("SELECT {} FROM {} WHERE NOT _deleted").format(
                            sql.SQL(", ").join(map(sql.Identifier, cols)),
                            sql.Identifier(s.target_schema, t),
                        )
                    ).fetchall()
                except Exception:
                    rows = []
                dst = {tuple(norm(r[i]) for i in npk): tuple(map(norm, r)) for r in rows}
                missing = src.keys() - dst.keys()
                extra = dst.keys() - src.keys()
                mism = [k for k in src.keys() & dst.keys() if src[k] != dst[k]]
                reports.append(
                    TableReport(
                        t,
                        len(src),
                        len(dst),
                        len(missing),
                        len(extra),
                        len(mism),
                        _sha(src.values()),
                        _sha(dst.values()),
                        [f"missing {k}" for k in list(missing)[:2]]
                        + [f"extra {k}" for k in list(extra)[:2]]
                        + [f"{k}: {src[k]} != {dst[k]}" for k in mism[:2]],
                    )
                )
        my.rollback()
    finally:
        my.close()
        pg.close()
    return reports


def _sha(rows: Any) -> str:
    h = hashlib.sha256()
    for line in sorted("\x1f".join(r) for r in rows):
        h.update(line.encode())
        h.update(b"\n")
    return h.hexdigest()
