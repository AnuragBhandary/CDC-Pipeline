"""Registers the Debezium MySQL connector with Kafka Connect through its REST API."""

from __future__ import annotations

import contextlib
import time
from typing import Any

import httpx

from .config import Settings


def connector_name(s: Settings) -> str:
    return f"{s.topic_prefix}-mysql"


def config(s: Settings, server_id: int) -> dict[str, Any]:
    return {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "tasks.max": "1",  # a MySQL binlog is one ordered stream: one reader
        # Connect runs inside the compose network, so it reaches MySQL by service name.
        "database.hostname": "mysql",
        "database.port": "3306",
        "database.user": "debezium",
        "database.password": "debezium",  # local dev credential (docker/mysql/init.sql)
        "database.server.id": str(server_id),  # must be unique among the server's replicas
        "topic.prefix": s.topic_prefix,
        "database.include.list": s.mysql_db,
        "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
        "schema.history.internal.kafka.topic": f"_history.{s.topic_prefix}",
        "include.schema.changes": "false",
        # Snapshot existing rows first (op = r), then stream the binlog from that point.
        "snapshot.mode": "initial",
        # DECIMAL as exact bytes + scale, not a lossy double or an untyped string.
        "decimal.handling.mode": "precise",
        "time.precision.mode": "adaptive_time_microseconds",
        "tombstones.on.delete": "true",
        # Latency: the default 500 ms poll interval alone would eat a quarter of the lag budget.
        "poll.interval.ms": "50",
        "max.batch.size": "4096",
        "max.queue.size": "32768",
        "producer.override.linger.ms": "5",
        "producer.override.compression.type": "lz4",
        # Every table gets 3 partitions. Debezium keys messages by primary key, so all changes
        # to one row land in one partition, in order.
        "topic.creation.default.replication.factor": "1",
        "topic.creation.default.partitions": "3",
    }


def create(s: Settings, server_id: int = 5501, wait_s: float = 60) -> dict[str, Any]:
    name = connector_name(s)
    with httpx.Client(base_url=s.connect_url, timeout=30) as c:
        r = c.put(f"/connectors/{name}/config", json=config(s, server_id))
        r.raise_for_status()
        deadline = time.time() + wait_s
        while time.time() < deadline:
            st = c.get(f"/connectors/{name}/status")
            if st.status_code == 200:
                body = st.json()
                tasks = body.get("tasks", [])
                if tasks and all(t["state"] == "RUNNING" for t in tasks):
                    return dict(body)
                if any(t["state"] == "FAILED" for t in tasks):
                    raise RuntimeError(f"connector failed: {tasks[0].get('trace', '')[:2000]}")
            time.sleep(0.5)
    raise TimeoutError(f"connector {name} not running after {wait_s}s")


def delete(s: Settings, forget_offsets: bool = True) -> None:
    """Removes the connector. With forget_offsets, its stored binlog position is deleted too
    (Kafka Connect 3.6+ offsets API), so a connector created later under the same name snapshots
    from scratch instead of resuming where this one stopped."""
    name = connector_name(s)
    with httpx.Client(base_url=s.connect_url, timeout=30) as c:
        if c.get(f"/connectors/{name}").status_code == 404:
            return
        if forget_offsets:
            c.put(f"/connectors/{name}/stop").raise_for_status()
            for _ in range(60):
                if c.get(f"/connectors/{name}/status").json()["connector"]["state"] == "STOPPED":
                    break
                time.sleep(0.5)
            r = c.delete(f"/connectors/{name}/offsets")
            if r.status_code not in (200, 404):
                r.raise_for_status()
        r = c.delete(f"/connectors/{name}")
        if r.status_code not in (204, 404):
            r.raise_for_status()


def delete_topics_and_groups(s: Settings, groups: list[str]) -> list[str]:
    """Deletes this prefix's change topics, its schema-history topic and the given groups."""
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": s.kafka})
    topics = [
        t
        for t in admin.list_topics(timeout=10).topics
        if t.startswith(f"{s.topic_prefix}.") or t in (s.topic_prefix, f"_history.{s.topic_prefix}")
    ]
    if topics:
        for f in admin.delete_topics(topics, operation_timeout=30).values():
            f.result()
    if groups:
        for f in admin.delete_consumer_groups(groups, request_timeout=30).values():
            with contextlib.suppress(Exception):  # the group doesn't exist
                f.result()
    return topics


def status(s: Settings) -> dict[str, Any]:
    with httpx.Client(base_url=s.connect_url, timeout=30) as c:
        return dict(c.get(f"/connectors/{connector_name(s)}/status").json())
