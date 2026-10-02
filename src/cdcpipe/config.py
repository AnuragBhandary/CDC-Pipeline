"""Connection settings from the environment; defaults match docker-compose.yml."""

from __future__ import annotations

import os
from dataclasses import dataclass

import psycopg
import pymysql


@dataclass(frozen=True)
class Settings:
    mysql_host: str = "127.0.0.1"
    mysql_port: int = 3308
    mysql_user: str = "app"
    mysql_password: str = "app"
    mysql_db: str = "shop"
    pg_dsn: str = "postgresql://cdc:cdc@127.0.0.1:5434/replica"
    target_schema: str = "shop"
    kafka: str = "127.0.0.1:9094"
    connect_url: str = "http://localhost:8083"
    topic_prefix: str = "cdc"

    @classmethod
    def from_env(cls) -> Settings:
        e = os.environ
        d = cls()
        return cls(
            mysql_host=e.get("MYSQL_HOST", d.mysql_host),
            mysql_port=int(e.get("MYSQL_PORT", d.mysql_port)),
            mysql_user=e.get("MYSQL_USER", d.mysql_user),
            mysql_password=e.get("MYSQL_PASSWORD", d.mysql_password),
            mysql_db=e.get("MYSQL_DATABASE", d.mysql_db),
            pg_dsn=e.get("TARGET_DSN", d.pg_dsn),
            target_schema=e.get("TARGET_SCHEMA", d.target_schema),
            kafka=e.get("KAFKA_BOOTSTRAP", d.kafka),
            connect_url=e.get("CONNECT_URL", d.connect_url),
            topic_prefix=e.get("TOPIC_PREFIX", d.topic_prefix),
        )

    @property
    def topic_regex(self) -> str:
        """Debezium names topics <prefix>.<database>.<table>."""
        return f"^{self.topic_prefix}\\.{self.mysql_db}\\..*"

    def mysql(self, db: str | None = None) -> pymysql.connections.Connection:
        return pymysql.connect(
            host=self.mysql_host,
            port=self.mysql_port,
            user=self.mysql_user,
            password=self.mysql_password,
            database=db if db is not None else self.mysql_db,
            autocommit=False,
            init_command="SET time_zone = '+00:00'",
        )

    def pg(self, autocommit: bool = False, app: str = "cdcpipe") -> psycopg.Connection:
        conn = psycopg.connect(self.pg_dsn, autocommit=autocommit, application_name=app)
        conn.execute("SET TIME ZONE 'UTC'")
        if not autocommit:
            conn.commit()
        return conn
