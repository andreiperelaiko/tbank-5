"""Configuration loaded from environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import List


def _split_csv(value: str) -> List[str]:
    return [s.strip() for s in value.split(",") if s.strip()]


@dataclass
class Config:
    # --- Kafka -------------------------------------------------------------
    kafka_bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    )
    kafka_topic: str = field(default_factory=lambda: os.getenv("KAFKA_TOPIC", "warehouse-events"))
    kafka_dlq_topic: str = field(
        default_factory=lambda: os.getenv("KAFKA_DLQ_TOPIC", "warehouse-events-dlq")
    )
    kafka_group_id: str = field(
        default_factory=lambda: os.getenv("KAFKA_GROUP_ID", "warehouse-state-consumer")
    )
    kafka_auto_offset_reset: str = field(
        default_factory=lambda: os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
    )
    kafka_session_timeout_ms: int = field(
        default_factory=lambda: int(os.getenv("KAFKA_SESSION_TIMEOUT_MS", "30000"))
    )
    kafka_max_poll_interval_ms: int = field(
        default_factory=lambda: int(os.getenv("KAFKA_MAX_POLL_INTERVAL_MS", "300000"))
    )

    # --- Schema Registry ---------------------------------------------------
    schema_registry_url: str = field(
        default_factory=lambda: os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    )

    # --- Cassandra ---------------------------------------------------------
    cassandra_contact_points: List[str] = field(
        default_factory=lambda: _split_csv(
            os.getenv("CASSANDRA_CONTACT_POINTS", "cassandra-1,cassandra-2,cassandra-3")
        )
    )
    cassandra_port: int = field(default_factory=lambda: int(os.getenv("CASSANDRA_PORT", "9042")))
    cassandra_keyspace: str = field(
        default_factory=lambda: os.getenv("CASSANDRA_KEYSPACE", "warehouse")
    )
    cassandra_dc: str = field(default_factory=lambda: os.getenv("CASSANDRA_DC", "dc1"))
    cassandra_username: str = field(default_factory=lambda: os.getenv("CASSANDRA_USERNAME", ""))
    cassandra_password: str = field(default_factory=lambda: os.getenv("CASSANDRA_PASSWORD", ""))
    cassandra_write_consistency: str = field(
        default_factory=lambda: os.getenv("CASSANDRA_WRITE_CONSISTENCY", "QUORUM")
    )
    cassandra_read_consistency: str = field(
        default_factory=lambda: os.getenv("CASSANDRA_READ_CONSISTENCY", "QUORUM")
    )

    # --- HTTP (health + metrics) ------------------------------------------
    http_host: str = field(default_factory=lambda: os.getenv("HTTP_HOST", "0.0.0.0"))
    http_port: int = field(default_factory=lambda: int(os.getenv("HTTP_PORT", "8000")))

    # --- Misc --------------------------------------------------------------
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    poll_timeout_seconds: float = field(
        default_factory=lambda: float(os.getenv("POLL_TIMEOUT_SECONDS", "1.0"))
    )
    cassandra_max_retries: int = field(
        default_factory=lambda: int(os.getenv("CASSANDRA_MAX_RETRIES", "5"))
    )

    @classmethod
    def from_env(cls) -> "Config":
        return cls()
