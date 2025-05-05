"""Prometheus metrics for the warehouse consumer."""
from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram


events_processed_total = Counter(
    "events_processed_total",
    "Total number of warehouse events successfully processed by the consumer.",
    labelnames=("event_type",),
)

events_skipped_total = Counter(
    "events_skipped_total",
    "Number of events skipped (duplicates by event_id or stale by timestamp).",
    labelnames=("reason",),  # "duplicate" | "stale"
)

events_dlq_total = Counter(
    "events_dlq_total",
    "Number of events sent to the dead-letter queue.",
    labelnames=("error_code",),
)

event_processing_duration_seconds = Histogram(
    "event_processing_duration_seconds",
    "End-to-end processing time of a single warehouse event.",
    labelnames=("event_type",),
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

cassandra_write_errors_total = Counter(
    "cassandra_write_errors_total",
    "Number of failed write attempts against Cassandra.",
)

consumer_lag = Gauge(
    "consumer_lag",
    "Difference between the high-watermark offset and the consumer's committed offset, per partition.",
    labelnames=("topic", "partition"),
)

kafka_connected = Gauge(
    "kafka_connected",
    "1 if the consumer is currently connected to Kafka (last poll within timeout), 0 otherwise.",
)

cassandra_connected = Gauge(
    "cassandra_connected",
    "1 if a Cassandra session is healthy, 0 otherwise.",
)
