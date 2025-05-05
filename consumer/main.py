"""Smart Warehouse Kafka consumer entry point."""
from __future__ import annotations

import datetime as dt
import logging
import signal
import sys
import time
from typing import Any, Dict, Optional

from cassandra import DriverException
from confluent_kafka import Consumer, KafkaError, KafkaException, Message, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

import metrics
from cassandra_repo import CassandraRepository
from config import Config
from dlq import DLQProducer
from errors import StaleEventError, ValidationError
from handlers import compute_batch, processed_stmt
from http_server import start_http_server
from logging_setup import setup_logging

log = logging.getLogger("consumer")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _coerce_event_type(value: Any) -> str:
    """Avro enum is exposed by confluent-kafka as a string already, but be safe."""
    if value is None:
        return ""
    return str(value)


def _normalize_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """Make event_type a plain string for downstream code."""
    if not isinstance(event, dict):
        raise ValidationError(
            f"Deserialized event is not a dict: {type(event).__name__}",
            code="DESERIALIZATION_ERROR",
        )
    if "event_type" in event:
        event["event_type"] = _coerce_event_type(event["event_type"])
    return event


# ---------------------------------------------------------------------------
# WarehouseConsumer
# ---------------------------------------------------------------------------
class WarehouseConsumer:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.cassandra = CassandraRepository(cfg)
        self.dlq = DLQProducer(cfg)
        self.sr_client = SchemaRegistryClient({"url": cfg.schema_registry_url})
        # No reader schema → the deserializer uses each message's writer schema,
        # which transparently supports both v1 (no supplier_id) and v2 (with it).
        self.deserializer = AvroDeserializer(self.sr_client)

        self.consumer = Consumer(
            {
                "bootstrap.servers": cfg.kafka_bootstrap_servers,
                "group.id": cfg.kafka_group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": cfg.kafka_auto_offset_reset,
                "session.timeout.ms": cfg.kafka_session_timeout_ms,
                "max.poll.interval.ms": cfg.kafka_max_poll_interval_ms,
                "isolation.level": "read_committed",
                "client.id": f"{cfg.kafka_group_id}-{int(time.time())}",
            }
        )
        self.consumer.subscribe(
            [cfg.kafka_topic],
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )

        self._running = True
        self._last_poll_ok_at = time.monotonic()
        self._lag_refresh_interval_s = 5.0
        self._last_lag_refresh_at = 0.0

    # ----- lifecycle ------------------------------------------------------
    def start(self) -> None:
        self.cassandra.connect()
        metrics.kafka_connected.set(0)  # set to 1 on first successful poll
        log.info(
            "Consumer starting",
            extra={"group_id": self.cfg.kafka_group_id, "topic": self.cfg.kafka_topic},
        )

    def stop(self) -> None:
        self._running = False

    def close(self) -> None:
        log.info("Closing consumer")
        try:
            self.consumer.close()
        except Exception:  # noqa: BLE001
            log.exception("Error closing Kafka consumer")
        try:
            self.dlq.flush(5.0)
        except Exception:  # noqa: BLE001
            log.exception("Error flushing DLQ producer")
        try:
            self.cassandra.close()
        except Exception:  # noqa: BLE001
            log.exception("Error closing Cassandra session")

    def is_healthy(self) -> bool:
        # liveness/readiness: Kafka responsive within last 60s AND Cassandra reachable.
        kafka_ok = (time.monotonic() - self._last_poll_ok_at) < 60.0
        metrics.kafka_connected.set(1 if kafka_ok else 0)
        cass_ok = self.cassandra.is_healthy()
        return kafka_ok and cass_ok

    # ----- partition events ----------------------------------------------
    def _on_assign(self, consumer, partitions):
        for tp in partitions:
            log.info("Partition assigned", extra={"topic": tp.topic, "partition": tp.partition})
        consumer.assign(partitions)

    def _on_revoke(self, consumer, partitions):
        for tp in partitions:
            log.info("Partition revoked", extra={"topic": tp.topic, "partition": tp.partition})

    # ----- main loop ------------------------------------------------------
    def run(self) -> None:
        while self._running:
            try:
                msg = self.consumer.poll(self.cfg.poll_timeout_seconds)
                self._last_poll_ok_at = time.monotonic()
                metrics.kafka_connected.set(1)
                self._maybe_refresh_lag()
                if msg is None:
                    continue
                if msg.error():
                    self._handle_kafka_error(msg)
                    continue
                self._process(msg)
            except KafkaException:
                log.exception("Kafka exception in poll loop")
                time.sleep(1.0)
            except Exception:  # noqa: BLE001
                log.exception("Unexpected error in poll loop")
                time.sleep(1.0)

    def _handle_kafka_error(self, msg: Message) -> None:
        err = msg.error()
        if err.code() == KafkaError._PARTITION_EOF:
            return
        log.error(
            "Kafka error",
            extra={"code": err.code(), "name": err.name(), "str": err.str()},
        )

    # ----- single message processing -------------------------------------
    def _process(self, msg: Message) -> None:
        start = time.perf_counter()
        event_type_for_metric = "unknown"
        event: Optional[Dict[str, Any]] = None
        try:
            event = self._deserialize(msg)
            event = _normalize_event(event)
            event_type_for_metric = event.get("event_type", "unknown") or "unknown"

            log.info(
                "Received event",
                extra={
                    "event_id": event.get("event_id"),
                    "event_type": event_type_for_metric,
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "supplier_id": event.get("supplier_id"),
                },
            )

            event_id = event.get("event_id")
            if not event_id:
                raise ValidationError("Missing event_id", code="MISSING_FIELD")

            # 1) Idempotency — duplicates by event_id are silently skipped.
            if self.cassandra.is_event_processed(event_id):
                metrics.events_skipped_total.labels(reason="duplicate").inc()
                log.info(
                    "Skipping duplicate event",
                    extra={"event_id": event_id, "event_type": event_type_for_metric},
                )
                self._commit(msg)
                return

            now = dt.datetime.now(tz=dt.timezone.utc)

            # 2) Compute the BATCH (raises Validation/Stale errors).
            try:
                statements = compute_batch(event, self.cassandra, now)
            except StaleEventError as stale:
                metrics.events_skipped_total.labels(reason="stale").inc()
                log.info(
                    "Skipping stale event",
                    extra={
                        "event_id": event_id,
                        "event_type": event_type_for_metric,
                        "reason": str(stale),
                    },
                )
                # Still record as processed so future duplicates short-circuit.
                self.cassandra.execute_batch([processed_stmt(event, now)])
                self._commit(msg)
                return

            # 3) Append "mark as processed" to the same batch → atomicity.
            statements.append(processed_stmt(event, now))

            # 4) Apply the BATCH.
            self.cassandra.execute_batch(statements)

            # 5) Commit Kafka offset (at-least-once: offset committed AFTER write).
            self._commit(msg)

            metrics.events_processed_total.labels(event_type=event_type_for_metric).inc()
            log.info(
                "Processed event",
                extra={
                    "event_id": event_id,
                    "event_type": event_type_for_metric,
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                },
            )

        except ValidationError as ve:
            log.warning(
                "Validation error → DLQ",
                extra={"error_code": ve.code, "error": str(ve)},
            )
            self.dlq.send(msg, error_reason=str(ve), error_code=ve.code,
                          deserialized_event=event)
            self._commit(msg)
        except DriverException:
            metrics.cassandra_write_errors_total.inc()
            log.exception("Cassandra error — will retry (offset NOT committed)")
            # Backoff to avoid hot-looping while Cassandra is unhealthy.
            time.sleep(2.0)
        finally:
            duration = time.perf_counter() - start
            metrics.event_processing_duration_seconds.labels(
                event_type=event_type_for_metric
            ).observe(duration)

    def _deserialize(self, msg: Message) -> Dict[str, Any]:
        try:
            value = self.deserializer(
                msg.value(),
                SerializationContext(msg.topic(), MessageField.VALUE),
            )
            if value is None:
                raise ValidationError("Null message value", code="DESERIALIZATION_ERROR")
            return value
        except ValidationError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise ValidationError(
                f"Avro deserialization failed: {exc}",
                code="DESERIALIZATION_ERROR",
            ) from exc

    def _commit(self, msg: Message) -> None:
        self.consumer.commit(message=msg, asynchronous=False)

    # ----- consumer lag ---------------------------------------------------
    def _maybe_refresh_lag(self) -> None:
        now = time.monotonic()
        if now - self._last_lag_refresh_at < self._lag_refresh_interval_s:
            return
        self._last_lag_refresh_at = now
        try:
            assignment = self.consumer.assignment()
            if not assignment:
                return
            committed = self.consumer.committed(assignment, timeout=5.0)
            for tp in committed:
                _, high = self.consumer.get_watermark_offsets(tp, timeout=5.0, cached=True)
                committed_offset = tp.offset if tp.offset >= 0 else 0
                lag = max(0, high - committed_offset)
                metrics.consumer_lag.labels(topic=tp.topic, partition=str(tp.partition)).set(lag)
        except KafkaException:
            log.exception("Failed to refresh consumer lag")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> int:
    cfg = Config.from_env()
    setup_logging(cfg.log_level)
    log.info("Loaded configuration", extra={"config": {
        "kafka": cfg.kafka_bootstrap_servers,
        "topic": cfg.kafka_topic,
        "group_id": cfg.kafka_group_id,
        "schema_registry": cfg.schema_registry_url,
        "cassandra": cfg.cassandra_contact_points,
        "keyspace": cfg.cassandra_keyspace,
        "dc": cfg.cassandra_dc,
        "write_cl": cfg.cassandra_write_consistency,
        "read_cl": cfg.cassandra_read_consistency,
    }})

    consumer = WarehouseConsumer(cfg)
    consumer.start()
    start_http_server(cfg.http_host, cfg.http_port, consumer.is_healthy)

    def _on_signal(sig, _frame):
        log.info("Signal received — stopping", extra={"signal": sig})
        consumer.stop()

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    try:
        consumer.run()
    finally:
        consumer.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
