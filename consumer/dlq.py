"""Dead-letter queue producer.

Events that cannot be processed (validation errors, business rule violations,
unparseable payloads) are forwarded to a dedicated Kafka topic as JSON so they
can be inspected and replayed by operators.
"""
from __future__ import annotations

import base64
import datetime as dt
import json
import logging
from typing import Any, Dict, Optional

from confluent_kafka import Message, Producer

from config import Config
from metrics import events_dlq_total

log = logging.getLogger(__name__)


class DLQProducer:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._producer = Producer(
            {
                "bootstrap.servers": cfg.kafka_bootstrap_servers,
                "client.id": f"{cfg.kafka_group_id}-dlq",
                "enable.idempotence": True,
                "acks": "all",
                "compression.type": "lz4",
            }
        )

    def send(
        self,
        msg: Message,
        error_reason: str,
        error_code: str,
        deserialized_event: Optional[Dict[str, Any]] = None,
    ) -> None:
        raw = msg.value()
        payload: Dict[str, Any] = {
            "original_event": (
                deserialized_event
                if deserialized_event is not None
                else {
                    "raw_base64": base64.b64encode(raw).decode("ascii") if raw else None,
                }
            ),
            "error_reason": error_reason,
            "error_code": error_code,
            "failed_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
            "kafka_metadata": {
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "key": (msg.key() or b"").decode("utf-8", errors="replace") or None,
                "timestamp_ms": msg.timestamp()[1] if msg.timestamp() else None,
            },
        }
        try:
            body = json.dumps(payload, default=str, ensure_ascii=False).encode("utf-8")
            self._producer.produce(
                topic=self.cfg.kafka_dlq_topic,
                key=msg.key(),
                value=body,
                callback=self._delivery_cb,
            )
            self._producer.poll(0)
            events_dlq_total.labels(error_code=error_code).inc()
            log.warning(
                "Event sent to DLQ",
                extra={
                    "error_code": error_code,
                    "error_reason": error_reason,
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                },
            )
        except Exception:  # noqa: BLE001
            log.exception("Failed to publish to DLQ")
            raise

    def flush(self, timeout: float = 5.0) -> None:
        self._producer.flush(timeout)

    @staticmethod
    def _delivery_cb(err, msg):
        if err is not None:
            log.error(
                "DLQ delivery failed",
                extra={"error": str(err), "topic": msg.topic()},
            )
