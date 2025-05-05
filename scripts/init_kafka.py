"""One-shot initialiser for Kafka topics + Schema Registry subjects.

Run inside a Python container that has `confluent-kafka` installed (the
producer image, in our docker-compose). Idempotent: re-running is a no-op.

Env vars (with defaults that match docker-compose):
    KAFKA_BOOTSTRAP_SERVERS=kafka:9092
    SCHEMA_REGISTRY_URL=http://schema-registry:8081
    SCHEMA_DIR=/schemas
    TOPIC_MAIN=warehouse-events
    TOPIC_DLQ=warehouse-events-dlq
    TOPIC_PARTITIONS=3
    TOPIC_REPLICATION=1
    SUBJECT=warehouse-events-value
    COMPATIBILITY=BACKWARD
"""
from __future__ import annotations

import json
import os
import pathlib
import sys
import time
import urllib.error
import urllib.request


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


KAFKA_BOOTSTRAP = _env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SR_URL = _env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081").rstrip("/")
SCHEMA_DIR = pathlib.Path(_env("SCHEMA_DIR", "/schemas"))
TOPIC_MAIN = _env("TOPIC_MAIN", "warehouse-events")
TOPIC_DLQ = _env("TOPIC_DLQ", "warehouse-events-dlq")
TOPIC_PARTITIONS = int(_env("TOPIC_PARTITIONS", "3"))
TOPIC_REPLICATION = int(_env("TOPIC_REPLICATION", "1"))
SUBJECT = _env("SUBJECT", "warehouse-events-value")
COMPATIBILITY = _env("COMPATIBILITY", "BACKWARD")


# ---------------------------------------------------------------------------
# Kafka topics
# ---------------------------------------------------------------------------
def ensure_topics() -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})

    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            admin.list_topics(timeout=5)
            break
        except Exception as exc:  # noqa: BLE001
            print(f"[kafka-init] Waiting for Kafka: {exc}", flush=True)
            time.sleep(3)
    else:
        raise SystemExit("Kafka did not become reachable in 120s")

    existing = set(admin.list_topics(timeout=10).topics.keys())
    to_create = []
    for name in (TOPIC_MAIN, TOPIC_DLQ):
        if name in existing:
            print(f"[kafka-init] Topic already exists: {name}", flush=True)
            continue
        to_create.append(
            NewTopic(name, num_partitions=TOPIC_PARTITIONS, replication_factor=TOPIC_REPLICATION)
        )

    if not to_create:
        return

    futures = admin.create_topics(to_create, request_timeout=15)
    for name, future in futures.items():
        try:
            future.result(timeout=15)
            print(f"[kafka-init] Created topic: {name}", flush=True)
        except Exception as exc:  # noqa: BLE001
            # If a race created the topic, ignore.
            if "TopicExistsError" in repr(type(exc).__name__) or "already exists" in str(exc):
                print(f"[kafka-init] Topic raced into existence: {name}", flush=True)
            else:
                raise


# ---------------------------------------------------------------------------
# Schema Registry
# ---------------------------------------------------------------------------
def _sr_request(method: str, path: str, body: dict | None = None) -> dict:
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        f"{SR_URL}{path}",
        data=data,
        method=method,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        body_text = resp.read().decode("utf-8")
        return json.loads(body_text) if body_text else {}


def wait_for_sr() -> None:
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            urllib.request.urlopen(f"{SR_URL}/subjects", timeout=5).read()
            return
        except (urllib.error.URLError, urllib.error.HTTPError) as exc:
            print(f"[kafka-init] Waiting for Schema Registry: {exc}", flush=True)
            time.sleep(3)
    raise SystemExit("Schema Registry did not become reachable in 120s")


def set_compatibility() -> None:
    print(f"[kafka-init] Setting compatibility={COMPATIBILITY} on {SUBJECT}", flush=True)
    try:
        _sr_request("PUT", f"/config/{SUBJECT}", {"compatibility": COMPATIBILITY})
    except urllib.error.HTTPError as exc:
        # Subject may not exist yet — set global compatibility instead.
        if exc.code in (404, 422):
            print(f"[kafka-init] Subject not yet present, setting global compatibility", flush=True)
            _sr_request("PUT", "/config", {"compatibility": COMPATIBILITY})
        else:
            raise


def register_schema(file: pathlib.Path, label: str) -> None:
    schema_str = file.read_text()
    print(f"[kafka-init] Registering {label} from {file}", flush=True)
    try:
        response = _sr_request(
            "POST",
            f"/subjects/{SUBJECT}/versions",
            {"schemaType": "AVRO", "schema": schema_str},
        )
        print(f"[kafka-init]   → id={response.get('id')}", flush=True)
    except urllib.error.HTTPError as exc:
        msg = exc.read().decode("utf-8", errors="replace")
        raise SystemExit(f"Failed to register {label}: HTTP {exc.code} {msg}")


def ensure_schemas() -> None:
    set_compatibility()
    register_schema(SCHEMA_DIR / "warehouse_event_v1.avsc", "WarehouseEvent v1")
    register_schema(SCHEMA_DIR / "warehouse_event_v2.avsc", "WarehouseEvent v2")
    versions = _sr_request("GET", f"/subjects/{SUBJECT}/versions")
    print(f"[kafka-init] Versions registered under {SUBJECT}: {versions}", flush=True)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main() -> int:
    print(
        f"[kafka-init] Bootstrap={KAFKA_BOOTSTRAP}  SR={SR_URL}  Subject={SUBJECT}",
        flush=True,
    )
    ensure_topics()
    wait_for_sr()
    ensure_schemas()
    print("[kafka-init] Done.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
