"""Smart Warehouse event producer CLI.

Sends Avro-encoded WarehouseEvent messages to the `warehouse-events` Kafka
topic. Supports both schema versions (v1 / v2) for the Schema Evolution demo
and lets you produce identical event_ids on demand (idempotency demo) or
malformed events (DLQ demo).

Examples
--------
  python events.py received      --product SKU-001 --zone ZONE-A --quantity 100
  python events.py received      --product SKU-001 --zone ZONE-A --quantity 100 \
                                 --supplier SUP-001 --schema-version v2
  python events.py shipped       --product SKU-001 --zone ZONE-A --quantity 10
  python events.py moved         --product SKU-001 --from-zone ZONE-A --to-zone ZONE-B --quantity 20
  python events.py reserved      --product SKU-001 --zone ZONE-A --quantity 30
  python events.py released      --product SKU-001 --zone ZONE-A --quantity 10
  python events.py counted       --product SKU-001 --zone ZONE-A --counted 100
  python events.py order-created --order ORD-1 --item SKU-001:ZONE-A:15
  python events.py order-completed --order ORD-1
  python events.py received      --product SKU-001 --zone ZONE-A --quantity -5   # → DLQ
  python events.py received      --product SKU-001 --zone ZONE-A --quantity 50 \
                                 --event-id fixed-id-001                          # → idempotency test
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import pathlib
import sys
import uuid
from typing import Any, Dict, List, Optional

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer


SCHEMA_DIR = pathlib.Path(os.getenv("SCHEMA_DIR", "/schemas"))
DEFAULT_TOPIC = os.getenv("KAFKA_TOPIC", "warehouse-events")
DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DEFAULT_SR_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")


def _load_schema(version: str) -> str:
    path = SCHEMA_DIR / f"warehouse_event_{version}.avsc"
    if not path.exists():
        raise SystemExit(f"Schema file not found: {path}")
    return path.read_text()


def _ts_ms(value: Optional[str]) -> int:
    if value is None:
        return int(dt.datetime.now(tz=dt.timezone.utc).timestamp() * 1000)
    try:
        return int(value)  # raw ms
    except ValueError:
        pass
    # ISO-8601, optionally with "Z"
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return int(parsed.timestamp() * 1000)


def _new_event_id() -> str:
    return f"evt-{uuid.uuid4()}"


def _parse_item(spec: str) -> Dict[str, Any]:
    parts = spec.split(":")
    if len(parts) != 3:
        raise argparse.ArgumentTypeError(
            f"Item must be PRODUCT:ZONE:QTY, got {spec!r}"
        )
    pid, zid, qty = parts
    return {"product_id": pid, "zone_id": zid, "quantity": int(qty)}


def _build_event(args: argparse.Namespace) -> Dict[str, Any]:
    cmd = args.command
    event_id = args.event_id or _new_event_id()
    timestamp = _ts_ms(args.timestamp)

    base: Dict[str, Any] = {
        "event_id": event_id,
        "event_timestamp": timestamp,
    }

    if cmd == "received":
        base.update(
            event_type="PRODUCT_RECEIVED",
            product_id=args.product,
            zone_id=args.zone,
            quantity=args.quantity,
        )
        if args.supplier is not None:
            base["supplier_id"] = args.supplier
    elif cmd == "shipped":
        base.update(
            event_type="PRODUCT_SHIPPED",
            product_id=args.product,
            zone_id=args.zone,
            quantity=args.quantity,
        )
    elif cmd == "moved":
        base.update(
            event_type="PRODUCT_MOVED",
            product_id=args.product,
            from_zone_id=args.from_zone,
            to_zone_id=args.to_zone,
            quantity=args.quantity,
        )
    elif cmd == "reserved":
        base.update(
            event_type="PRODUCT_RESERVED",
            product_id=args.product,
            zone_id=args.zone,
            quantity=args.quantity,
        )
    elif cmd == "released":
        base.update(
            event_type="PRODUCT_RELEASED",
            product_id=args.product,
            zone_id=args.zone,
            quantity=args.quantity,
        )
    elif cmd == "counted":
        base.update(
            event_type="INVENTORY_COUNTED",
            product_id=args.product,
            zone_id=args.zone,
            counted_quantity=args.counted,
        )
    elif cmd == "order-created":
        base.update(
            event_type="ORDER_CREATED",
            order_id=args.order,
            items=args.items,
        )
    elif cmd == "order-completed":
        base.update(
            event_type="ORDER_COMPLETED",
            order_id=args.order,
        )
    elif cmd == "raw":
        payload = json.loads(args.json)
        base.update(payload)
    else:  # pragma: no cover
        raise SystemExit(f"Unknown command: {cmd}")
    return base


def _pick_key(event: Dict[str, Any]) -> Optional[str]:
    # Send same-entity events to the same partition for in-order delivery.
    return event.get("product_id") or event.get("order_id") or "warehouse"


def _delivery(err, msg):
    if err is not None:
        print(f"ERROR delivering message: {err}", file=sys.stderr)
    else:
        print(
            f"sent topic={msg.topic()} partition={msg.partition()} "
            f"offset={msg.offset()} key={(msg.key() or b'').decode('utf-8', errors='replace')}"
        )


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="events", description="WarehouseEvent producer")
    p.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP)
    p.add_argument("--schema-registry-url", default=DEFAULT_SR_URL)
    p.add_argument("--topic", default=DEFAULT_TOPIC)
    p.add_argument("--schema-version", choices=["v1", "v2"], default="v1",
                   help="Avro schema version to encode with (default: v1)")
    p.add_argument("--event-id", default=None,
                   help="Override event_id (use to test idempotency)")
    p.add_argument("--timestamp", default=None,
                   help="Event timestamp (ISO-8601 or ms-since-epoch); default: now")
    p.add_argument("--repeat", type=int, default=1, help="Send the same event N times")

    sub = p.add_subparsers(dest="command", required=True)

    pr = sub.add_parser("received", help="PRODUCT_RECEIVED")
    pr.add_argument("--product", required=True)
    pr.add_argument("--zone", required=True)
    pr.add_argument("--quantity", type=int, required=True)
    pr.add_argument("--supplier", default=None,
                    help="Supplier id (requires --schema-version=v2)")

    ps = sub.add_parser("shipped", help="PRODUCT_SHIPPED")
    ps.add_argument("--product", required=True)
    ps.add_argument("--zone", required=True)
    ps.add_argument("--quantity", type=int, required=True)

    pm = sub.add_parser("moved", help="PRODUCT_MOVED")
    pm.add_argument("--product", required=True)
    pm.add_argument("--from-zone", dest="from_zone", required=True)
    pm.add_argument("--to-zone", dest="to_zone", required=True)
    pm.add_argument("--quantity", type=int, required=True)

    prv = sub.add_parser("reserved", help="PRODUCT_RESERVED")
    prv.add_argument("--product", required=True)
    prv.add_argument("--zone", required=True)
    prv.add_argument("--quantity", type=int, required=True)

    pre = sub.add_parser("released", help="PRODUCT_RELEASED")
    pre.add_argument("--product", required=True)
    pre.add_argument("--zone", required=True)
    pre.add_argument("--quantity", type=int, required=True)

    pc = sub.add_parser("counted", help="INVENTORY_COUNTED")
    pc.add_argument("--product", required=True)
    pc.add_argument("--zone", required=True)
    pc.add_argument("--counted", type=int, required=True)

    oc = sub.add_parser("order-created", help="ORDER_CREATED")
    oc.add_argument("--order", required=True)
    oc.add_argument("--item", action="append", dest="items", type=_parse_item,
                    required=True,
                    help="Repeatable: PRODUCT_ID:ZONE_ID:QTY")

    ok = sub.add_parser("order-completed", help="ORDER_COMPLETED")
    ok.add_argument("--order", required=True)

    raw = sub.add_parser("raw", help="Send a raw JSON event")
    raw.add_argument("--json", required=True, help="JSON event payload")

    return p


def main(argv: List[str]) -> int:
    args = build_parser().parse_args(argv)
    schema_str = _load_schema(args.schema_version)

    sr_client = SchemaRegistryClient({"url": args.schema_registry_url})
    serializer = AvroSerializer(
        sr_client,
        schema_str,
        conf={"auto.register.schemas": True},
    )
    key_ser = StringSerializer("utf_8")

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "lz4",
        }
    )

    event = _build_event(args)
    if args.schema_version == "v1" and event.get("supplier_id") is not None:
        print(
            "WARNING: supplier_id is set but schema-version=v1 does not contain that "
            "field. Either use --schema-version=v2 or drop --supplier.",
            file=sys.stderr,
        )
        # Strip the field so v1 serializer does not fail.
        event.pop("supplier_id", None)

    key = _pick_key(event)
    ctx = SerializationContext(args.topic, MessageField.VALUE)

    print(
        json.dumps(
            {
                "topic": args.topic,
                "schema_version": args.schema_version,
                "key": key,
                "event": event,
            },
            default=str,
            ensure_ascii=False,
            indent=2,
        )
    )

    for _ in range(args.repeat):
        producer.produce(
            topic=args.topic,
            key=key_ser(key, SerializationContext(args.topic, MessageField.KEY)),
            value=serializer(event, ctx),
            on_delivery=_delivery,
        )
        producer.poll(0)

    producer.flush(15.0)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
