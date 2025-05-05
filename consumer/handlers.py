"""Per-event-type handlers that compute the BATCH of Cassandra statements.

Each handler:
1. Validates the event payload (raises ValidationError on bad input → DLQ).
2. Reads the current state of all affected denormalised tables.
3. Performs the timestamp/ordering check (raises StaleEventError if the
   event is older than what is already applied).
4. Computes new totals and returns the list of `(prepared-key, params)`
   tuples to be applied in a single logged BATCH.

The main loop appends the `processed_events` and `event_history` inserts to
the same BATCH so that "mark as processed" and "apply state change" are
atomic per event.
"""
from __future__ import annotations

import datetime as dt
import json
from typing import Any, Dict, List, Optional, Tuple

from cassandra_repo import CassandraRepository, OrderItem
from errors import StaleEventError, ValidationError


Statement = Tuple[str, tuple]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _require(event: Dict[str, Any], field: str) -> Any:
    value = event.get(field)
    if value is None:
        raise ValidationError(f"Missing required field '{field}'", code="MISSING_FIELD")
    return value


def _require_positive_qty(event: Dict[str, Any], field: str = "quantity") -> int:
    qty = _require(event, field)
    if not isinstance(qty, int):
        raise ValidationError(f"Field '{field}' must be int, got {type(qty).__name__}",
                              code="VALIDATION_ERROR")
    if qty <= 0:
        raise ValidationError(f"Field '{field}' must be positive, got {qty}",
                              code="VALIDATION_ERROR")
    return qty


def _ts(event: Dict[str, Any]) -> dt.datetime:
    """Extract event timestamp as timezone-aware UTC datetime."""
    raw = event.get("event_timestamp")
    if raw is None:
        raise ValidationError("Missing required field 'event_timestamp'", code="MISSING_FIELD")
    if isinstance(raw, dt.datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=dt.timezone.utc)
    if isinstance(raw, (int, float)):
        return dt.datetime.fromtimestamp(raw / 1000.0, tz=dt.timezone.utc)
    raise ValidationError(f"Unsupported event_timestamp type: {type(raw).__name__}",
                          code="VALIDATION_ERROR")


def _check_order(
    new_ts: dt.datetime,
    existing_state: Optional[Dict[str, Any]],
    entity_desc: str,
) -> None:
    """Raise StaleEventError if existing state has a newer/equal timestamp."""
    if existing_state is None:
        return
    last = existing_state.get("last_event_timestamp")
    if last is None:
        return
    if last.tzinfo is None:
        last = last.replace(tzinfo=dt.timezone.utc)
    if last >= new_ts:
        raise StaleEventError(
            f"Event for {entity_desc} is stale: event_ts={new_ts.isoformat()} <= "
            f"last_applied_ts={last.isoformat()}"
        )


def _history_stmt(
    event: Dict[str, Any],
    product_id: str,
    zone_id: Optional[str],
    quantity: Optional[int],
) -> Statement:
    payload = json.dumps(_event_to_json(event), default=str, ensure_ascii=False)
    return (
        "ins_history",
        (
            product_id,
            _ts(event),
            event["event_id"],
            event["event_type"],
            zone_id,
            quantity,
            payload,
        ),
    )


def processed_stmt(event: Dict[str, Any], now: dt.datetime) -> Statement:
    return (
        "ins_processed",
        (event["event_id"], str(event["event_type"]), now),
    )


def _event_to_json(event: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in event.items():
        if isinstance(v, dt.datetime):
            out[k] = v.isoformat()
        elif hasattr(v, "name") and hasattr(v, "value"):
            out[k] = str(v)
        else:
            out[k] = v
    return out


def _upsert_ipz(
    product_id: str,
    zone_id: str,
    available: int,
    reserved: int,
    supplier_id: Optional[str],
    event: Dict[str, Any],
    now: dt.datetime,
) -> Statement:
    return (
        "upsert_ipz",
        (
            product_id,
            zone_id,
            available,
            reserved,
            supplier_id,
            event["event_id"],
            _ts(event),
            now,
        ),
    )


def _upsert_ip(
    product_id: str,
    total_available: int,
    total_reserved: int,
    event: Dict[str, Any],
    now: dt.datetime,
) -> Statement:
    return (
        "upsert_ip",
        (
            product_id,
            total_available,
            total_reserved,
            event["event_id"],
            _ts(event),
            now,
        ),
    )


def _upsert_iz(
    zone_id: str,
    product_id: str,
    available: int,
    reserved: int,
    event: Dict[str, Any],
    now: dt.datetime,
) -> Statement:
    return (
        "upsert_iz",
        (
            zone_id,
            product_id,
            available,
            reserved,
            event["event_id"],
            _ts(event),
            now,
        ),
    )


def _apply_delta_to_zone(
    repo: CassandraRepository,
    event: Dict[str, Any],
    product_id: str,
    zone_id: str,
    delta_available: int,
    delta_reserved: int,
    *,
    supplier_id: Optional[str] = None,
    set_supplier: bool = False,
    now: dt.datetime,
) -> List[Statement]:
    """Compute statements for a single-zone inventory delta with consistency
    across the three denormalised tables and the product aggregate.
    """
    state_pz = repo.get_inventory_by_product_zone(product_id, zone_id)
    _check_order(_ts(event), state_pz, f"product={product_id} zone={zone_id}")

    cur_available_pz = state_pz["available_quantity"] if state_pz else 0
    cur_reserved_pz = state_pz["reserved_quantity"] if state_pz else 0
    cur_supplier_pz = state_pz["supplier_id"] if state_pz else None

    new_available_pz = cur_available_pz + delta_available
    new_reserved_pz = cur_reserved_pz + delta_reserved
    if new_available_pz < 0:
        raise ValidationError(
            f"available_quantity would become negative ({new_available_pz}) for "
            f"product={product_id} zone={zone_id}",
            code="INSUFFICIENT_STOCK",
        )
    if new_reserved_pz < 0:
        raise ValidationError(
            f"reserved_quantity would become negative ({new_reserved_pz}) for "
            f"product={product_id} zone={zone_id}",
            code="INSUFFICIENT_RESERVATION",
        )

    state_p = repo.get_inventory_by_product(product_id)
    cur_total_available = state_p["total_available"] if state_p else 0
    cur_total_reserved = state_p["total_reserved"] if state_p else 0
    new_total_available = cur_total_available + delta_available
    new_total_reserved = cur_total_reserved + delta_reserved

    new_supplier = supplier_id if set_supplier else cur_supplier_pz

    return [
        _upsert_ipz(
            product_id,
            zone_id,
            new_available_pz,
            new_reserved_pz,
            new_supplier,
            event,
            now,
        ),
        _upsert_ip(
            product_id,
            new_total_available,
            new_total_reserved,
            event,
            now,
        ),
        _upsert_iz(
            zone_id,
            product_id,
            new_available_pz,
            new_reserved_pz,
            event,
            now,
        ),
    ]


# ---------------------------------------------------------------------------
# Concrete handlers
# ---------------------------------------------------------------------------
def handle_product_received(
    event: Dict[str, Any], repo: CassandraRepository, now: dt.datetime
) -> List[Statement]:
    product_id = _require(event, "product_id")
    zone_id = _require(event, "zone_id")
    quantity = _require_positive_qty(event)
    supplier_id = event.get("supplier_id")  # v2 only; may be absent for v1

    stmts = _apply_delta_to_zone(
        repo,
        event,
        product_id=product_id,
        zone_id=zone_id,
        delta_available=+quantity,
        delta_reserved=0,
        supplier_id=supplier_id,
        set_supplier=supplier_id is not None,
        now=now,
    )
    stmts.append(_history_stmt(event, product_id, zone_id, quantity))
    return stmts


def handle_product_shipped(
    event: Dict[str, Any], repo: CassandraRepository, now: dt.datetime
) -> List[Statement]:
    product_id = _require(event, "product_id")
    zone_id = _require(event, "zone_id")
    quantity = _require_positive_qty(event)

    stmts = _apply_delta_to_zone(
        repo,
        event,
        product_id=product_id,
        zone_id=zone_id,
        delta_available=-quantity,
        delta_reserved=0,
        now=now,
    )
    stmts.append(_history_stmt(event, product_id, zone_id, quantity))
    return stmts


def handle_product_moved(
    event: Dict[str, Any], repo: CassandraRepository, now: dt.datetime
) -> List[Statement]:
    product_id = _require(event, "product_id")
    from_zone = _require(event, "from_zone_id")
    to_zone = _require(event, "to_zone_id")
    quantity = _require_positive_qty(event)
    if from_zone == to_zone:
        raise ValidationError("from_zone_id and to_zone_id must differ",
                              code="VALIDATION_ERROR")

    new_ts = _ts(event)

    state_from = repo.get_inventory_by_product_zone(product_id, from_zone)
    _check_order(new_ts, state_from, f"product={product_id} zone={from_zone}")
    state_to = repo.get_inventory_by_product_zone(product_id, to_zone)
    _check_order(new_ts, state_to, f"product={product_id} zone={to_zone}")

    cur_av_from = state_from["available_quantity"] if state_from else 0
    cur_rv_from = state_from["reserved_quantity"] if state_from else 0
    cur_sup_from = state_from["supplier_id"] if state_from else None
    new_av_from = cur_av_from - quantity
    if new_av_from < 0:
        raise ValidationError(
            f"Not enough stock in {from_zone}: have {cur_av_from}, need {quantity}",
            code="INSUFFICIENT_STOCK",
        )

    cur_av_to = state_to["available_quantity"] if state_to else 0
    cur_rv_to = state_to["reserved_quantity"] if state_to else 0
    cur_sup_to = state_to["supplier_id"] if state_to else None
    new_av_to = cur_av_to + quantity

    state_p = repo.get_inventory_by_product(product_id)
    cur_total_available = state_p["total_available"] if state_p else 0
    cur_total_reserved = state_p["total_reserved"] if state_p else 0
    # Moving inventory does not change the global total.

    stmts: List[Statement] = []
    stmts.append(_upsert_ipz(product_id, from_zone, new_av_from, cur_rv_from,
                             cur_sup_from, event, now))
    stmts.append(_upsert_iz(from_zone, product_id, new_av_from, cur_rv_from, event, now))
    stmts.append(_upsert_ipz(product_id, to_zone, new_av_to, cur_rv_to,
                             cur_sup_to, event, now))
    stmts.append(_upsert_iz(to_zone, product_id, new_av_to, cur_rv_to, event, now))
    stmts.append(_upsert_ip(product_id, cur_total_available, cur_total_reserved, event, now))
    stmts.append(_history_stmt(event, product_id, f"{from_zone}->{to_zone}", quantity))
    return stmts


def handle_product_reserved(
    event: Dict[str, Any], repo: CassandraRepository, now: dt.datetime
) -> List[Statement]:
    product_id = _require(event, "product_id")
    zone_id = _require(event, "zone_id")
    quantity = _require_positive_qty(event)

    stmts = _apply_delta_to_zone(
        repo,
        event,
        product_id=product_id,
        zone_id=zone_id,
        delta_available=-quantity,
        delta_reserved=+quantity,
        now=now,
    )
    stmts.append(_history_stmt(event, product_id, zone_id, quantity))
    return stmts


def handle_product_released(
    event: Dict[str, Any], repo: CassandraRepository, now: dt.datetime
) -> List[Statement]:
    product_id = _require(event, "product_id")
    zone_id = _require(event, "zone_id")
    quantity = _require_positive_qty(event)

    stmts = _apply_delta_to_zone(
        repo,
        event,
        product_id=product_id,
        zone_id=zone_id,
        delta_available=+quantity,
        delta_reserved=-quantity,
        now=now,
    )
    stmts.append(_history_stmt(event, product_id, zone_id, quantity))
    return stmts


def handle_inventory_counted(
    event: Dict[str, Any], repo: CassandraRepository, now: dt.datetime
) -> List[Statement]:
    product_id = _require(event, "product_id")
    zone_id = _require(event, "zone_id")
    counted = event.get("counted_quantity")
    if counted is None:
        counted = event.get("quantity")
    if counted is None:
        raise ValidationError("INVENTORY_COUNTED requires 'counted_quantity'",
                              code="MISSING_FIELD")
    if not isinstance(counted, int) or counted < 0:
        raise ValidationError(f"counted_quantity must be non-negative int, got {counted!r}",
                              code="VALIDATION_ERROR")

    new_ts = _ts(event)

    state_pz = repo.get_inventory_by_product_zone(product_id, zone_id)
    _check_order(new_ts, state_pz, f"product={product_id} zone={zone_id}")

    cur_available_pz = state_pz["available_quantity"] if state_pz else 0
    cur_reserved_pz = state_pz["reserved_quantity"] if state_pz else 0
    cur_supplier_pz = state_pz["supplier_id"] if state_pz else None
    delta = counted - cur_available_pz

    state_p = repo.get_inventory_by_product(product_id)
    cur_total_available = state_p["total_available"] if state_p else 0
    cur_total_reserved = state_p["total_reserved"] if state_p else 0
    new_total_available = cur_total_available + delta

    stmts = [
        _upsert_ipz(product_id, zone_id, counted, cur_reserved_pz, cur_supplier_pz, event, now),
        _upsert_iz(zone_id, product_id, counted, cur_reserved_pz, event, now),
        _upsert_ip(product_id, new_total_available, cur_total_reserved, event, now),
        _history_stmt(event, product_id, zone_id, counted),
    ]
    return stmts


def _coerce_items(items: Any) -> List[Dict[str, Any]]:
    if not items:
        raise ValidationError("Order has empty 'items'", code="VALIDATION_ERROR")
    result = []
    for it in items:
        if not isinstance(it, dict):
            raise ValidationError(f"items must be a list of objects, got {type(it).__name__}",
                                  code="VALIDATION_ERROR")
        pid = it.get("product_id")
        zid = it.get("zone_id")
        qty = it.get("quantity")
        if not pid or not zid or not isinstance(qty, int) or qty <= 0:
            raise ValidationError(f"Invalid order item: {it!r}", code="VALIDATION_ERROR")
        result.append({"product_id": pid, "zone_id": zid, "quantity": qty})
    return result


def handle_order_created(
    event: Dict[str, Any], repo: CassandraRepository, now: dt.datetime
) -> List[Statement]:
    order_id = _require(event, "order_id")
    items = _coerce_items(event.get("items"))

    existing_order = repo.get_order(order_id)
    if existing_order is not None and existing_order.get("status") in ("CREATED", "COMPLETED"):
        raise ValidationError(
            f"Order {order_id} already exists with status={existing_order['status']}",
            code="ORDER_ALREADY_EXISTS",
        )

    stmts: List[Statement] = []
    new_ts = _ts(event)

    aggregated: Dict[str, Tuple[int, int]] = {}  # product_id -> (delta_available, delta_reserved)
    per_zone: Dict[Tuple[str, str], Tuple[int, int]] = {}

    for item in items:
        pid, zid, qty = item["product_id"], item["zone_id"], item["quantity"]
        state_pz = repo.get_inventory_by_product_zone(pid, zid)
        _check_order(new_ts, state_pz, f"product={pid} zone={zid}")
        cur_av_pz = state_pz["available_quantity"] if state_pz else 0
        cur_rv_pz = state_pz["reserved_quantity"] if state_pz else 0
        cur_sup_pz = state_pz["supplier_id"] if state_pz else None

        new_av_pz = cur_av_pz - qty
        new_rv_pz = cur_rv_pz + qty
        if new_av_pz < 0:
            raise ValidationError(
                f"Cannot reserve {qty} of {pid} in {zid}: only {cur_av_pz} available",
                code="INSUFFICIENT_STOCK",
            )
        stmts.append(_upsert_ipz(pid, zid, new_av_pz, new_rv_pz, cur_sup_pz, event, now))
        stmts.append(_upsert_iz(zid, pid, new_av_pz, new_rv_pz, event, now))

        agg_a, agg_r = aggregated.get(pid, (0, 0))
        aggregated[pid] = (agg_a - qty, agg_r + qty)

    for pid, (da, dr) in aggregated.items():
        state_p = repo.get_inventory_by_product(pid)
        cur_total_a = state_p["total_available"] if state_p else 0
        cur_total_r = state_p["total_reserved"] if state_p else 0
        stmts.append(_upsert_ip(pid, cur_total_a + da, cur_total_r + dr, event, now))
        stmts.append(_history_stmt(event, pid, None, sum(it["quantity"] for it in items
                                                          if it["product_id"] == pid)))

    items_param = [OrderItem(it["product_id"], it["zone_id"], it["quantity"]) for it in items]
    stmts.append(
        (
            "upsert_order",
            (order_id, "CREATED", items_param, now, now, event["event_id"], new_ts),
        )
    )
    return stmts


def handle_order_completed(
    event: Dict[str, Any], repo: CassandraRepository, now: dt.datetime
) -> List[Statement]:
    order_id = _require(event, "order_id")
    order = repo.get_order(order_id)
    if order is None:
        raise ValidationError(f"Cannot complete unknown order {order_id}",
                              code="ORDER_NOT_FOUND")
    if order["status"] == "COMPLETED":
        raise StaleEventError(f"Order {order_id} is already COMPLETED")
    if order["status"] != "CREATED":
        raise ValidationError(
            f"Cannot complete order {order_id} in status {order['status']}",
            code="INVALID_ORDER_STATE",
        )

    stmts: List[Statement] = []
    new_ts = _ts(event)

    per_product_delta_reserved: Dict[str, int] = {}
    for item in order["items"]:
        pid, zid, qty = item["product_id"], item["zone_id"], item["quantity"]
        state_pz = repo.get_inventory_by_product_zone(pid, zid)
        _check_order(new_ts, state_pz, f"product={pid} zone={zid}")
        cur_av_pz = state_pz["available_quantity"] if state_pz else 0
        cur_rv_pz = state_pz["reserved_quantity"] if state_pz else 0
        cur_sup_pz = state_pz["supplier_id"] if state_pz else None
        new_rv_pz = cur_rv_pz - qty
        if new_rv_pz < 0:
            raise ValidationError(
                f"Reserved underflow for {pid} in {zid}: have {cur_rv_pz}, need {qty}",
                code="INSUFFICIENT_RESERVATION",
            )
        stmts.append(_upsert_ipz(pid, zid, cur_av_pz, new_rv_pz, cur_sup_pz, event, now))
        stmts.append(_upsert_iz(zid, pid, cur_av_pz, new_rv_pz, event, now))
        per_product_delta_reserved[pid] = per_product_delta_reserved.get(pid, 0) - qty

    for pid, dr in per_product_delta_reserved.items():
        state_p = repo.get_inventory_by_product(pid)
        cur_total_a = state_p["total_available"] if state_p else 0
        cur_total_r = state_p["total_reserved"] if state_p else 0
        stmts.append(_upsert_ip(pid, cur_total_a, cur_total_r + dr, event, now))
        stmts.append(_history_stmt(event, pid, None, abs(dr)))

    items_param = [
        OrderItem(it["product_id"], it["zone_id"], it["quantity"]) for it in order["items"]
    ]
    stmts.append(
        (
            "upsert_order",
            (order_id, "COMPLETED", items_param, now, now, event["event_id"], new_ts),
        )
    )
    return stmts


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------
HANDLERS = {
    "PRODUCT_RECEIVED": handle_product_received,
    "PRODUCT_SHIPPED": handle_product_shipped,
    "PRODUCT_MOVED": handle_product_moved,
    "PRODUCT_RESERVED": handle_product_reserved,
    "PRODUCT_RELEASED": handle_product_released,
    "INVENTORY_COUNTED": handle_inventory_counted,
    "ORDER_CREATED": handle_order_created,
    "ORDER_COMPLETED": handle_order_completed,
}


def compute_batch(
    event: Dict[str, Any], repo: CassandraRepository, now: dt.datetime
) -> List[Statement]:
    event_type = event.get("event_type")
    if event_type is None:
        raise ValidationError("Missing required field 'event_type'", code="MISSING_FIELD")
    event_type = str(event_type)
    handler = HANDLERS.get(event_type)
    if handler is None:
        raise ValidationError(f"Unknown event_type: {event_type}", code="UNKNOWN_EVENT_TYPE")
    if not event.get("event_id"):
        raise ValidationError("Missing required field 'event_id'", code="MISSING_FIELD")
    return handler(event, repo, now)
