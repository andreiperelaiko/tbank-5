"""Cassandra repository: connection, prepared statements, BATCH writes."""
from __future__ import annotations

import collections
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, NoHostAvailable, Session
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.query import BatchStatement, BatchType, PreparedStatement

from config import Config
from metrics import cassandra_connected

log = logging.getLogger(__name__)


OrderItem = collections.namedtuple("OrderItem", ["product_id", "zone_id", "quantity"])


_CONSISTENCY_LEVELS = {
    "ANY": ConsistencyLevel.ANY,
    "ONE": ConsistencyLevel.ONE,
    "TWO": ConsistencyLevel.TWO,
    "THREE": ConsistencyLevel.THREE,
    "QUORUM": ConsistencyLevel.QUORUM,
    "ALL": ConsistencyLevel.ALL,
    "LOCAL_ONE": ConsistencyLevel.LOCAL_ONE,
    "LOCAL_QUORUM": ConsistencyLevel.LOCAL_QUORUM,
    "EACH_QUORUM": ConsistencyLevel.EACH_QUORUM,
}


def _parse_cl(name: str) -> int:
    return _CONSISTENCY_LEVELS[name.upper()]


class CassandraRepository:
    """All Cassandra interactions used by the consumer."""

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._cluster: Optional[Cluster] = None
        self._session: Optional[Session] = None
        self._prepared: Dict[str, PreparedStatement] = {}
        self.write_cl = _parse_cl(cfg.cassandra_write_consistency)
        self.read_cl = _parse_cl(cfg.cassandra_read_consistency)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def connect(self) -> None:
        auth = None
        if self.cfg.cassandra_username:
            auth = PlainTextAuthProvider(
                username=self.cfg.cassandra_username,
                password=self.cfg.cassandra_password,
            )

        last_err: Optional[BaseException] = None
        for attempt in range(1, 31):
            try:
                log.info(
                    "Connecting to Cassandra",
                    extra={
                        "contact_points": self.cfg.cassandra_contact_points,
                        "keyspace": self.cfg.cassandra_keyspace,
                        "attempt": attempt,
                    },
                )
                self._cluster = Cluster(
                    contact_points=self.cfg.cassandra_contact_points,
                    port=self.cfg.cassandra_port,
                    auth_provider=auth,
                    load_balancing_policy=TokenAwarePolicy(
                        DCAwareRoundRobinPolicy(local_dc=self.cfg.cassandra_dc)
                    ),
                    protocol_version=4,
                )
                session = self._cluster.connect(self.cfg.cassandra_keyspace)
                session.default_consistency_level = self.write_cl
                # Register the UDT so that list<frozen<order_item>> columns are
                # accepted as namedtuples on write and returned as namedtuples
                # on read.
                self._cluster.register_user_type(
                    self.cfg.cassandra_keyspace, "order_item", OrderItem
                )
                self._session = session
                self._prepare_statements()
                cassandra_connected.set(1)
                log.info("Connected to Cassandra")
                return
            except Exception as exc:  # noqa: BLE001
                last_err = exc
                log.warning(
                    "Cassandra connect failed, retrying",
                    extra={"attempt": attempt, "error": str(exc)},
                )
                time.sleep(min(5.0, 1.0 + attempt * 0.5))
        cassandra_connected.set(0)
        raise RuntimeError(f"Could not connect to Cassandra after retries: {last_err}")

    def close(self) -> None:
        if self._cluster is not None:
            self._cluster.shutdown()
            self._cluster = None
            self._session = None
        cassandra_connected.set(0)

    def is_healthy(self) -> bool:
        try:
            if self._session is None:
                return False
            self._session.execute("SELECT now() FROM system.local")
            cassandra_connected.set(1)
            return True
        except Exception as exc:  # noqa: BLE001
            log.warning("Cassandra health check failed", extra={"error": str(exc)})
            cassandra_connected.set(0)
            return False

    @property
    def session(self) -> Session:
        if self._session is None:
            raise RuntimeError("Cassandra session is not initialised")
        return self._session

    # ------------------------------------------------------------------
    # Prepared statements
    # ------------------------------------------------------------------
    def _prepare_statements(self) -> None:
        s = self.session
        self._prepared["sel_processed"] = s.prepare(
            "SELECT event_id FROM processed_events WHERE event_id = ?"
        )
        self._prepared["sel_ipz"] = s.prepare(
            "SELECT available_quantity, reserved_quantity, supplier_id, "
            "last_event_id, last_event_timestamp "
            "FROM inventory_by_product_zone WHERE product_id = ? AND zone_id = ?"
        )
        self._prepared["sel_ip"] = s.prepare(
            "SELECT total_available, total_reserved, last_event_id, last_event_timestamp "
            "FROM inventory_by_product WHERE product_id = ?"
        )
        self._prepared["sel_iz"] = s.prepare(
            "SELECT available_quantity, reserved_quantity, last_event_id, last_event_timestamp "
            "FROM inventory_by_zone WHERE zone_id = ? AND product_id = ?"
        )
        self._prepared["sel_order"] = s.prepare(
            "SELECT order_id, status, items, last_event_id, last_event_timestamp "
            "FROM orders WHERE order_id = ?"
        )

        self._prepared["ins_processed"] = s.prepare(
            "INSERT INTO processed_events (event_id, event_type, processed_at) "
            "VALUES (?, ?, ?)"
        )
        self._prepared["ins_history"] = s.prepare(
            "INSERT INTO event_history "
            "(product_id, event_timestamp, event_id, event_type, zone_id, quantity, payload) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        )

        self._prepared["upsert_ipz"] = s.prepare(
            "INSERT INTO inventory_by_product_zone "
            "(product_id, zone_id, available_quantity, reserved_quantity, supplier_id, "
            " last_event_id, last_event_timestamp, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )
        self._prepared["upsert_ip"] = s.prepare(
            "INSERT INTO inventory_by_product "
            "(product_id, total_available, total_reserved, last_event_id, "
            " last_event_timestamp, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)"
        )
        self._prepared["upsert_iz"] = s.prepare(
            "INSERT INTO inventory_by_zone "
            "(zone_id, product_id, available_quantity, reserved_quantity, "
            " last_event_id, last_event_timestamp, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
        self._prepared["upsert_order"] = s.prepare(
            "INSERT INTO orders "
            "(order_id, status, items, created_at, updated_at, last_event_id, last_event_timestamp) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        )

        # Set read consistency on SELECT statements specifically.
        for key in ("sel_processed", "sel_ipz", "sel_ip", "sel_iz", "sel_order"):
            self._prepared[key].consistency_level = self.read_cl
        for key in (
            "ins_processed",
            "ins_history",
            "upsert_ipz",
            "upsert_ip",
            "upsert_iz",
            "upsert_order",
        ):
            self._prepared[key].consistency_level = self.write_cl

    def prepared(self, key: str) -> PreparedStatement:
        return self._prepared[key]

    # ------------------------------------------------------------------
    # Read helpers
    # ------------------------------------------------------------------
    def is_event_processed(self, event_id: str) -> bool:
        row = self.session.execute(self._prepared["sel_processed"], (event_id,)).one()
        return row is not None

    def get_inventory_by_product_zone(
        self, product_id: str, zone_id: str
    ) -> Optional[Dict[str, Any]]:
        row = self.session.execute(self._prepared["sel_ipz"], (product_id, zone_id)).one()
        if row is None:
            return None
        return {
            "available_quantity": row.available_quantity or 0,
            "reserved_quantity": row.reserved_quantity or 0,
            "supplier_id": row.supplier_id,
            "last_event_id": row.last_event_id,
            "last_event_timestamp": row.last_event_timestamp,
        }

    def get_inventory_by_product(self, product_id: str) -> Optional[Dict[str, Any]]:
        row = self.session.execute(self._prepared["sel_ip"], (product_id,)).one()
        if row is None:
            return None
        return {
            "total_available": row.total_available or 0,
            "total_reserved": row.total_reserved or 0,
            "last_event_id": row.last_event_id,
            "last_event_timestamp": row.last_event_timestamp,
        }

    def get_inventory_by_zone(
        self, zone_id: str, product_id: str
    ) -> Optional[Dict[str, Any]]:
        row = self.session.execute(self._prepared["sel_iz"], (zone_id, product_id)).one()
        if row is None:
            return None
        return {
            "available_quantity": row.available_quantity or 0,
            "reserved_quantity": row.reserved_quantity or 0,
            "last_event_id": row.last_event_id,
            "last_event_timestamp": row.last_event_timestamp,
        }

    def get_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        row = self.session.execute(self._prepared["sel_order"], (order_id,)).one()
        if row is None:
            return None
        items = []
        for it in row.items or []:
            items.append(
                {"product_id": it.product_id, "zone_id": it.zone_id, "quantity": it.quantity}
            )
        return {
            "order_id": row.order_id,
            "status": row.status,
            "items": items,
            "last_event_id": row.last_event_id,
            "last_event_timestamp": row.last_event_timestamp,
        }

    # ------------------------------------------------------------------
    # BATCH apply
    # ------------------------------------------------------------------
    def execute_batch(self, statements: List[Tuple[str, tuple]]) -> None:
        """Apply a list of (prepared-statement-key, params) as a logged batch.

        Logged batches give us atomicity across multiple tables: either all
        statements succeed or all fail. This is exactly what we need to keep
        the denormalised inventory tables consistent.
        """
        batch = BatchStatement(batch_type=BatchType.LOGGED, consistency_level=self.write_cl)
        for key, params in statements:
            batch.add(self._prepared[key], params)
        self.session.execute(batch)
