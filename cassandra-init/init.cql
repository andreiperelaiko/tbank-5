-- ============================================================================
-- Smart Warehouse keyspace and tables
--
-- Cluster: 3-node NetworkTopologyStrategy, RF=3 (see docker-compose: dc1)
--
-- Data model is purpose-built for the query patterns required by the spec.
-- All inventory tables are denormalized and kept consistent within a single
-- logged BATCH on every event.  See README.md for design rationale.
-- ============================================================================

CREATE KEYSPACE IF NOT EXISTS warehouse
    WITH replication = {
        'class': 'NetworkTopologyStrategy',
        'dc1': 3
    }
    AND durable_writes = true;

USE warehouse;

-- ----------------------------------------------------------------------------
-- 1. Inventory at the (product, zone) granularity.
--    Query: "What is the available/reserved quantity of product X in zone Y?"
--    PK = product_id, CK = zone_id  →  one partition per product, rows per zone.
--    supplier_id added for Schema Evolution v2 (PRODUCT_RECEIVED).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS inventory_by_product_zone (
    product_id           text,
    zone_id              text,
    available_quantity   bigint,
    reserved_quantity    bigint,
    supplier_id          text,
    last_event_id        text,
    last_event_timestamp timestamp,
    updated_at           timestamp,
    PRIMARY KEY ((product_id), zone_id)
);

-- ----------------------------------------------------------------------------
-- 2. Aggregated inventory per product across all zones.
--    Query: "What is the total available/reserved for product X?"
--    PK = product_id.  Maintained eagerly by the consumer on every event.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS inventory_by_product (
    product_id           text PRIMARY KEY,
    total_available      bigint,
    total_reserved       bigint,
    last_event_id        text,
    last_event_timestamp timestamp,
    updated_at           timestamp
);

-- ----------------------------------------------------------------------------
-- 3. Inventory grouped by zone — useful for "What's in zone Y?".
--    PK = zone_id, CK = product_id  →  partition per zone, rows per product.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS inventory_by_zone (
    zone_id              text,
    product_id           text,
    available_quantity   bigint,
    reserved_quantity    bigint,
    last_event_id        text,
    last_event_timestamp timestamp,
    updated_at           timestamp,
    PRIMARY KEY ((zone_id), product_id)
);

-- ----------------------------------------------------------------------------
-- 4. Processed events log — used for idempotent processing of duplicates.
--    Before applying an event, the consumer checks if event_id is already here.
--    TTL of 30 days keeps the table bounded.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     text PRIMARY KEY,
    event_type   text,
    processed_at timestamp
) WITH default_time_to_live = 2592000;  -- 30 days

-- ----------------------------------------------------------------------------
-- 5. Event history per product — append-only audit log.
--    PK = product_id, CK = (event_timestamp DESC, event_id) for "give me the
--    most recent N events for product X" reads.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS event_history (
    product_id      text,
    event_timestamp timestamp,
    event_id        text,
    event_type      text,
    zone_id         text,
    quantity        int,
    payload         text,
    PRIMARY KEY ((product_id), event_timestamp, event_id)
) WITH CLUSTERING ORDER BY (event_timestamp DESC, event_id ASC);

-- ----------------------------------------------------------------------------
-- 6. Orders table — ORDER_CREATED / ORDER_COMPLETED state machine.
--    items is a frozen list of (product_id, zone_id, quantity) tuples.
-- ----------------------------------------------------------------------------
CREATE TYPE IF NOT EXISTS order_item (
    product_id text,
    zone_id    text,
    quantity   int
);

CREATE TABLE IF NOT EXISTS orders (
    order_id             text PRIMARY KEY,
    status               text,
    items                list<frozen<order_item>>,
    created_at           timestamp,
    updated_at           timestamp,
    last_event_id        text,
    last_event_timestamp timestamp
);
