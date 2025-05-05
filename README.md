# Smart Warehouse — Event-Driven State Management with Cassandra

Эта реализация выполняет все 10 пунктов задания «Smart Warehouse» (`statements.txt`):
event-driven система управления складом на базе **Kafka + Schema Registry + Cassandra**,
устойчивая к сбоям, с DLQ, мониторингом и поддержкой schema evolution.

## TL;DR — запуск одной командой

```bash
docker compose up --build -d
```

После того как все health-чеки станут зелёными (≈ 60–120 c):

```bash
# Отправить событие
docker compose run --rm producer received \
    --product SKU-001 --zone ZONE-A --quantity 100

# Запустить все E2E-сценарии из задания
bash scripts/scenarios.sh

# Открыть мониторинг
open http://localhost:8000/health
open http://localhost:8000/metrics
open http://localhost:9090            # Prometheus
open http://localhost:3000            # Grafana (anonymous viewer)
```

Для остановки и полной очистки состояния:

```bash
docker compose down -v
```

---

## Что разворачивается

| Сервис             | Порт   | Назначение                                          |
|--------------------|--------|-----------------------------------------------------|
| `zookeeper`        | —      | координация Kafka                                   |
| `kafka`            | 9094   | брокер сообщений (PLAINTEXT_HOST для хост-клиентов) |
| `schema-registry`  | 8081   | реестр Avro-схем                                    |
| `cassandra-1..3`   | 9042   | 3-нодовый кластер Cassandra (DC=dc1, RF=3)          |
| `consumer`         | 8000   | сервис-потребитель: `/health`, `/metrics`           |
| `prometheus`       | 9090   | сбор метрик                                         |
| `grafana`          | 3000   | дашборд «Smart Warehouse — Consumer»                |
| `kafka-init`       | —      | one-shot: создаёт топики + регистрирует схемы v1/v2 |
| `cassandra-init`   | —      | one-shot: применяет `cassandra-init/init.cql`       |
| `producer`         | —      | CLI-утилита для отправки событий (profile `cli`)    |

---

## Архитектура

```
                                  ┌─────────────────────┐
                                  │  Schema Registry    │
                                  │ (Avro v1, v2;       │
                                  │  BACKWARD compat)   │
                                  └──────────┬──────────┘
                                             │
   ┌─────────────┐   produce    ┌────────────▼───────────┐  consume   ┌─────────────────┐
   │  Producer   │ ───────────▶ │  Kafka                 │ ─────────▶ │  Consumer       │
   │  (CLI)      │              │  topic: warehouse-events│            │  Python service │
   └─────────────┘              │  partitions: 3          │            │  • idempotency  │
                                │                         │            │  • ordering     │
                                │  topic: warehouse-events-dlq         │  • BATCH writes │
                                └─────────────────────────┘            │  • DLQ producer │
                                             ▲                         │  • /metrics     │
                                             │ DLQ                     │  • /health      │
                                             └─────────────────────────┤                 │
                                                                       └────────┬────────┘
                                                                                │ BATCH (QUORUM)
                            ┌─────────────────┬──────────────────┬──────────────▼────────────┐
                            │  cassandra-1    │  cassandra-2     │  cassandra-3              │
                            │      ┌──────────┴──────────────────┴────────┐                  │
                            │      │  keyspace warehouse  (NTS, RF=3)     │                  │
                            │      │  inventory_by_product_zone           │                  │
                            │      │  inventory_by_product                │                  │
                            │      │  inventory_by_zone                   │                  │
                            │      │  processed_events  (TTL 30 days)     │                  │
                            │      │  event_history                       │                  │
                            │      │  orders                              │                  │
                            │      └──────────────────────────────────────┘                  │
                            └──────────────────────────────────────────────────────────────────┘

                         ┌────────────────┐        ┌──────────────────────┐
                         │  Prometheus    │ ─────▶ │  Grafana dashboard   │
                         │  (alerts.yml)  │        │  (lag, throughput,   │
                         └────────┬───────┘        │   errors, latency)   │
                                  │ scrape          └──────────────────────┘
                                  ▼
                         consumer:/metrics
```

---

## Модель данных в Cassandra

Все таблицы спроектированы **под запросы** (query-first). JOIN'ов нет;
денормализация выполнена осознанно и обеспечивается одним **logged BATCH** на каждое событие.
`init.cql` лежит в `cassandra-init/init.cql`.

| Таблица                       | Partition key | Clustering key       | Под какие запросы                                   |
|-------------------------------|---------------|----------------------|-----------------------------------------------------|
| `inventory_by_product_zone`   | `product_id`  | `zone_id`            | «Сколько товара X в зоне Y?» / «Остатки товара X по всем зонам» |
| `inventory_by_product`        | `product_id`  | —                    | «Сколько всего товара X на складе?» (агрегат)        |
| `inventory_by_zone`           | `zone_id`     | `product_id`         | «Какие товары и сколько лежит в зоне Y?»             |
| `processed_events`            | `event_id`    | —                    | Идемпотентность (TTL 30 дней)                        |
| `event_history`               | `product_id`  | `event_timestamp DESC, event_id` | Аудит-лог последних событий по товару   |
| `orders`                      | `order_id`    | —                    | Состояние заказа (CREATED/COMPLETED) и его позиции   |

### Обоснование выборов

- **`inventory_by_product_zone` PK=`product_id`, CK=`zone_id`.**
  Большая часть запросов — «остаток товара X в зоне Y» и «остатки товара X по всем зонам».
  Партиция по продукту даёт **локальный single-partition** доступ и для одиночного зон,
  и для перечисления зон одного товара. Перекоса нет: при сотнях тысяч SKU и десятках зон
  партиции остаются маленькими (≪ 100 МБ).
- **`inventory_by_zone` PK=`zone_id`, CK=`product_id`.**
  Обратное представление под запрос «что лежит в зоне Y». Тот же ряд, но партиционируется
  по зоне. Поддерживается тем же BATCH'ем, что и основная таблица.
- **`inventory_by_product` PK=`product_id`.**
  Денормализованный агрегат по всем зонам. Поддерживается consumer'ом эажeрно: при каждом
  событии пересчитываем `total_available/total_reserved` и пишем в этот ряд тем же BATCH'ем.
  Альтернатива (counter table) была отвергнута, т. к. counters несовместимы с обычными
  колонками и не поддерживают условные обновления для проверки порядка событий.
- **`processed_events` PK=`event_id`.**
  Лук-ап по event_id за O(1). TTL 30 дней удерживает таблицу маленькой и автоматически
  чистит «горячий хвост» дубликатов. 30 дней значительно больше реалистичной задержки
  доставки в Kafka.
- **`event_history` PK=`product_id`, CK=`event_timestamp DESC, event_id`.**
  Аудит-лог. Кластеризация DESC по времени делает выборку «последних N событий по товару»
  тривиальной (`LIMIT N`).
- **`orders` PK=`order_id`.**
  Простой look-up по идентификатору заказа. Позиции хранятся как `list<frozen<order_item>>`,
  чтобы атомарно сохранять снимок заказа.

### Маппинг событий в изменения состояния

| Событие              | `inventory_by_product_zone`                                | `inventory_by_product`                          | `orders`                              |
|----------------------|-------------------------------------------------------------|--------------------------------------------------|----------------------------------------|
| `PRODUCT_RECEIVED`   | `available += quantity` (опц. `supplier_id`)               | `total_available += quantity`                    | —                                      |
| `PRODUCT_SHIPPED`    | `available -= quantity`                                    | `total_available -= quantity`                    | —                                      |
| `PRODUCT_MOVED`      | `available -= q` в `from_zone`, `available += q` в `to_zone` | без изменений (move = zero-sum)                | —                                      |
| `PRODUCT_RESERVED`   | `available -= q`, `reserved += q`                          | `total_available -= q`, `total_reserved += q`    | —                                      |
| `PRODUCT_RELEASED`   | `reserved -= q`, `available += q`                          | `total_reserved -= q`, `total_available += q`    | —                                      |
| `INVENTORY_COUNTED`  | `available = counted`                                      | пересчитан агрегат по дельте                     | —                                      |
| `ORDER_CREATED`      | резерв по каждой позиции (как `RESERVED`)                  | пересчитан агрегат                               | новая запись со статусом `CREATED`     |
| `ORDER_COMPLETED`    | `reserved -= q` по позициям (available не меняется)        | `total_reserved -= q`                            | статус → `COMPLETED`                   |

---

## Семантика обработки

### At-least-once + offset commit после записи

`enable.auto.commit=false`. Offset фиксируется **после** того, как BATCH в Cassandra
выполнен успешно. Если консьюмер падает между `execute_batch()` и `commit()` — событие
будет переотправлено, и idempotency-фильтр (см. ниже) корректно пропустит дубликат.

### Идемпотентность (пункт 4)

Перед обработкой события consumer делает `SELECT event_id FROM processed_events WHERE event_id=?`.
Если событие уже обрабатывалось — оно молча пропускается и offset фиксируется.

Вставка в `processed_events` входит в **тот же BATCH**, что и обновления инвентаря,
поэтому переход «событие обработано ↔ состояние обновлено» атомарен (logged batch
гарантирует «все или ничего»).

### Консистентность денормализованных таблиц (пункт 5)

Все обновления для одного события (inventory_by_product_zone, inventory_by_product,
inventory_by_zone, processed_events, event_history и, опционально, orders) выполняются
**в одном `LOGGED BATCH`** на consistency level `QUORUM`. Не бывает ситуации, когда одна
из таблиц обновлена, а другая — нет.

### Обработка событий вне порядка (пункт 6)

В каждой строке `inventory_by_*` мы храним `last_event_timestamp`. Перед обновлением
consumer сравнивает `event.event_timestamp` со старым значением — если событие старше
или равно последнему обработанному, оно молча пропускается и помечается как обработанное
(в `processed_events`), чтобы повторные доставки тоже сразу отсекались.

Партиционирование Kafka по `product_id` (или `order_id` для заказов) гарантирует строгий
порядок Kafka-доставки внутри одного логического агрегата; проверка по timestamp защищает
от ситуаций, когда события генерируются распределённо и приходят логически вне порядка.

### Dead-Letter Queue (пункт 7)

Любой `ValidationError` (отрицательное количество, неизвестный `event_type`,
не хватает обязательного поля, проблема при десериализации Avro) приводит к публикации
конверта в **`warehouse-events-dlq`**:

```json
{
  "original_event": { ... десериализованное событие или raw_base64 ... },
  "error_reason": "Field 'quantity' must be positive, got -5",
  "error_code": "VALIDATION_ERROR",
  "failed_at": "2026-04-01T12:00:00Z",
  "kafka_metadata": {
    "topic": "warehouse-events",
    "partition": 2,
    "offset": 12345,
    "key": "SKU-005",
    "timestamp_ms": 1733212345678
  }
}
```

DLQ-продьюсер настроен с `enable.idempotence=true, acks=all` (надёжная доставка).
Consumer **не падает**: после DLQ offset фиксируется и обработка следующего события
продолжается. Ошибки Cassandra (`DriverException`) НЕ отправляются в DLQ — это
транзиентные ошибки инфраструктуры, и offset не фиксируется, чтобы событие было
повторено после восстановления.

### Cassandra cluster и consistency level (пункт 8)

- **Кластер:** 3 ноды (`cassandra-1/2/3`), `NetworkTopologyStrategy`, RF=3, DC=dc1.
- **Запись:** `QUORUM` — большинство (2 из 3) реплик. При выпадении одной ноды система
  продолжает писать; при выпадении двух — write завершается с ошибкой (retry).
- **Чтение:** `QUORUM`. Я сознательно выбрал именно `QUORUM`, потому что consumer
  делает read-modify-write для подсчёта новых остатков, и `R + W > N` (2 + 2 > 3) даёт
  строгую консистентность: чтение **гарантированно** видит все committed-операции.
  Альтернатива `ONE` была бы быстрее, но при недавно выпадавшей ноде могла бы вернуть
  устаревшее значение и привести к расхождению между подсчитанным и фактическим
  состоянием. Trade-off — латентность ради корректности; для warehouse state это
  правильный выбор.

Сценарий отказоустойчивости (см. `scripts/scenarios.sh` сценарий 6): при `docker stop
cassandra-2` consumer продолжает обрабатывать события — `QUORUM` достижим (2 из 3).

### Мониторинг (пункт 9)

Эндпоинты `consumer`:
- `GET /health` — 200 если Kafka отвечает (последний poll < 60 c) **и** Cassandra
  отвечает на `SELECT now() FROM system.local`. Иначе 503.
- `GET /metrics` — Prometheus-exposition.

Метрики:

| Метрика                                  | Тип       | Лейблы         | Смысл                                                       |
|------------------------------------------|-----------|----------------|-------------------------------------------------------------|
| `events_processed_total`                 | Counter   | `event_type`   | Успешно обработанные события                                 |
| `events_skipped_total`                   | Counter   | `reason`       | Пропущенные (`duplicate` / `stale`)                          |
| `events_dlq_total`                       | Counter   | `error_code`   | Отправленные в DLQ                                           |
| `event_processing_duration_seconds`      | Histogram | `event_type`   | E2E-время обработки одного события                           |
| `cassandra_write_errors_total`           | Counter   | —              | Ошибки записи в Cassandra                                    |
| `consumer_lag`                           | Gauge     | `topic`, `partition` | HEAD − committed offset (отставание consumer)          |
| `kafka_connected`, `cassandra_connected` | Gauge     | —              | 0/1 — текущее состояние подключений                          |

Prometheus собирает их каждые 5 с (`monitoring/prometheus.yml`); правила алертов
`monitoring/alerts.yml` поднимают `ConsumerLagHigh`, `ConsumerDown`, `CassandraWriteErrors`.

Grafana-дашборд **«Smart Warehouse — Consumer»** (UID `warehouse-consumer`) разворачивается
автоматически. В нём 7 панелей: lag по партициям, throughput по типу события, ошибки
Cassandra, DLQ-rate, skipped, латентность p50/p95/p99, статусы подключений.

### Schema Evolution (пункт 10)

- Стратегия совместимости: **BACKWARD**. Это значит, что consumer на новой схеме (v2)
  должен уметь читать сообщения, написанные старой (v1). Конкретно: новые поля в v2
  обязаны иметь `default`.
- Subject в Schema Registry: `warehouse-events-value` (TopicNameStrategy).
- При старте `kafka-init` регистрирует **обе** версии:
  1. v1 — `schemas/warehouse_event_v1.avsc`
  2. v2 — `schemas/warehouse_event_v2.avsc` (поле `supplier_id: ["null", "string"], default: null`)
- Consumer создаёт `AvroDeserializer` **без** фиксированной reader-схемы — каждое
  сообщение разворачивается по writer-схеме, что прозрачно поддерживает обе версии.
- В Cassandra `inventory_by_product_zone` есть колонка `supplier_id`; для v1-событий
  она остаётся `null`, для v2-событий — заполняется значением из события.

#### Как добавить новую версию схемы события

1. Скопировать `schemas/warehouse_event_v2.avsc` в `warehouse_event_v3.avsc`.
2. Добавить новое поле **с `default`** (например, `default: null` для `["null", T]`).
3. Зарегистрировать схему:
   ```bash
   curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data "{\"schema\": $(jq -Rs . < schemas/warehouse_event_v3.avsc)}" \
        http://localhost:8081/subjects/warehouse-events-value/versions
   ```
4. Если новое поле нужно сохранять — добавить колонку в Cassandra:
   `ALTER TABLE warehouse.inventory_by_product_zone ADD new_field text;`
5. В `consumer/handlers.py` — прочитать поле через `event.get("new_field")` и
   передать в `_upsert_ipz`. Для v1/v2-сообщений значение будет `None`.

Никакого даунтайма consumer'а не требуется (новый код backward-compatible с v1+v2).

---

## E2E-сценарии задания

Все 8 сценариев из задания собраны в один скрипт:

```bash
bash scripts/scenarios.sh            # 1..8
bash scripts/scenarios.sh 4 5 6      # выборочно
```

Краткое отображение:

| #  | Сценарий                                  | Подтверждает пункты |
|----|-------------------------------------------|----------------------|
| 1  | Базовый цикл (receive → reserve → move → ship → order) | 1, 2, 3 |
| 2  | Идемпотентность (тот же `event_id` дважды) | 4 |
| 3  | Консистентность 3 таблиц после одного события | 5 |
| 4  | События вне порядка по timestamp           | 6 |
| 5  | DLQ при `quantity=-5`, consumer не падает  | 7 |
| 6  | Кластер Cassandra: `docker stop cassandra-2`, продолжаем писать | 8 |
| 7  | `/health`, `/metrics`, Grafana, consumer lag | 9 |
| 8  | v1- и v2-события вместе, `supplier_id`     | 10 |

---

## Producer CLI

```bash
docker compose run --rm producer received --product SKU-001 --zone ZONE-A --quantity 100
docker compose run --rm producer received --product SKU-001 --zone ZONE-A --quantity 100 \
                                          --supplier SUP-001 --schema-version v2
docker compose run --rm producer shipped  --product SKU-001 --zone ZONE-A --quantity 10
docker compose run --rm producer moved    --product SKU-001 --from-zone ZONE-A --to-zone ZONE-B --quantity 20
docker compose run --rm producer reserved --product SKU-001 --zone ZONE-A --quantity 30
docker compose run --rm producer released --product SKU-001 --zone ZONE-A --quantity 5
docker compose run --rm producer counted  --product SKU-001 --zone ZONE-A --counted 100
docker compose run --rm producer order-created   --order ORD-1 --item SKU-001:ZONE-A:15
docker compose run --rm producer order-completed --order ORD-1

# Идемпотентность: тот же event_id
docker compose run --rm producer received --product SKU-X --zone ZONE-A --quantity 50 \
                                          --event-id evt-fixed-001

# Конкретный timestamp (out-of-order сценарий)
docker compose run --rm producer received --product SKU-Y --zone ZONE-A --quantity 100 \
                                          --timestamp 2026-04-01T12:00:00Z

# Невалидное событие (для DLQ)
docker compose run --rm producer received --product SKU-Z --zone ZONE-A --quantity -5
```

---

## Запросы к Cassandra

```bash
docker exec -it cassandra-1 cqlsh
```

```sql
USE warehouse;

-- Остаток конкретного товара в конкретной зоне
SELECT * FROM inventory_by_product_zone WHERE product_id='SKU-001' AND zone_id='ZONE-A';

-- Все зоны, в которых лежит товар
SELECT * FROM inventory_by_product_zone WHERE product_id='SKU-001';

-- Все товары в зоне
SELECT * FROM inventory_by_zone WHERE zone_id='ZONE-A';

-- Агрегированный остаток
SELECT * FROM inventory_by_product WHERE product_id='SKU-001';

-- История событий по товару (последние 20)
SELECT event_timestamp, event_type, zone_id, quantity
FROM event_history WHERE product_id='SKU-001' LIMIT 20;

-- Заказ
SELECT * FROM orders WHERE order_id='ORD-1';

-- Статус кластера
EXIT;
docker exec -it cassandra-1 nodetool status
```

---

## Структура репозитория

```
.
├── docker-compose.yml
├── README.md
├── statements.txt                    # исходное задание
├── cassandra-init/
│   └── init.cql                      # keyspace + 6 таблиц + UDT order_item
├── consumer/                         # Python consumer service
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── config.py                     # env-based config
│   ├── logging_setup.py              # JSON-логи
│   ├── metrics.py                    # Prometheus метрики
│   ├── cassandra_repo.py             # prepared statements, BATCH, CL
│   ├── dlq.py                        # DLQ producer
│   ├── errors.py                     # ValidationError / StaleEventError
│   ├── handlers.py                   # 8 хэндлеров событий
│   ├── http_server.py                # Flask: /health, /metrics
│   └── main.py                       # entry point + main loop
├── producer/                         # CLI для отправки событий
│   ├── Dockerfile
│   ├── requirements.txt
│   └── events.py
├── schemas/
│   ├── warehouse_event_v1.avsc
│   └── warehouse_event_v2.avsc
├── monitoring/
│   ├── prometheus.yml
│   ├── alerts.yml
│   └── grafana/
│       ├── provisioning/
│       │   ├── datasources/datasource.yml
│       │   └── dashboards/dashboards.yml
│       └── dashboards/warehouse.json
└── scripts/
    ├── init_kafka.py                 # one-shot: создаёт топики + регистрирует v1/v2 в SR
    └── scenarios.sh                  # все 8 E2E-сценариев
```

---

## Troubleshooting

- **Cassandra не успевает подняться** — у `cassandra-init` есть retry-loop на 60 итераций
  по 2 c. Если кластер всё ещё стартует, перезапустите контейнер: `docker compose up -d
  cassandra-init`.
- **`consumer` не подключается к Cassandra** — проверьте, что `cassandra-init` выполнился
  без ошибок: `docker compose logs cassandra-init`. Keyspace `warehouse` должен существовать.
- **Schema Registry ошибки совместимости** — проверьте уровень совместимости:
  `curl http://localhost:8081/config/warehouse-events-value`. Должно быть `BACKWARD`.
- **Сброс состояния** — `docker compose down -v` (удаляет тома Cassandra/Kafka/Grafana).
