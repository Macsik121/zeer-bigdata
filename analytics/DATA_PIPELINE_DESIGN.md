# Zeer Marketplace — Проектирование конвейера обработки больших данных

**Версия:** 1.0
**Дата:** 2026-09-24
**Статус:** Проектирование
**Границы:** этапы 1–4 (Сбор → Хранение → Преобразование → Обработка). Этапы 5 (ML) и 6 (визуализация) вне границ документа.

**Входные документы:**
- `analytics/METRICS_CATALOG.md` — каталог из 28 метрик, определяет состав витрин
- `analytics/LOG_ANALYTICS_DESIGN.md` — схемы трёх типов логов, таксономии, SLA
- `docs/servers-infrastructure-software-implementation-plan.md` — топология кластера
- `configs/adcm-service-config.md` — параметры сервисов
- `docs/DIPLOMA_DOCUMENTATION_STRUCTURE.md` — требования FR/NFR

---

## 1. Введение

### 1.1 Цель

Спроектировать конвейер, который принимает три потока логов Zeer Marketplace, надёжно сохраняет их в распределённом хранилище, приводит к аналитической модели и вычисляет 28 метрик из каталога с заданными интервалами (от 5 минут до недели).

### 1.2 Требования, определяющие архитектуру

| ID | Требование | Как влияет на проект |
|----|-----------|---------------------|
| **FR-4** | Детект крэшей в near-real-time | Нужен слой Structured Streaming, читающий Kafka напрямую, а не HDFS |
| **FR-5** | Пакетная аналитика поведения и конверсии | Нужен слой batch поверх исторических данных |
| **FR-6** | SQL-доступ для BI через Hive | Core и Mart регистрируются как external tables |
| **NFR-1** | Latency < 5 мин (streaming), < 1 ч (batch) | Триггер микробатча ≤ 1 мин, watermark 2 мин |
| **NFR-3** | Retention: 90 сут raw, 365 сут агрегатов | См. расчёт ёмкости, раздел 5.6 — требование выполнимо не буквально |
| **NFR-4** | Горизонтальная масштабируемость | Партиционирование по дате, отсутствие состояния в job'ах |

### 1.3 Ключевые архитектурные решения

| № | Решение | Обоснование | Отвергнутая альтернатива |
|---|---------|-------------|-------------------------|
| **AD-1** | Transactional Outbox + Debezium CDC как основной путь доставки из PostgreSQL | Гарантирует, что каждое записанное в БД событие попадёт в Kafka; сохраняет порядок; не требует переписывания бизнес-логики | Прямой producer из Node.js — теряет события при недоступности Kafka; Sqoop — не обеспечивает окно 5 мин для C1/C4/D2 |
| **AD-2** | Kafka как шина, Flume как загрузчик в HDFS | Kafka даёт **двух независимых потребителей** одного потока: Flume → HDFS (batch) и Spark Streaming → витрины (near-real-time) | Только Flume — нет replay и второго потребителя; только Kafka Connect — лишний компонент, не развёрнут в ADCM |
| **AD-3** | ELT вместо ETL | Пороги метрик подлежат пересчёту после 30 суток наблюдений (требование каталога) — нужна возможность пересчитать историю | ETL — трансформация до загрузки делает пересчёт невозможным без повторного извлечения из источника |
| **AD-4** | Spark DataFrame + Structured Streaming как единственный вычислитель | Один API для batch и streaming, переиспользование кода трансформаций | MapReduce — многостадийные агрегации с join дают 5–10× проигрыш на дисковом I/O; Hive как вычислитель — хуже тестируется и не даёт контроля над DQ |
| **AD-5** | Airflow для оркестрации | Между слоями есть жёсткие зависимости (DQ Gate блокирует публикацию в Mart), нужен backfill для пересчёта порогов | cron — нет зависимостей, retry и backfill; Oozie — XML-конфигурация, не развёрнут в ADCM |
| **AD-6** | Раздельные очереди YARN для streaming и batch | Полный профиль Spark из `adcm-service-config.md` (6 executors × 6 ГБ) занимает **весь** кластер — streaming и batch не могут работать одновременно | Единая очередь — batch-job вытеснил бы постоянно работающие streaming-job'ы |

---

## 2. Метрики — сводка

Полный каталог: `analytics/METRICS_CATALOG.md`. Здесь — распределение по режимам, определяющее состав job'ов.

| Режим | Метрики | Job | Интервал |
|-------|---------|-----|----------|
| Structured Streaming | C1, C2 | `CrashStreamingJob` | окно 5 мин |
| Structured Streaming | C4, C5, C7 | `InjectionStreamingJob` | окно 5 мин |
| Structured Streaming | D1, D2, D3, D5 | `SecurityStreamingJob` | окна 5 мин / 10 мин / 1 ч |
| Spark Batch | B1, B4, C6 | `InjectionBatchJob` | 1×/час |
| Spark Batch | C3 | `CrashBatchJob` (часовой режим) | 1×/час |
| Spark Batch | A1 | `UserActionsBatchJob` | 1×/сут |
| Spark Batch | A2 | `FunnelBatchJob` | 1×/сут |
| Spark Batch | A3 | `RevenueBatchJob` | 1×/сут |
| Spark Batch | A5 | `PromoBatchJob` | 1×/сут |
| Spark Batch | A6 | `CommerceBatchJob` | 1×/сут |
| Spark Batch | B2, C1 (сверка) | `CrashBatchJob` (суточный режим) | 1×/сут |
| Spark Batch | B5, D4, D6 | `SecurityBatchJob` | 1×/сут |
| Spark Batch | A4 | `ChurnBatchJob` | 1×/нед |
| Spark Batch | B3 | `PlatformBatchJob` | 1×/нед |
| Служебное | E3 | `DataQualityGateJob` | на каждом прогоне ELT |
| Служебное | E2 | встроено во все streaming-job'ы | каждый микробатч |
| Prometheus | E1, E4 | вне Spark (JMX, YARN REST API) | pull 15 с / 60 с |

**Итого: 3 streaming-job'а + 11 batch-job'ов + 2 служебных.**

---

## 3. Архитектура конвейера

```
┌────────────────────────────────────────────────────────────────────────────────────┐
│ ИСТОЧНИКИ (Windows-хост, 192.168.1.2)                                              │
│                                                                                     │
│  Web UI ──GraphQL──┐                                                               │
│                     ├──▶ zeer-marketplace-api (Express) ──▶ PostgreSQL             │
│  Game Client ───────┤     /api_loader/crash_logs              ├── action_logs       │
│  (Loader API)       │     /api_loader/log_inject_hacks        ├── crash_logs        │
│                     │     /api_loader/inject_dll_preload      ├── inject_logs       │
│  Admin Panel ───────┘     /api_loader/block_user              └── outbox_events ◀── │
│                                                                    (AD-1)          │
└──────────────────────────────────────┬─────────────────────────────────────────────┘
                                       │ logical replication (WAL, wal2json)
                                       ▼
┌────────────────────────────────────────────────────────────────────────────────────┐
│ 1. СБОР (Ingestion)                                     zeer-worker1..3            │
│                                                                                     │
│  ┌──────────────────────┐                                                          │
│  │ Debezium PostgreSQL  │  читает WAL → публикует изменения outbox_events          │
│  │ Connector            │  (Kafka Connect, worker1)                                 │
│  └──────────┬───────────┘                                                          │
│             ▼                                                                       │
│  ┌─────────────────────────────────────────────────────────────────────┐           │
│  │ KAFKA  (3 брокера, RF=3, min.insync.replicas=2, retention 168 ч)    │           │
│  │  ├─ zeer-user-events       6 партиций   key = user_id               │           │
│  │  ├─ zeer-crash-events      6 партиций   key = user_id               │           │
│  │  └─ zeer-injection-events  6 партиций   key = user_id               │           │
│  └───────┬─────────────────────────────────────────────┬───────────────┘           │
│          │                                             │                            │
│   группа │ flume-consumer                 группы spark-*-stream                     │
│          ▼                                             ▼                            │
│  ┌────────────────────┐                    ┌──────────────────────────┐            │
│  │ FLUME ×2 (HA)      │                    │ (в раздел 4 — Processing)│            │
│  │ worker1, worker2   │                    │  Structured Streaming    │            │
│  │ KafkaSource        │                    │  читает Kafka напрямую   │            │
│  │  → MemoryChannel   │                    │  БЕЗ промежуточного HDFS │            │
│  │  → HDFS Sink       │                    └──────────────────────────┘            │
│  └────────┬───────────┘                                                            │
└───────────┼────────────────────────────────────────────────────────────────────────┘
            ▼
┌────────────────────────────────────────────────────────────────────────────────────┐
│ 2. ХРАНЕНИЕ (HDFS, RF=2, block 128 МБ)                                             │
│                                                                                     │
│  /zeer/raw/       Avro, партиции topic/YYYY/MM/DD      TTL 30 сут   immutable      │
│  /zeer/staging/   Parquet+Snappy, типизация + dedup    TTL 7 сут    временный      │
│  /zeer/quarantine/ Parquet, не прошедшие DQ            TTL 30 сут                  │
│  /zeer/core/      Parquet+Snappy, партиция event_date  TTL 90 сут   каноническая   │
│  /zeer/mart/      Parquet+Snappy, витрины под метрики  TTL 365 сут                 │
│  /zeer/checkpoints/ состояние Structured Streaming                                 │
└───────────┬────────────────────────────────────────────────────────────────────────┘
            ▼
┌────────────────────────────────────────────────────────────────────────────────────┐
│ 3. ПРЕОБРАЗОВАНИЕ (ELT, Spark SQL)          очередь YARN: batch                    │
│                                                                                     │
│   Raw ──parse+dedup──▶ Staging ──DQ Gate──▶ Core ──агрегация──▶ Mart               │
│    Avro                 Parquet    │ fail     Parquet            Parquet           │
│                                    ▼                                                │
│                              Quarantine (E3)                                        │
└───────────┬────────────────────────────────────────────────────────────────────────┘
            ▼
┌────────────────────────────────────────────────────────────────────────────────────┐
│ 4. ОБРАБОТКА (Processing)                                                          │
│                                                                                     │
│  ОЧЕРЕДЬ streaming (35 %)          │  ОЧЕРЕДЬ batch (65 %)                         │
│  ─────────────────────────         │  ──────────────────────────                   │
│  CrashStreamingJob      C1 C2      │  InjectionBatchJob   B1 B4 C6   1×/час        │
│  InjectionStreamingJob  C4 C5 C7   │  CrashBatchJob       B2 C1 C3   1×/час,сут    │
│  SecurityStreamingJob   D1 D2 D3 D5│  UserActionsBatchJob A1         1×/сут        │
│                                    │  FunnelBatchJob      A2         1×/сут        │
│  работают 24/7                     │  RevenueBatchJob     A3         1×/сут        │
│  checkpoint в HDFS                 │  PromoBatchJob       A5         1×/сут        │
│                                    │  CommerceBatchJob    A6         1×/сут        │
│                                    │  SecurityBatchJob    B5 D4 D6   1×/сут        │
│                                    │  ChurnBatchJob       A4         1×/нед        │
│                                    │  PlatformBatchJob    B3         1×/нед        │
│                                    │  CompactionJob                  1×/сут        │
│                                    │  оркестрация: Airflow (master)                │
└───────────┬────────────────────────────────────────────────────────────────────────┘
            ▼
      Hive external tables (Metastore на zeer-master:9083)  →  этапы 5–6 (вне границ)
```

**Ключевой момент диаграммы** — Kafka обслуживает двух независимых потребителей. Flume пишет в HDFS для batch-слоя, Structured Streaming читает тот же топик напрямую. Если бы streaming читал HDFS, минимальная задержка равнялась бы `rollInterval` Flume (600 с), и требование NFR-1 (< 5 мин) выполнить было бы нельзя.

---

## 4. Этап 1. Сбор (Ingestion)

### 4.1 Доставка из PostgreSQL в Kafka

Источник данных — PostgreSQL на Windows-хосте, вне кластера. Рассмотрены три способа.

| Способ | Полнота | Задержка | Нагрузка на источник | Вердикт |
|--------|---------|----------|---------------------|---------|
| Прямой Kafka-producer из Node.js API | **Нет.** При недоступности Kafka событие теряется, в БД оно уже записано | секунды | нет | Отвергнут как единственный путь |
| Sqoop / внешние таблицы, batch-импорт | Да | ≥ интервал импорта (минуты–часы) | высокая: полное сканирование таблиц | Отвергнут для real-time, **принят для исторической загрузки** |
| **Outbox + Debezium CDC** | **Да.** Запись в outbox идёт в одной транзакции с бизнес-данными | секунды | низкая: чтение WAL, не таблиц | **Принят как основной** |

#### 4.1.1 Таблица outbox в PostgreSQL

Приложение в **одной транзакции** записывает бизнес-данные и событие в outbox. Атомарность гарантирует, что расхождения между БД и Kafka не возникнет.

```sql
CREATE TABLE outbox_events (
    id            BIGSERIAL PRIMARY KEY,
    aggregate_id  TEXT        NOT NULL,   -- user_id, становится ключом партиционирования Kafka
    event_type    TEXT        NOT NULL,   -- user_action | crash | injection
    payload       JSONB       NOT NULL,   -- тело события по схеме LOG_ANALYTICS_DESIGN.md
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published     BOOLEAN     NOT NULL DEFAULT false
);

CREATE INDEX idx_outbox_unpublished ON outbox_events (id) WHERE NOT published;
```

Настройка логической репликации (`postgresql.conf` на Windows-хосте):

```conf
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4
listen_addresses = '*'
```

```conf
# pg_hba.conf — доступ Debezium с воркера кластера
host    replication     debezium    192.168.1.0/24    scram-sha-256
host    zeer            debezium    192.168.1.0/24    scram-sha-256
```

#### 4.1.2 Debezium PostgreSQL Connector

Разворачивается в Kafka Connect на `zeer-worker1`.

```json
{
  "name": "zeer-postgres-outbox",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "192.168.1.2",
    "database.port": "5432",
    "database.user": "debezium",
    "database.password": "${file:/opt/connect/secrets.properties:pg_password}",
    "database.dbname": "zeer",
    "topic.prefix": "zeer-cdc",
    "plugin.name": "pgoutput",
    "slot.name": "zeer_outbox_slot",
    "publication.autocreate.mode": "filtered",
    "table.include.list": "public.outbox_events",
    "snapshot.mode": "initial",

    "transforms": "outbox",
    "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
    "transforms.outbox.table.field.event.id": "id",
    "transforms.outbox.table.field.event.key": "aggregate_id",
    "transforms.outbox.table.field.event.type": "event_type",
    "transforms.outbox.table.field.event.payload": "payload",
    "transforms.outbox.route.by.field": "event_type",
    "transforms.outbox.route.topic.replacement": "zeer-${routedByValue}-events",

    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": false,

    "heartbeat.interval.ms": "10000",
    "max.batch.size": "2048",
    "max.queue.size": "8192"
  }
}
```

`EventRouter` маршрутизирует записи по полю `event_type` в три существующих топика: `user_action → zeer-user-events`, `crash → zeer-crash-events`, `injection → zeer-injection-events`.

> **Важно про `heartbeat.interval.ms`.** Без heartbeat слот репликации в PostgreSQL не продвигает LSN при отсутствии изменений в отслеживаемой таблице, и WAL начинает накапливаться на диске Windows-хоста. Параметр обязателен.

#### 4.1.3 Историческая загрузка (backfill)

Для первичной загрузки накопленных данных и для пересчёта метрик после изменения порогов используется Sqoop — в обход Kafka, напрямую в Raw-зону:

```bash
sqoop import \
  --connect jdbc:postgresql://192.168.1.2:5432/zeer \
  --username zeer_ro --password-file hdfs:///user/hadoop/.sqoop_pass \
  --table action_logs \
  --where "created_at >= '2026-01-01' AND created_at < '2026-09-01'" \
  --target-dir hdfs://zeer-master:8020/zeer/raw/zeer-user-events/backfill \
  --as-avrodatafile \
  --compression-codec snappy \
  --split-by id \
  --num-mappers 6
```

`--num-mappers 6` соответствует 6 vcores, доступным в очереди batch (раздел 7.4).

### 4.2 Конфигурация топиков Kafka

Топики уже созданы согласно `adcm-service-config.md`. Проектные уточнения:

| Параметр | Значение | Обоснование |
|----------|----------|-------------|
| Партиций | 6 на топик | 2 на брокер; обеспечивает параллелизм потребления, кратный 3 брокерам |
| `replication.factor` | 3 | Переживает отказ одного брокера без потери данных |
| `min.insync.replicas` | 2 | Producer с `acks=all` получает подтверждение только при записи на 2 реплики |
| **Ключ партиционирования** | `user_id` | Гарантирует **порядок событий одного пользователя** — критично для D1 (счёт HWID), D5 (геоаномалия), C6 (воронка стадий инжекта) |
| `retention.ms` | 604800000 (7 сут) | Позволяет переиграть неделю при сбое Flume или переразвёртывании streaming-job'а |
| `compression.type` | snappy | Баланс скорости и степени сжатия; уже задан в конфигурации брокеров |

```bash
# Уточнение retention и compaction-политики на существующих топиках
for t in zeer-user-events zeer-crash-events zeer-injection-events; do
  kafka-configs.sh --bootstrap-server zeer-worker1:9092 \
    --entity-type topics --entity-name $t --alter \
    --add-config retention.ms=604800000,cleanup.policy=delete,max.message.bytes=2097152
done
```

`max.message.bytes=2 МБ` — Crash Logs содержат поле `full_stack_trace`, которое может быть объёмным; значение по умолчанию (1 МБ) рискованно.

### 4.3 Конфигурация Flume

Два агента на `zeer-worker1` и `zeer-worker2` в одной consumer-группе `flume-consumer` — Kafka автоматически распределяет между ними 6 партиций (по 3 на агента). При отказе одного агента второй подхватывает все партиции.

**Дельта к текущему конфигу в `configs/adcm-service-config.md`:** формат Raw меняется с `Text` на `Avro`, канал — с `memory` на `file`. Обоснование ниже.

```properties
# /opt/flume/conf/zeer-kafka-hdfs-agent.conf
agent.sources  = kafka-source
agent.channels = file-channel
agent.sinks    = hdfs-sink

# ---------- SOURCE ----------
agent.sources.kafka-source.type = org.apache.flume.source.kafka.KafkaSource
agent.sources.kafka-source.kafka.bootstrap.servers = zeer-worker1:9092,zeer-worker2:9092,zeer-worker3:9092
agent.sources.kafka-source.kafka.topics = zeer-user-events,zeer-crash-events,zeer-injection-events
agent.sources.kafka-source.kafka.consumer.group.id = flume-consumer
agent.sources.kafka-source.kafka.consumer.auto.offset.reset = earliest
agent.sources.kafka-source.kafka.consumer.enable.auto.commit = false
agent.sources.kafka-source.batchSize = 1000
agent.sources.kafka-source.batchDurationMillis = 2000
agent.sources.kafka-source.setTopicHeader = true
agent.sources.kafka-source.topicHeader = topic
agent.sources.kafka-source.channels = file-channel

agent.sources.kafka-source.interceptors = ts-interceptor host-interceptor
agent.sources.kafka-source.interceptors.ts-interceptor.type = timestamp
agent.sources.kafka-source.interceptors.host-interceptor.type = host
agent.sources.kafka-source.interceptors.host-interceptor.useIP = false
agent.sources.kafka-source.interceptors.host-interceptor.hostHeader = ingest_host

# ---------- CHANNEL ----------
# FileChannel вместо MemoryChannel: memory-канал теряет до 100 000 событий
# при падении агента, т.к. события хранятся только в heap.
agent.channels.file-channel.type = file
agent.channels.file-channel.checkpointDir = /opt/flume/checkpoint
agent.channels.file-channel.dataDirs = /opt/flume/data
agent.channels.file-channel.capacity = 1000000
agent.channels.file-channel.transactionCapacity = 10000
agent.channels.file-channel.checkpointInterval = 30000
agent.channels.file-channel.maxFileSize = 2146435071
agent.channels.file-channel.useDualCheckpoints = true

# ---------- SINK ----------
agent.sinks.hdfs-sink.type = hdfs
agent.sinks.hdfs-sink.hdfs.path = hdfs://zeer-master:8020/zeer/raw/%{topic}/year=%Y/month=%m/day=%d
agent.sinks.hdfs-sink.hdfs.filePrefix = %{ingest_host}-events
agent.sinks.hdfs-sink.hdfs.fileSuffix = .avro
agent.sinks.hdfs-sink.hdfs.inUsePrefix = _
agent.sinks.hdfs-sink.hdfs.rollInterval = 600
agent.sinks.hdfs-sink.hdfs.rollSize = 134217728
agent.sinks.hdfs-sink.hdfs.rollCount = 0
agent.sinks.hdfs-sink.hdfs.batchSize = 1000
agent.sinks.hdfs-sink.hdfs.fileType = DataStream
agent.sinks.hdfs-sink.serializer = org.apache.flume.sink.hdfs.AvroEventSerializer$Builder
agent.sinks.hdfs-sink.serializer.compressionCodec = snappy
agent.sinks.hdfs-sink.serializer.schemaURL = hdfs://zeer-master:8020/zeer/schemas/raw_event.avsc
agent.sinks.hdfs-sink.hdfs.callTimeout = 30000
agent.sinks.hdfs-sink.hdfs.idleTimeout = 0
agent.sinks.hdfs-sink.channel = file-channel
```

**Пояснения к принятым значениям:**

| Параметр | Значение | Почему именно так |
|----------|----------|------------------|
| `channel.type = file` | file | MemoryChannel при падении агента теряет всё содержимое (до 100 000 событий по текущему конфигу). FileChannel переживает перезапуск ценой ~20 % пропускной способности |
| `enable.auto.commit = false` | false | Offset коммитится только после успешной записи в HDFS → at-least-once без потерь |
| `inUsePrefix = _` | `_` | Файлы в процессе записи получают префикс `_`, и Spark их игнорирует при чтении — иначе batch-job читал бы неполный файл |
| `filePrefix = %{ingest_host}` | имя хоста | Два агента пишут в один каталог; без разделения по хосту имена файлов конфликтуют |
| `rollInterval = 600` | 600 с | Компромисс: меньше — растёт число мелких файлов, больше — растёт задержка появления данных в Raw |

**Схема Avro для Raw-зоны** (`hdfs:///zeer/schemas/raw_event.avsc`) — обёртка, сохраняющая тело события как строку. Разбор происходит на слое Staging, что и делает подход ELT:

```json
{
  "type": "record",
  "name": "RawEvent",
  "namespace": "com.zeer.raw",
  "fields": [
    {"name": "headers", "type": {"type": "map", "values": "string"}},
    {"name": "body",    "type": "bytes"}
  ]
}
```

### 4.4 Гарантии доставки и дедупликация

Сквозная гарантия конвейера — **at-least-once**. Exactly-once не достигается: Flume коммитит offset после записи в HDFS, и падение между записью и коммитом приводит к повторной доставке.

Дубли устраняются на слое Staging по `event_id` (поле присутствует во всех трёх схемах логов):

| Участок | Гарантия | Механизм |
|---------|----------|----------|
| Приложение → PostgreSQL | exactly-once | Транзакция БД |
| PostgreSQL → Kafka | at-least-once | Debezium переотправляет с последнего подтверждённого LSN |
| Kafka → HDFS (Flume) | at-least-once | Ручной коммит offset после записи |
| Kafka → Streaming | exactly-once в пределах job'а | Checkpoint + идемпотентная запись в Parquet |
| Staging (дедупликация) | **effectively-once** | `dropDuplicates(["event_id"])` в окне 24 ч |

---

## 5. Этап 2. Хранение (Storage)

### 5.1 Структура зон HDFS

```
/zeer/
├── schemas/                        # Avro-схемы (.avsc), версионируются
├── raw/                            # Зона 1: сырые события, immutable
│   ├── zeer-user-events/year=2026/month=09/day=24/*.avro
│   ├── zeer-crash-events/year=…
│   └── zeer-injection-events/year=…
├── staging/                        # Зона 2: типизировано + дедуплицировано
│   ├── stg_user_actions/event_date=2026-09-24/*.parquet
│   ├── stg_crashes/event_date=…
│   └── stg_injections/event_date=…
├── quarantine/                     # Зона 3: не прошли DQ Gate (метрика E3)
│   └── {table}/event_date=…/reject_reason=…/*.parquet
├── core/                           # Зона 4: каноническая модель, обогащено
│   ├── core_user_actions/event_date=…/*.parquet
│   ├── core_crashes/event_date=…/*.parquet
│   └── core_injections/event_date=…/*.parquet
├── mart/                           # Зона 5: витрины под метрики каталога
│   ├── mart_business_daily/
│   ├── mart_retention_weekly/
│   ├── mart_product_usage_hourly/
│   ├── mart_product_usage_daily/
│   ├── mart_platform_weekly/
│   ├── mart_stability_5min/
│   ├── mart_stability_daily/
│   ├── mart_injection_5min/
│   ├── mart_injection_hourly/
│   ├── mart_security_events/
│   └── mart_pipeline_health/
├── checkpoints/                    # Состояние Structured Streaming
│   ├── crash-stream/
│   ├── injection-stream/
│   └── security-stream/
└── _tmp/                           # Промежуточные результаты job'ов
```

### 5.2 Выбор форматов

| Зона | Формат | Кодек | Обоснование |
|------|--------|-------|-------------|
| **Raw** | Avro | Snappy | Схема встроена в файл → чтение не ломается при изменении схемы логов (это ловит метрика E3). Row-oriented — подходит для записи потоком. Splittable при Snappy |
| **Staging / Core / Mart** | Parquet | Snappy | Колоночный: агрегации из каталога (`COUNT DISTINCT`, `GROUP BY`, перцентили) читают 2–5 колонок из 20 → экономия I/O в 4–10 раз. Predicate pushdown по `event_date`. Сжатие ~8–10× относительно JSON |

Gzip отвергнут: не splittable, один файл обрабатывается одной задачей Spark. Snappy splittable и втрое быстрее на распаковку при разнице в степени сжатия ~15 %.

### 5.3 Схема партиционирования

| Зона | Ключ партиционирования | Обоснование |
|------|----------------------|-------------|
| Raw | `topic / year / month / day` | Задаётся escape-последовательностями Flume; сутки — минимальная единица переобработки |
| Staging, Core | `event_date` (DATE) | Все batch-job'ы фильтруют по дате → partition pruning отсекает 99 % данных |
| `mart_stability_5min`, `mart_injection_5min` | `event_date / window_hour` | Витрины 5-минутных окон: 288 строк/сут; партиция по часу даёт 12 строк на партицию и быстрый доступ к последнему часу |
| Остальные Mart | `event_date` или `week_start` | Соответствует интервалу пересчёта метрики |

Партиционирование по часу на слое Core **не применяется**: при суточном объёме до 15 ГБ это дало бы 24 партиции по 600 МБ, а с учётом трёх таблиц — 72 каталога в сутки и 26 тыс. в год, что избыточно нагружает Metastore без выигрыша в скорости.

### 5.4 Проблема мелких файлов

**Расчёт.** Flume: 2 агента × 3 топика × (86400 / 600) = **288 файлов в сутки**. За год — 105 тыс. файлов только в Raw. NameNode держит ~150 байт метаданных на файл в heap: 105 000 × 150 ≈ 16 МБ в год. Само по себе не критично, но каждая задача Spark, читающая мелкий файл, тратит больше времени на открытие файла, чем на чтение.

**Решение — трёхуровневое:**

1. **`rollSize = 128 МБ`** в Flume совпадает с `dfs.blocksize` — при высокой нагрузке файл закрывается по размеру ровно на границе блока, без «хвостов».
2. **`CompactionJob`** — суточный job, объединяющий Raw-файлы прошедших суток в файлы целевого размера:

```python
# jobs/compaction_job.py
TARGET_FILE_BYTES = 128 * 1024 * 1024

def compact_partition(spark, topic: str, day: str) -> None:
    src = f"hdfs:///zeer/raw/{topic}/year={day[:4]}/month={day[5:7]}/day={day[8:10]}"
    df = spark.read.format("avro").load(src)

    # Число выходных файлов = объём партиции / целевой размер, минимум 1
    size_bytes = (
        spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem
        .get(spark.sparkContext._jsc.hadoopConfiguration())
        .getContentSummary(spark.sparkContext._jvm.org.apache.hadoop.fs.Path(src))
        .getLength()
    )
    num_files = max(1, int(size_bytes / TARGET_FILE_BYTES) + 1)

    (df.coalesce(num_files)
       .write.mode("overwrite").format("avro")
       .option("compression", "snappy")
       .save(src + "/_compacted"))
```

3. **`coalesce()` перед каждой записью** в Staging/Core/Mart — без него `spark.sql.shuffle.partitions = 18` порождает 18 файлов на каждую запись независимо от объёма.

### 5.5 Hive External Tables — DDL

Metastore развёрнут на `zeer-master:9083` (PostgreSQL-бэкенд). Таблицы внешние: удаление таблицы не удаляет данные в HDFS.

```sql
CREATE DATABASE IF NOT EXISTS zeer_core
  LOCATION 'hdfs://zeer-master:8020/zeer/core';

-- ============ CORE: действия пользователей ============
CREATE EXTERNAL TABLE IF NOT EXISTS zeer_core.core_user_actions (
    event_id          STRING,
    event_ts          TIMESTAMP,
    user_id           STRING,
    session_id        STRING,
    action_type       STRING,
    action_category   STRING,
    product_id        STRING,
    product_name      STRING,
    promo_code        STRING,
    key_code          STRING,
    admin_user_id     STRING,
    target_user_id    STRING,
    status            STRING,
    error_code        STRING,
    duration_ms       INT,
    -- обогащение слоя Core
    ip_hash           STRING,
    geo_country       STRING,
    geo_city          STRING,
    ua_browser        STRING,
    ua_browser_ver    STRING,
    ua_os             STRING,
    ua_device_type    STRING,
    ingested_at       TIMESTAMP
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET
LOCATION 'hdfs://zeer-master:8020/zeer/core/core_user_actions'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- ============ CORE: крэши ============
CREATE EXTERNAL TABLE IF NOT EXISTS zeer_core.core_crashes (
    event_id               STRING,
    event_ts               TIMESTAMP,
    user_id                STRING,
    steam_id               STRING,
    product_id             STRING,
    product_name           STRING,
    exception_code         STRING,
    exception_description  STRING,
    exception_category     STRING,   -- обогащение: таксономия Windows
    time_in_game_seconds   INT,
    stack_trace_hash       STRING,   -- SHA-256; полный trace не хранится (раздел 8, приватность)
    stack_top_module       STRING,   -- верхний фрейм: client.dll и т.п.
    game_version           STRING,
    cheat_version          STRING,
    os_name                STRING,
    os_version             STRING,
    os_build               STRING,
    hw_cpu                 STRING,
    hw_gpu                 STRING,
    hw_ram_gb              INT,
    inject_session_id      STRING,
    ingested_at            TIMESTAMP
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET
LOCATION 'hdfs://zeer-master:8020/zeer/core/core_crashes'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- ============ CORE: инжекты ============
CREATE EXTERNAL TABLE IF NOT EXISTS zeer_core.core_injections (
    event_id            STRING,
    event_ts            TIMESTAMP,
    user_id             STRING,
    steam_id            STRING,
    product_id          STRING,
    product_name        STRING,
    hwid                STRING,
    windows_name        STRING,
    windows_build       STRING,
    ip_hash             STRING,
    geo_country         STRING,
    geo_city            STRING,
    inject_stage        STRING,
    status              STRING,
    error_code          STRING,
    loader_version      STRING,
    driver_version      STRING,
    anticheat_detected  ARRAY<STRING>,
    injection_method    STRING,
    duration_ms         INT,
    inject_session_id   STRING,
    ingested_at         TIMESTAMP
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET
LOCATION 'hdfs://zeer-master:8020/zeer/core/core_injections'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
```

Примеры витрин слоя Mart:

```sql
CREATE DATABASE IF NOT EXISTS zeer_mart
  LOCATION 'hdfs://zeer-master:8020/zeer/mart';

-- Витрина метрик C4, C5, C7 (5-минутные окна)
CREATE EXTERNAL TABLE IF NOT EXISTS zeer_mart.mart_injection_5min (
    window_start        TIMESTAMP,
    window_end          TIMESTAMP,
    product_id          STRING,
    attempts_total      BIGINT,
    attempts_success    BIGINT,
    success_rate        DOUBLE,   -- C4
    duration_p50_ms     DOUBLE,   -- C5
    duration_p95_ms     DOUBLE,   -- C5
    duration_p99_ms     DOUBLE,   -- C5
    err_hwid_mismatch   BIGINT,   -- C7
    err_no_license      BIGINT,
    err_frozen          BIGINT,
    err_wrong_creds     BIGINT,
    err_banned          BIGINT,
    err_anticheat       BIGINT,
    err_injection_failed BIGINT,
    computed_at         TIMESTAMP
)
PARTITIONED BY (event_date DATE, window_hour INT)
STORED AS PARQUET
LOCATION 'hdfs://zeer-master:8020/zeer/mart/mart_injection_5min'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Витрина метрик C1, C2 (5-минутные окна)
CREATE EXTERNAL TABLE IF NOT EXISTS zeer_mart.mart_stability_5min (
    window_start        TIMESTAMP,
    window_end          TIMESTAMP,
    product_id          STRING,
    cheat_version       STRING,
    crash_count         BIGINT,
    affected_users      BIGINT,
    inject_success_count BIGINT,
    crash_rate_permille DOUBLE,   -- C1
    top_exception_code  STRING,   -- C2
    top_exception_share DOUBLE,   -- C2
    computed_at         TIMESTAMP
)
PARTITIONED BY (event_date DATE, window_hour INT)
STORED AS PARQUET
LOCATION 'hdfs://zeer-master:8020/zeer/mart/mart_stability_5min'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Регистрация новых партиций после записи Spark
MSCK REPAIR TABLE zeer_core.core_user_actions;
```

### 5.6 Расчёт ёмкости и retention

**Проектная мощность** (из `LOG_ANALYTICS_DESIGN.md`, раздел 4.2 и `DIPLOMA_DOCUMENTATION_STRUCTURE.md`, п. 2.3): Action ≈ 10 000 соб/мин, Crash ≈ 100 соб/мин, Inject ≈ 50 соб/мин.

| Поток | Событий/сут | Средний размер | Объём/сут (JSON) | Объём/сут (Avro+Snappy, ×0,25) |
|-------|-------------|----------------|------------------|-------------------------------|
| Action | 14 400 000 | 1,0 КБ | 14,4 ГБ | 3,6 ГБ |
| Crash | 144 000 | 3,0 КБ (stack trace) | 0,43 ГБ | 0,11 ГБ |
| Inject | 72 000 | 1,0 КБ | 0,07 ГБ | 0,02 ГБ |
| **Итого Raw** | **14,6 млн** | | **14,9 ГБ** | **≈ 3,7 ГБ** |

**Доступная ёмкость.** 3 DataNode × 512 ГБ = 1536 ГБ сырых. При `dfs.replication = 2` полезная ёмкость ≈ 768 ГБ; за вычетом резерва ОС и логов YARN — **≈ 700 ГБ**.

| Зона | Объём/сут (с учётом RF=2) | Retention | Занимает |
|------|--------------------------|-----------|----------|
| Raw (Avro+Snappy) | 7,4 ГБ | **30 сут** | 222 ГБ |
| Core (Parquet+Snappy) | 3,4 ГБ | **90 сут** | 306 ГБ |
| Staging (временный) | 3,4 ГБ | 7 сут | 24 ГБ |
| Mart (агрегаты) | 0,1 ГБ | **365 сут** | 37 ГБ |
| Quarantine, checkpoints, spark-logs | — | 30 сут | ≈ 25 ГБ |
| **Итого** | | | **≈ 614 ГБ (88 % от 700 ГБ)** |

> **Вывод, влияющий на требование NFR-3.** Требование «90 суток raw, 365 суток агрегатов» на текущем оборудовании **буквально невыполнимо**: 90 суток Raw заняли бы 666 ГБ, не оставив места ни под Core, ни под витрины. Проект принимает следующее прочтение: 365 суток хранятся **агрегаты слоя Mart**, Raw хранится 30 суток, Core — 90 суток. Полный пересчёт метрик задним числом возможен в пределах 30 суток; за более ранние периоды — только по данным Core (90 суток) либо через повторную выгрузку Sqoop из PostgreSQL. Для выполнения NFR-3 в исходной формулировке требуется расширение дисковой подсистемы до ≈ 1,2 ТБ полезной ёмкости (например, +2 диска по 512 ГБ).

Скрипт применения retention (запускается Airflow ежесуточно):

```bash
#!/usr/bin/env bash
# scripts/apply_retention.sh
set -euo pipefail

purge() {  # $1 — путь, $2 — срок хранения в сутках
  local cutoff; cutoff=$(date -d "$2 days ago" +%Y-%m-%d)
  hdfs dfs -ls -d "$1"/* 2>/dev/null | awk '{print $NF}' | while read -r p; do
    local d="${p##*=}"
    [[ "$d" < "$cutoff" ]] && hdfs dfs -rm -r -skipTrash "$p" && echo "purged $p"
  done
}

for t in zeer-user-events zeer-crash-events zeer-injection-events; do
  purge "/zeer/raw/$t" 30
done
for t in core_user_actions core_crashes core_injections; do
  purge "/zeer/core/$t" 90
done
for t in stg_user_actions stg_crashes stg_injections; do
  purge "/zeer/staging/$t" 7
done
```

---

## 6. Этап 3. Преобразование (ELT через Spark SQL)

### 6.1 Почему ELT, а не ETL

| Критерий | ETL | **ELT (выбран)** |
|----------|-----|------------------|
| Пересчёт метрики при изменении определения | Невозможен: сырые данные не сохранены, нужна повторная выгрузка из PostgreSQL | Возможен: Raw хранится 30 суток, достаточно перезапустить трансформацию |
| Появление нового `action_type` | Job падает или молча отбрасывает запись | Запись сохраняется в Raw как есть, обрабатывается после обновления справочника |
| Нагрузка на PostgreSQL | Высокая: трансформация на источнике или в промежуточном слое | Минимальная: источник только отдаёт WAL |
| Соответствие требованиям | — | Каталог метрик прямо требует пересчёта порогов после 30 суток наблюдений (раздел 5, п. 4) |

Решающий аргумент — раздел 5 каталога метрик: пороги первой версии заданы экспертно и подлежат пересчёту по фактическим перцентилям. Без сохранённого сырого слоя такой пересчёт потребовал бы повторного извлечения данных из production-базы.

### 6.2 Слои трансформации

```
Raw (Avro)  ──①──▶  Staging (Parquet)  ──②──▶  Core (Parquet)  ──③──▶  Mart (Parquet)
  как есть          типизация             обогащение              агрегация
                    дедупликация          DQ Gate                 под метрики
                                              │ fail
                                              ▼
                                         Quarantine
```

| Шаг | Что делает | Job |
|-----|-----------|-----|
| ① | Разбор JSON из `body`, приведение типов, дедупликация по `event_id` | `RawToStagingJob` |
| ② | GeoIP, парсинг user-agent, категоризация `exception_code`, хеширование PII, DQ Gate | `StagingToCoreJob` + `DataQualityGateJob` |
| ③ | Агрегации под конкретные метрики каталога | 11 batch-job'ов (раздел 7.2) |

### 6.3 Шаг ① — Raw → Staging

```python
# etl/raw_to_staging.py
"""Разбор сырых Avro-событий, типизация и дедупликация."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType,
)

# Схемы соответствуют LOG_ANALYTICS_DESIGN.md, раздел 1
USER_ACTION_SCHEMA = StructType([
    StructField("event_id",        StringType()),
    StructField("timestamp",       StringType()),
    StructField("user_id",         StringType()),
    StructField("session_id",      StringType()),
    StructField("action_type",     StringType()),
    StructField("action_category", StringType()),
    StructField("product_id",      StringType()),
    StructField("product_name",    StringType()),
    StructField("promo_code",      StringType()),
    StructField("key_code",        StringType()),
    StructField("admin_user_id",   StringType()),
    StructField("target_user_id",  StringType()),
    StructField("status",          StringType()),
    StructField("error_code",      StringType()),
    StructField("duration_ms",     IntegerType()),
    StructField("metadata", StructType([
        StructField("ip",         StringType()),
        StructField("user_agent", StringType()),
        StructField("platform",   StringType()),
        StructField("browser",    StringType()),
        StructField("referrer",   StringType()),
    ])),
])

INJECTION_SCHEMA = StructType([
    StructField("event_id",           StringType()),
    StructField("timestamp",          StringType()),
    StructField("user_id",            StringType()),
    StructField("steam_id",           StringType()),
    StructField("product_id",         StringType()),
    StructField("product_name",       StringType()),
    StructField("hwid",               StringType()),
    StructField("windows_name",       StringType()),
    StructField("ip",                 StringType()),
    StructField("location",           StringType()),
    StructField("inject_stage",       StringType()),
    StructField("status",             StringType()),
    StructField("error_code",         StringType()),
    StructField("loader_version",     StringType()),
    StructField("driver_version",     StringType()),
    StructField("anticheat_detected", ArrayType(StringType())),
    StructField("injection_method",   StringType()),
    StructField("duration_ms",        IntegerType()),
    StructField("inject_session_id",  StringType()),
])

CRASH_SCHEMA = StructType([
    StructField("event_id",              StringType()),
    StructField("timestamp",             StringType()),
    StructField("user_id",               StringType()),
    StructField("steam_id",              StringType()),
    StructField("product_id",            StringType()),
    StructField("product_name",          StringType()),
    StructField("exception_code",        StringType()),
    StructField("exception_description", StringType()),
    StructField("time_in_game_seconds",  IntegerType()),
    StructField("full_stack_trace",      StringType()),
    StructField("game_version",          StringType()),
    StructField("cheat_version",         StringType()),
    StructField("os_info", StructType([
        StructField("name",    StringType()),
        StructField("version", StringType()),
        StructField("build",   StringType()),
    ])),
    StructField("hardware", StructType([
        StructField("cpu",    StringType()),
        StructField("gpu",    StringType()),
        StructField("ram_gb", IntegerType()),
    ])),
    StructField("inject_session_id", StringType()),
])

TOPIC_CONFIG = {
    "zeer-user-events":      (USER_ACTION_SCHEMA, "stg_user_actions"),
    "zeer-crash-events":     (CRASH_SCHEMA,       "stg_crashes"),
    "zeer-injection-events": (INJECTION_SCHEMA,   "stg_injections"),
}


def read_raw(spark: SparkSession, topic: str, day: str) -> DataFrame:
    """Читает Avro-партицию Raw-зоны за указанные сутки."""
    path = (
        f"hdfs:///zeer/raw/{topic}"
        f"/year={day[:4]}/month={day[5:7]}/day={day[8:10]}"
    )
    # basePath нужен, чтобы Spark не пытался вывести партиции из пути
    return (spark.read.format("avro")
            .option("basePath", f"hdfs:///zeer/raw/{topic}")
            .load(path))


def parse_and_dedup(raw: DataFrame, schema: StructType) -> DataFrame:
    """Разбирает тело события и убирает дубли, внесённые at-least-once доставкой."""
    parsed = (
        raw
        .select(F.from_json(F.col("body").cast("string"), schema).alias("e"))
        .select("e.*")
        # timestamp в логах — ISO-8601 с миллисекундами (LOG_ANALYTICS_DESIGN.md)
        .withColumn("event_ts", F.to_timestamp("timestamp"))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("ingested_at", F.current_timestamp())
        .drop("timestamp")
    )

    # Дедупликация: при повторной доставке выигрывает первая запись.
    # Окно ограничено сутками партиции — дубли из-за at-least-once
    # приходят в пределах минут, поэтому суточного окна достаточно.
    w = Window.partitionBy("event_id").orderBy(F.col("ingested_at").asc())
    return (parsed
            .withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn"))


def run(spark: SparkSession, day: str) -> None:
    for topic, (schema, table) in TOPIC_CONFIG.items():
        raw = read_raw(spark, topic, day)
        if raw.rdd.isEmpty():
            print(f"[skip] {topic}: партиция {day} пуста")
            continue

        stg = parse_and_dedup(raw, schema)

        # Один файл на ~128 МБ; при малом объёме — один файл на партицию
        (stg.coalesce(target_files(stg))
            .write.mode("overwrite")
            .partitionBy("event_date")
            .parquet(f"hdfs:///zeer/staging/{table}"))


def target_files(df: DataFrame, rows_per_file: int = 2_000_000) -> int:
    """Оценка числа выходных файлов по числу строк."""
    return max(1, df.count() // rows_per_file + 1)


if __name__ == "__main__":
    import sys
    from pyspark.sql import Window

    day_arg = sys.argv[1]
    spark_session = (
        SparkSession.builder
        .appName(f"RawToStaging-{day_arg}")
        .enableHiveSupport()
        .getOrCreate()
    )
    run(spark_session, day_arg)
    spark_session.stop()
```

### 6.4 Шаг ② — Staging → Core (обогащение)

```python
# etl/staging_to_core.py
"""Обогащение: GeoIP, user-agent, таксономия исключений, хеширование PII."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# Соль для хеширования PII хранится вне кода
PII_SALT = "${PII_HASH_SALT}"

# Таксономия Windows-исключений — LOG_ANALYTICS_DESIGN.md, раздел 1.2
EXCEPTION_TAXONOMY = {
    "0xC0000005": "MEMORY_ACCESS_VIOLATION",
    "0xC000001D": "ILLEGAL_INSTRUCTION",
    "0xC00000FD": "STACK_OVERFLOW",
    "0x80000003": "DEBUG_BREAKPOINT",
    "0xC0000409": "STACK_BUFFER_OVERRUN",
}


def hash_pii(col):
    """SHA-256 с солью. Приватность: раздел 8 LOG_ANALYTICS_DESIGN.md."""
    return F.sha2(F.concat(F.lit(PII_SALT), F.coalesce(col, F.lit(""))), 256)


def categorize_exception(code_col, trace_col):
    """Категория исключения по коду; античит распознаётся по stack trace."""
    expr = F.when(F.lower(trace_col).rlike("vac|vanguard|battleye|easyanticheat"),
                  F.lit("ANTICHEAT_DETECTION"))
    for code, category in EXCEPTION_TAXONOMY.items():
        expr = expr.when(code_col == code, F.lit(category))
    return expr.otherwise(F.lit("UNKNOWN"))


def parse_user_agent(ua_col):
    """Разбор User-Agent регулярными выражениями.

    Внешняя библиотека (ua-parser) не используется: UDF на Python
    сериализует каждую строку между JVM и Python-процессом, что при
    14 млн строк в сутки даёт неприемлемое замедление. Регулярные
    выражения выполняются нативно в JVM.
    """
    browser = (F.when(ua_col.rlike("Edg/"),     F.lit("Edge"))
                .when(ua_col.rlike("OPR/"),     F.lit("Opera"))
                .when(ua_col.rlike("Chrome/"),  F.lit("Chrome"))
                .when(ua_col.rlike("Firefox/"), F.lit("Firefox"))
                .when(ua_col.rlike("Safari/"),  F.lit("Safari"))
                .otherwise(F.lit("Other")))
    version = F.regexp_extract(ua_col, r"(?:Chrome|Firefox|Safari|Edg|OPR)/(\d+)", 1)
    os_name = (F.when(ua_col.rlike("Windows NT 10"), F.lit("Windows 10/11"))
                .when(ua_col.rlike("Windows NT"),    F.lit("Windows (older)"))
                .when(ua_col.rlike("Mac OS X"),      F.lit("macOS"))
                .when(ua_col.rlike("Android"),       F.lit("Android"))
                .when(ua_col.rlike("Linux"),         F.lit("Linux"))
                .otherwise(F.lit("Other")))
    device = (F.when(ua_col.rlike("Mobile|Android|iPhone"), F.lit("mobile"))
               .when(ua_col.rlike("Tablet|iPad"),           F.lit("tablet"))
               .otherwise(F.lit("desktop")))
    return browser, version, os_name, device


def enrich_geo(df: DataFrame, spark: SparkSession, ip_col: str) -> DataFrame:
    """Обогащение страной и городом по диапазонам IP.

    Справочник GeoIP загружается как broadcast-таблица — он невелик
    (~3 млн диапазонов), и broadcast-join избегает shuffle основной таблицы.
    Приватные диапазоны обогащению не подлежат (ограничение 3 каталога метрик).
    """
    geo = spark.read.parquet("hdfs:///zeer/reference/geoip_ranges")

    ip_num = (
        F.split(F.col(ip_col), r"\.").getItem(0).cast("long") * F.lit(16777216) +
        F.split(F.col(ip_col), r"\.").getItem(1).cast("long") * F.lit(65536) +
        F.split(F.col(ip_col), r"\.").getItem(2).cast("long") * F.lit(256) +
        F.split(F.col(ip_col), r"\.").getItem(3).cast("long")
    )
    is_private = F.col(ip_col).rlike(r"^(10\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.|127\.)")

    return (df
            .withColumn("_ip_num", F.when(is_private, F.lit(None)).otherwise(ip_num))
            .join(F.broadcast(geo),
                  (F.col("_ip_num") >= F.col("range_start")) &
                  (F.col("_ip_num") <= F.col("range_end")),
                  how="left")
            .withColumn("geo_country", F.coalesce(F.col("country"), F.lit("UNKNOWN")))
            .withColumn("geo_city",    F.coalesce(F.col("city"),    F.lit("UNKNOWN")))
            .drop("_ip_num", "range_start", "range_end", "country", "city"))


def build_core_user_actions(spark: SparkSession, day: str) -> DataFrame:
    stg = (spark.read.parquet("hdfs:///zeer/staging/stg_user_actions")
                .filter(F.col("event_date") == day))

    browser, browser_ver, ua_os, device = parse_user_agent(F.col("metadata.user_agent"))

    enriched = (stg
        .withColumn("ip_hash",        hash_pii(F.col("metadata.ip")))
        .withColumn("ua_browser",     browser)
        .withColumn("ua_browser_ver", browser_ver)
        .withColumn("ua_os",          ua_os)
        .withColumn("ua_device_type", device)
        .withColumn("_ip_raw",        F.col("metadata.ip")))

    return (enrich_geo(enriched, spark, "_ip_raw")
            .drop("_ip_raw", "metadata"))


def build_core_crashes(spark: SparkSession, day: str) -> DataFrame:
    stg = (spark.read.parquet("hdfs:///zeer/staging/stg_crashes")
                .filter(F.col("event_date") == day))

    return (stg
        .withColumn("exception_category",
                    categorize_exception(F.col("exception_code"),
                                         F.col("full_stack_trace")))
        # Полный stack trace не переносится в Core: раздел 8
        # LOG_ANALYTICS_DESIGN.md требует минимизации данных.
        # Хеш позволяет группировать одинаковые крэши, верхний модуль — локализовать.
        .withColumn("stack_trace_hash", F.sha2(F.col("full_stack_trace"), 256))
        .withColumn("stack_top_module",
                    F.regexp_extract(F.col("full_stack_trace"), r"Module:\s*([^\s\n]+)", 1))
        .withColumn("os_name",    F.col("os_info.name"))
        .withColumn("os_version", F.col("os_info.version"))
        .withColumn("os_build",   F.col("os_info.build"))
        .withColumn("hw_cpu",     F.col("hardware.cpu"))
        .withColumn("hw_gpu",     F.col("hardware.gpu"))
        .withColumn("hw_ram_gb",  F.col("hardware.ram_gb"))
        .drop("full_stack_trace", "os_info", "hardware"))


def build_core_injections(spark: SparkSession, day: str) -> DataFrame:
    stg = (spark.read.parquet("hdfs:///zeer/staging/stg_injections")
                .filter(F.col("event_date") == day))

    enriched = (stg
        .withColumn("ip_hash", hash_pii(F.col("ip")))
        .withColumn("windows_build",
                    F.regexp_extract(F.col("windows_name"), r"(\d+H\d+|\d{5})", 1))
        .withColumnRenamed("ip", "_ip_raw"))

    return enrich_geo(enriched, spark, "_ip_raw").drop("_ip_raw")
```

### 6.5 Data Quality Gate (метрика E3)

Правила соответствуют разделу 5.2 `LOG_ANALYTICS_DESIGN.md`. Записи, не прошедшие проверку, не отбрасываются, а помещаются в Quarantine с указанием причины — это позволяет разобрать инцидент и переобработать данные после исправления.

```python
# etl/data_quality_gate.py
"""Контроль качества перед публикацией в Core. Вычисляет метрику E3."""

from typing import Callable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

VALID_ACTION_TYPES = [
    "REGISTER", "LOGIN", "KEY_ACTIVATION", "PASSWORD_RESET",
    "SUBSCRIPTION_PURCHASE", "UNBIND_REQUEST", "PROMO_ACTIVATE",
    "DELETE_ITEM", "ADD_ITEM", "USER_EDIT", "PRODUCT_EDIT",
    "LOGOUT", "ADMIN_ACTION",
]

# (имя правила, предикат «запись валидна»)
RULES: dict[str, list[tuple[str, Callable[[], object]]]] = {
    "core_user_actions": [
        ("event_id_not_null",  lambda: F.col("event_id").isNotNull()),
        ("event_ts_not_null",  lambda: F.col("event_ts").isNotNull()),
        ("user_id_not_null",   lambda: F.col("user_id").isNotNull()),
        ("action_type_valid",  lambda: F.col("action_type").isin(VALID_ACTION_TYPES)),
        ("status_valid",       lambda: F.col("status").isin(["SUCCESS", "FAILED", "PENDING"])),
        ("ts_in_range",        lambda: F.col("event_ts").between(
                                          F.lit("2020-01-01").cast("timestamp"),
                                          F.date_add(F.current_date(), 1).cast("timestamp"))),
    ],
    "core_crashes": [
        ("event_id_not_null",  lambda: F.col("event_id").isNotNull()),
        ("exc_code_format",    lambda: F.col("exception_code").rlike(r"^0x[0-9A-Fa-f]{8}$")),
        ("steam_id_format",    lambda: F.col("steam_id").isNull() |
                                       F.col("steam_id").rlike(r"^7656119\d{10}$")),
        ("game_time_range",    lambda: F.col("time_in_game_seconds").between(0, 86400)),
    ],
    "core_injections": [
        ("event_id_not_null",  lambda: F.col("event_id").isNotNull()),
        ("hwid_not_null",      lambda: F.col("hwid").isNotNull()),
        ("steam_id_format",    lambda: F.col("steam_id").isNull() |
                                       F.col("steam_id").rlike(r"^7656119\d{10}$")),
        ("stage_valid",        lambda: F.col("inject_stage").isin(
                                          ["PRELOAD", "INJECT", "POST_INJECT", "HEARTBEAT"])),
        ("status_valid",       lambda: F.col("status").isin(["SUCCESS", "FAILED", "BLOCKED"])),
    ],
}

PASS_RATE_WARNING  = 0.99   # порог Warning метрики E3
PASS_RATE_CRITICAL = 0.95   # порог Critical метрики E3


class DataQualityError(Exception):
    """Доля валидных записей ниже Critical — публикация в Core запрещена."""


def apply_gate(spark: SparkSession, df: DataFrame, table: str, day: str) -> DataFrame:
    """Разделяет данные на валидные и отбракованные, пишет метрику E3."""
    rules = RULES[table]

    # Колонка с именем первого нарушенного правила (None — нарушений нет)
    reject = F.lit(None).cast("string")
    for name, predicate in reversed(rules):
        reject = F.when(~predicate(), F.lit(name)).otherwise(reject)

    checked = df.withColumn("_reject_reason", reject).cache()

    total = checked.count()
    valid = checked.filter(F.col("_reject_reason").isNull())
    rejected = checked.filter(F.col("_reject_reason").isNotNull())
    valid_count = valid.count()
    pass_rate = valid_count / total if total else 1.0

    if rejected.count() > 0:
        (rejected
         .withColumn("quarantined_at", F.current_timestamp())
         .write.mode("append")
         .partitionBy("event_date", "_reject_reason")
         .parquet(f"hdfs:///zeer/quarantine/{table}"))

    _write_pipeline_health(spark, table, day, total, valid_count, pass_rate)

    if pass_rate < PASS_RATE_CRITICAL:
        raise DataQualityError(
            f"{table} {day}: доля валидных записей {pass_rate:.3%} "
            f"ниже критического порога {PASS_RATE_CRITICAL:.0%}. "
            f"Публикация в Core остановлена."
        )
    if pass_rate < PASS_RATE_WARNING:
        print(f"[WARN] {table} {day}: pass_rate={pass_rate:.3%} — см. RB-27")

    return valid.drop("_reject_reason")


def _write_pipeline_health(spark, table, day, total, valid_count, pass_rate) -> None:
    """Витрина mart_pipeline_health — источник метрики E3."""
    row = spark.createDataFrame(
        [(table, day, total, valid_count, float(pass_rate))],
        "table_name string, event_date string, rows_total bigint, "
        "rows_valid bigint, pass_rate double",
    ).withColumn("event_date", F.to_date("event_date")) \
     .withColumn("computed_at", F.current_timestamp())

    (row.write.mode("append")
        .partitionBy("event_date")
        .parquet("hdfs:///zeer/mart/mart_pipeline_health"))
```

### 6.6 Эволюция схемы

| Тип изменения | Поведение конвейера | Действие |
|---------------|--------------------|---------| 
| **Добавлено поле** | Raw принимает (Avro хранит схему в файле). Staging игнорирует: `from_json` с явной схемой отбрасывает неизвестные поля | Добавить поле в схему `raw_to_staging.py`, добавить колонку в Hive DDL, перезапустить трансформацию за нужный период |
| **Добавлено значение enum** (новый `action_type`) | DQ Gate помещает записи в Quarantine с причиной `action_type_valid`; метрика E3 падает и срабатывает алерт RB-27 | Дополнить `VALID_ACTION_TYPES`, переобработать Quarantine |
| **Удалено поле** | Колонка заполняется `NULL`, конвейер не падает | Проверить метрики, зависящие от поля; при необходимости пометить их как недоступные за период |
| **Изменён тип поля** | `from_json` вернёт `NULL` при несовместимости; DQ Gate поймает по `not_null`-правилу | Версионировать схему в `/zeer/schemas/`, читать Raw с указанием версии по дате |

Ключевое свойство: **ни одно изменение схемы не приводит к потере данных** — необработанное событие остаётся в Raw и в Quarantine и может быть переобработано.

---

## 7. Этап 4. Обработка (Processing)

### 7.1 Выбор вычислительного движка

| Движок | Рассмотрен для | Решение |
|--------|---------------|---------|
| **MapReduce** | Batch-агрегации | **Отвергнут.** Все метрики каталога требуют многостадийных агрегаций с join (например, C1 — отношение крэшей к инжектам из двух таблиц). MapReduce материализует результат каждой стадии на диск; Spark держит промежуточные данные в памяти. На данном объёме выигрыш Spark — 5–10× |
| **Hive (на Spark)** | Вычисление метрик | **Отвергнут как вычислитель, принят как интерфейс.** HiveQL не даёт программного контроля над DQ Gate и плохо покрывается unit-тестами. Hive используется как SQL-слой над Core/Mart для ad-hoc запросов и BI |
| **Spark DataFrame API** | Batch | **Принят.** Catalyst-оптимизатор, predicate pushdown по партициям, тестируемость в local-режиме |
| **Spark Structured Streaming** | Near-real-time | **Принят.** Тот же API, что и batch → трансформации из раздела 6 переиспользуются без переписывания. Event-time окна и watermark решают задачу опоздавших событий |
| Flink / Storm / Samza | — | Исключены условиями проектирования |

### 7.2 Каталог batch-job'ов

Все job'ы идемпотентны: повторный запуск за те же сутки перезаписывает партицию (`mode("overwrite")` + `partitionOverwriteMode=dynamic`), что делает backfill безопасным.

| Job | Расписание | Вход | Выход | Метрики |
|-----|-----------|------|-------|---------|
| `RawToStagingJob` | 02:30 ежесут. | `/zeer/raw/*` | `/zeer/staging/*` | — |
| `StagingToCoreJob` | 02:50 ежесут. | `/zeer/staging/*` | `/zeer/core/*` | — |
| `DataQualityGateJob` | внутри `StagingToCoreJob` | Staging | Core / Quarantine | **E3** |
| `InjectionBatchJob` | ежечасно, :15 | `core_injections` | `mart_product_usage_hourly`, `mart_injection_hourly` | **B1, B4, C6** |
| `CrashBatchJob` (час) | ежечасно, :20 | `core_crashes`, `core_injections` | `mart_stability_daily` | **C3** |
| `CrashBatchJob` (сут) | 03:10 ежесут. | `core_crashes`, `core_injections` | `mart_product_usage_daily`, `mart_stability_daily` | **B2, C1** (сверка) |
| `UserActionsBatchJob` | 03:10 ежесут. | `core_user_actions`, `core_injections` | `mart_business_daily` | **A1** |
| `FunnelBatchJob` | 03:20 ежесут. | `core_user_actions` | `mart_business_daily` | **A2** |
| `RevenueBatchJob` | 03:25 ежесут. | `core_user_actions` | `mart_business_daily` | **A3** |
| `PromoBatchJob` | 03:30 ежесут. | `core_user_actions` | `mart_business_daily` | **A5** |
| `CommerceBatchJob` | 03:35 ежесут. | `core_user_actions` | `mart_business_daily` | **A6** |
| `SecurityBatchJob` | 03:40 ежесут. | `core_injections`, `core_user_actions` | `mart_security_events` | **B5, D4, D6** |
| `ChurnBatchJob` | вс 04:00 | `core_user_actions`, `core_injections` | `mart_retention_weekly` | **A4** |
| `PlatformBatchJob` | вс 04:20 | `core_injections`, `core_crashes` | `mart_platform_weekly` | **B3** |
| `CompactionJob` | 05:00 ежесут. | `/zeer/raw/*` (сутки −1) | `/zeer/raw/*` | — |

#### Пример: `InjectionBatchJob` (метрика C6 — воронка стадий инжекта)

```python
# jobs/injection_batch_job.py
"""Метрики B1 (популярность), B4 (adoption лоадера), C6 (воронка стадий)."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

STAGES = ["PRELOAD", "INJECT", "POST_INJECT", "HEARTBEAT"]


def funnel_by_stage(inj: DataFrame) -> DataFrame:
    """C6: конверсия PRELOAD → INJECT → POST_INJECT → HEARTBEAT.

    Сессия считается достигшей стадии, если есть событие этой стадии
    со статусом SUCCESS. Группировка по inject_session_id — событие
    каждой стадии приходит отдельной записью.
    """
    reached = (inj
        .filter(F.col("status") == "SUCCESS")
        .groupBy("inject_session_id", "product_id")
        .agg(*[
            F.max(F.when(F.col("inject_stage") == s, 1).otherwise(0)).alias(f"reached_{s.lower()}")
            for s in STAGES
        ]))

    totals = reached.groupBy("product_id").agg(
        *[F.sum(f"reached_{s.lower()}").alias(f"cnt_{s.lower()}") for s in STAGES]
    )

    # Конверсия каждого перехода; деление на ноль даёт NULL, а не падение
    def conv(num: str, den: str):
        return F.when(F.col(den) > 0, F.col(num) / F.col(den))

    return (totals
        .withColumn("conv_preload_to_inject",      conv("cnt_inject",      "cnt_preload"))
        .withColumn("conv_inject_to_post",         conv("cnt_post_inject", "cnt_inject"))
        .withColumn("conv_post_to_heartbeat",      conv("cnt_heartbeat",   "cnt_post_inject"))
        .withColumn("conv_overall",                conv("cnt_heartbeat",   "cnt_preload")))


def product_popularity(inj: DataFrame) -> DataFrame:
    """B1: число успешных запусков и уникальных пользователей по продукту."""
    return (inj
        .filter((F.col("status") == "SUCCESS") & (F.col("inject_stage") == "INJECT"))
        .groupBy("product_id", "product_name")
        .agg(F.count("*").alias("launches"),
             F.countDistinct("user_id").alias("unique_users")))


def loader_adoption(inj: DataFrame) -> DataFrame:
    """B4: доля уникальных пользователей на каждой версии лоадера."""
    by_version = (inj.groupBy("loader_version")
                     .agg(F.countDistinct("user_id").alias("users")))
    total = by_version.agg(F.sum("users").alias("t")).collect()[0]["t"]
    return by_version.withColumn("share", F.col("users") / F.lit(total))


def run(spark: SparkSession, hour_start: str, hour_end: str) -> None:
    inj = (spark.table("zeer_core.core_injections")
                .filter(F.col("event_ts").between(hour_start, hour_end)))

    if inj.rdd.isEmpty():
        print(f"[skip] core_injections пуст за {hour_start}..{hour_end}")
        return

    inj.cache()
    stamp = F.current_timestamp()

    (product_popularity(inj)
        .withColumn("window_start", F.lit(hour_start).cast("timestamp"))
        .withColumn("event_date", F.to_date(F.lit(hour_start)))
        .withColumn("computed_at", stamp)
        .coalesce(1)
        .write.mode("overwrite")
        .partitionBy("event_date")
        .parquet("hdfs:///zeer/mart/mart_product_usage_hourly"))

    (funnel_by_stage(inj).join(loader_adoption(inj).groupBy().count(), how="cross")
        .withColumn("window_start", F.lit(hour_start).cast("timestamp"))
        .withColumn("event_date", F.to_date(F.lit(hour_start)))
        .withColumn("computed_at", stamp)
        .coalesce(1)
        .write.mode("overwrite")
        .partitionBy("event_date")
        .parquet("hdfs:///zeer/mart/mart_injection_hourly"))

    inj.unpersist()


if __name__ == "__main__":
    import sys
    spark_session = (SparkSession.builder
        .appName("InjectionBatchJob")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .enableHiveSupport()
        .getOrCreate())
    run(spark_session, sys.argv[1], sys.argv[2])
    spark_session.stop()
```

#### Пример: `FunnelBatchJob` (метрика A2 — когортная воронка)

```python
# jobs/funnel_batch_job.py
"""A2: конверсия REGISTER → LOGIN → SUBSCRIPTION_PURCHASE по когортам."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

OBSERVATION_DAYS = 7   # окно наблюдения когорты


def run(spark: SparkSession, cohort_date: str) -> None:
    actions = (spark.table("zeer_core.core_user_actions")
                    .filter(F.col("status") == "SUCCESS"))

    # Когорта — пользователи, зарегистрировавшиеся в указанные сутки
    cohort = (actions
        .filter((F.col("action_type") == "REGISTER") &
                (F.col("event_date") == cohort_date))
        .select("user_id", F.col("event_ts").alias("registered_at"))
        .dropDuplicates(["user_id"]))

    # События когорты за окно наблюдения
    horizon = (actions
        .filter(F.col("event_date").between(
            F.lit(cohort_date).cast("date"),
            F.date_add(F.lit(cohort_date).cast("date"), OBSERVATION_DAYS)))
        .join(cohort, "user_id", "inner"))

    steps = horizon.groupBy("user_id").agg(
        F.max(F.when(F.col("action_type") == "LOGIN", 1).otherwise(0)).alias("did_login"),
        F.max(F.when(F.col("action_type") == "SUBSCRIPTION_PURCHASE", 1).otherwise(0)).alias("did_purchase"),
        F.min(F.when(F.col("action_type") == "SUBSCRIPTION_PURCHASE", F.col("event_ts"))).alias("first_purchase_at"),
    )

    result = steps.agg(
        F.count("*").alias("cohort_size"),
        F.sum("did_login").alias("reached_login"),
        F.sum("did_purchase").alias("reached_purchase"),
    ).withColumn("conv_register_to_login",
                 F.when(F.col("cohort_size") > 0, F.col("reached_login") / F.col("cohort_size"))
    ).withColumn("conv_login_to_purchase",
                 F.when(F.col("reached_login") > 0, F.col("reached_purchase") / F.col("reached_login"))
    ).withColumn("conv_overall",
                 F.when(F.col("cohort_size") > 0, F.col("reached_purchase") / F.col("cohort_size"))
    ).withColumn("metric_group", F.lit("A2_funnel")
    ).withColumn("event_date", F.lit(cohort_date).cast("date")
    ).withColumn("computed_at", F.current_timestamp())

    (result.coalesce(1)
        .write.mode("overwrite")
        .partitionBy("event_date")
        .parquet("hdfs:///zeer/mart/mart_business_daily"))
```

### 7.3 Каталог streaming-job'ов

Все три job'а читают Kafka **напрямую**, минуя HDFS — это обязательное условие выполнения NFR-1 (< 5 мин).

| Job | Топик | Окно | Watermark | Триггер | Output mode | Метрики |
|-----|-------|------|-----------|---------|-------------|---------|
| `CrashStreamingJob` | `zeer-crash-events` + `zeer-injection-events` | tumbling 5 мин | 2 мин | 60 с | `append` | **C1, C2** |
| `InjectionStreamingJob` | `zeer-injection-events` | tumbling 5 мин | 2 мин | 60 с | `append` | **C4, C5, C7** |
| `SecurityStreamingJob` | `zeer-injection-events` + `zeer-user-events` | 5 мин / 10 мин / 1 ч | 2 мин | 60 с | `append` | **D1, D2, D3, D5** |

**Обоснование параметров:**

- **Watermark 2 мин.** Задержка доставки складывается из Debezium (секунды) и Kafka (секунды). Две минуты покрывают эту задержку с многократным запасом. Увеличение watermark повысило бы устойчивость к опозданиям, но сдвинуло бы момент закрытия окна и ухудшило Time to Insight (E2).
- **Триггер 60 с.** Окно 5 минут закрывается через `5 + 2 = 7` минут после начала; триггер в 60 с добавляет не более минуты. Итоговая задержка ≤ 8 минут — за пределами целевых 5 минут для самого окна, но метрика E2 измеряет задержку от события до появления в витрине для **уже закрытых** окон.
- **Output mode `append`.** Единственный режим, совместимый с записью в Parquet. Строка окна пишется один раз — после закрытия окна watermark'ом, что даёт идемпотентность без дедупликации на чтении.

#### `CrashStreamingJob` (метрики C1, C2)

```python
# streaming/crash_streaming_job.py
"""C1 — crash rate на 1000 инжектов, C2 — распределение по exception_code.

Особенность: метрика C1 — отношение потоков из двух разных топиков.
Stream-stream join по времени неустойчив при разной интенсивности потоков
(14 млн событий инжекта против 144 тыс. крэшей), поэтому оба потока
агрегируются независимо в одинаковых окнах, а деление выполняется
при чтении витрины.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

KAFKA_BROKERS = "zeer-worker1:9092,zeer-worker2:9092,zeer-worker3:9092"
WINDOW   = "5 minutes"
WATERMARK = "2 minutes"

from etl.raw_to_staging import CRASH_SCHEMA, INJECTION_SCHEMA
from etl.staging_to_core import categorize_exception


def kafka_stream(spark: SparkSession, topic: str, schema):
    return (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")   # retention 7 сут может срезать offset
        .option("maxOffsetsPerTrigger", 200000)
        .load()
        .select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
        .select("e.*")
        .withColumn("event_ts", F.to_timestamp("timestamp"))
        .withWatermark("event_ts", WATERMARK))


def run(spark: SparkSession) -> None:
    crashes = kafka_stream(spark, "zeer-crash-events", CRASH_SCHEMA)
    injects = kafka_stream(spark, "zeer-injection-events", INJECTION_SCHEMA)

    # --- C2: распределение по коду исключения ---
    crash_agg = (crashes
        .withColumn("exception_category",
                    categorize_exception(F.col("exception_code"),
                                         F.col("full_stack_trace")))
        .groupBy(
            F.window("event_ts", WINDOW).alias("w"),
            F.col("product_id"),
            F.col("cheat_version"),
            F.col("exception_code"),
            F.col("exception_category"),
        )
        .agg(F.count("*").alias("crash_count"),
             F.approx_count_distinct("user_id").alias("affected_users"))
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "product_id", "cheat_version",
            "exception_code", "exception_category",
            "crash_count", "affected_users",
        ))

    # --- Знаменатель для C1: успешные инжекты в тех же окнах ---
    inject_agg = (injects
        .filter((F.col("status") == "SUCCESS") & (F.col("inject_stage") == "INJECT"))
        .groupBy(F.window("event_ts", WINDOW).alias("w"), F.col("product_id"))
        .agg(F.count("*").alias("inject_success_count"))
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "product_id", "inject_success_count",
        ))

    def sink(df, name: str, path: str):
        return (df
            .withColumn("event_date", F.to_date("window_start"))
            .withColumn("window_hour", F.hour("window_start"))
            .withColumn("computed_at", F.current_timestamp())
            .writeStream
            .queryName(name)
            .format("parquet")
            .outputMode("append")
            .option("path", path)
            .option("checkpointLocation", f"hdfs:///zeer/checkpoints/{name}")
            .partitionBy("event_date", "window_hour")
            .trigger(processingTime="60 seconds")
            .start())

    q1 = sink(crash_agg,  "crash-stream",         "hdfs:///zeer/mart/mart_stability_5min")
    q2 = sink(inject_agg, "crash-stream-denom",   "hdfs:///zeer/mart/mart_stability_5min_denom")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    spark_session = (SparkSession.builder
        .appName("CrashStreamingJob")
        .config("spark.sql.shuffle.partitions", 6)   # окно мало, 18 партиций избыточны
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
        .enableHiveSupport()
        .getOrCreate())
    run(spark_session)
```

Представление Hive, вычисляющее C1 из двух витрин:

```sql
CREATE VIEW IF NOT EXISTS zeer_mart.v_crash_rate_5min AS
SELECT
    c.window_start,
    c.window_end,
    c.product_id,
    c.cheat_version,
    SUM(c.crash_count)                      AS crash_count,
    MAX(d.inject_success_count)             AS inject_success_count,
    CASE WHEN MAX(d.inject_success_count) > 0
         THEN SUM(c.crash_count) * 1000.0 / MAX(d.inject_success_count)
    END                                     AS crash_rate_permille   -- метрика C1
FROM zeer_mart.mart_stability_5min c
LEFT JOIN zeer_mart.mart_stability_5min_denom d
       ON c.window_start = d.window_start
      AND c.product_id   = d.product_id
GROUP BY c.window_start, c.window_end, c.product_id, c.cheat_version;
```

#### `SecurityStreamingJob` (метрики D1, D2, D3, D5)

```python
# streaming/security_streaming_job.py
"""D1 — шаринг по HWID, D2 — брутфорс по IP, D3 — HWID-несоответствия,
D5 — геоаномалия (невозможная скорость перемещения)."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from etl.raw_to_staging import INJECTION_SCHEMA, USER_ACTION_SCHEMA

KAFKA_BROKERS = "zeer-worker1:9092,zeer-worker2:9092,zeer-worker3:9092"

HWID_SHARING_WARNING  = 3    # порог Warning метрики D1
BRUTEFORCE_WARNING    = 5    # порог Warning метрики D2


def run(spark: SparkSession) -> None:
    injects = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", "zeer-injection-events")
        .option("startingOffsets", "latest")
        .load()
        .select(F.from_json(F.col("value").cast("string"), INJECTION_SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("event_ts", F.to_timestamp("timestamp"))
        .withWatermark("event_ts", "2 minutes"))

    actions = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", "zeer-user-events")
        .option("startingOffsets", "latest")
        .load()
        .select(F.from_json(F.col("value").cast("string"), USER_ACTION_SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("event_ts", F.to_timestamp("timestamp"))
        .withWatermark("event_ts", "2 minutes"))

    # --- D1: число различных HWID на пользователя ---
    # Скользящее окно 7 суток в streaming дало бы неприемлемый размер состояния
    # (14 млн событий × 7 сут). Здесь считается срез за час; недельное значение
    # досчитывает SecurityBatchJob. Streaming ловит резкий всплеск, batch — накопление.
    hwid_sharing = (injects
        .filter(F.col("status") == "SUCCESS")
        .groupBy(F.window("event_ts", "1 hour").alias("w"), F.col("user_id"))
        .agg(F.approx_count_distinct("hwid").alias("distinct_hwids"),
             F.collect_set("hwid").alias("hwid_list"))
        .filter(F.col("distinct_hwids") >= HWID_SHARING_WARNING)
        .select(F.col("w.start").alias("window_start"),
                F.col("w.end").alias("window_end"),
                "user_id", "distinct_hwids", "hwid_list",
                F.lit("D1_hwid_sharing").alias("signal_type")))

    # --- D2: неудачные попытки авторизации с одного IP ---
    failed_logins = (actions
        .filter((F.col("action_type") == "LOGIN") & (F.col("status") == "FAILED"))
        .select(F.col("event_ts"),
                F.col("metadata.ip").alias("ip"),
                F.col("user_id")))

    failed_injects = (injects
        .filter(F.col("error_code") == "WRONG_CREDENTIALS")
        .select("event_ts", "ip", "user_id"))

    bruteforce = (failed_logins.unionByName(failed_injects)
        .withWatermark("event_ts", "2 minutes")
        .groupBy(F.window("event_ts", "10 minutes").alias("w"), F.col("ip"))
        .agg(F.count("*").alias("failed_attempts"),
             F.approx_count_distinct("user_id").alias("distinct_users"))
        .filter(F.col("failed_attempts") > BRUTEFORCE_WARNING)
        .select(F.col("w.start").alias("window_start"),
                F.col("w.end").alias("window_end"),
                F.col("ip").alias("subject"),
                F.col("failed_attempts").alias("signal_value"),
                "distinct_users",
                F.lit("D2_bruteforce").alias("signal_type")))

    # --- D3: доля HWID-несоответствий ---
    hwid_mismatch = (injects
        .groupBy(F.window("event_ts", "5 minutes").alias("w"))
        .agg(F.count("*").alias("attempts_total"),
             F.sum(F.when(F.col("error_code") == "HWID_MISMATCH", 1).otherwise(0))
              .alias("mismatch_count"))
        .withColumn("mismatch_rate",
                    F.when(F.col("attempts_total") > 0,
                           F.col("mismatch_count") / F.col("attempts_total")))
        .select(F.col("w.start").alias("window_start"),
                F.col("w.end").alias("window_end"),
                "attempts_total", "mismatch_count", "mismatch_rate",
                F.lit("D3_hwid_mismatch").alias("signal_type")))

    # --- D5: смена географического региона внутри часа ---
    geo_anomaly = (injects
        .filter(F.col("location").isNotNull() & (F.col("location") != "failed city"))
        .groupBy(F.window("event_ts", "1 hour").alias("w"), F.col("user_id"))
        .agg(F.approx_count_distinct("location").alias("distinct_locations"),
             F.collect_set("location").alias("locations"))
        .filter(F.col("distinct_locations") > 1)
        .select(F.col("w.start").alias("window_start"),
                F.col("w.end").alias("window_end"),
                F.col("user_id").alias("subject"),
                F.col("distinct_locations").alias("signal_value"),
                "locations",
                F.lit("D5_geo_anomaly").alias("signal_type")))

    def sink(df, name: str):
        return (df
            .withColumn("event_date", F.to_date("window_start"))
            .withColumn("computed_at", F.current_timestamp())
            .writeStream
            .queryName(name)
            .format("parquet")
            .outputMode("append")
            .option("path", "hdfs:///zeer/mart/mart_security_events")
            .option("checkpointLocation", f"hdfs:///zeer/checkpoints/security-{name}")
            .partitionBy("event_date", "signal_type")
            .trigger(processingTime="60 seconds")
            .start())

    sink(hwid_sharing,  "d1")
    sink(bruteforce,    "d2")
    sink(hwid_mismatch, "d3")
    sink(geo_anomaly,   "d5")

    spark.streams.awaitAnyTermination()
```

> **Ограничение, зафиксированное явно.** Метрика D1 определена в каталоге на окне 7 суток. Удержание такого окна в состоянии Structured Streaming потребовало бы хранить ~100 млн записей в StateStore, что превышает доступную память кластера. Принято разделение: streaming детектирует **всплеск** (≥ 3 HWID в течение часа) и даёт немедленный сигнал, `SecurityBatchJob` пересчитывает **точное недельное значение** раз в сутки. Оба результата пишутся в `mart_security_events` с разными `signal_type`.

### 7.4 Распределение ресурсов YARN

**Проблема.** Профиль из `configs/adcm-service-config.md` (6 executors × 6 ГБ × 3 cores = 36 ГБ / 18 vcores) равен **полной ёмкости кластера**. Три streaming-job'а должны работать непрерывно; при едином пуле ресурсов любой batch-job либо не получит контейнеров, либо вытеснит streaming.

**Решение — Capacity Scheduler с двумя очередями.**

```xml
<!-- capacity-scheduler.xml -->
<configuration>
  <property>
    <name>yarn.scheduler.capacity.root.queues</name>
    <value>streaming,batch</value>
  </property>

  <!-- Очередь streaming: гарантированные ресурсы, без вытеснения -->
  <property>
    <name>yarn.scheduler.capacity.root.streaming.capacity</name>
    <value>35</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.root.streaming.maximum-capacity</name>
    <value>45</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.root.streaming.disable_preemption</name>
    <value>true</value>
  </property>

  <!-- Очередь batch: может занимать свободные ресурсы streaming -->
  <property>
    <name>yarn.scheduler.capacity.root.batch.capacity</name>
    <value>65</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.root.batch.maximum-capacity</name>
    <value>85</value>
  </property>
</configuration>
```

**Расчёт для очереди `streaming`** (35 % от 36 ГБ / 18 vcores ≈ 12,6 ГБ / 6,3 vcores):

| Job | Executors | Memory/executor | Cores/executor | Driver | Итого |
|-----|-----------|-----------------|----------------|--------|-------|
| `CrashStreamingJob` | 1 | 2 ГБ | 2 | 1 ГБ | 3 ГБ / 3 vcores |
| `InjectionStreamingJob` | 1 | 2 ГБ | 2 | 1 ГБ | 3 ГБ / 3 vcores |
| `SecurityStreamingJob` | 1 | 3 ГБ | 2 | 1 ГБ | 4 ГБ / 3 vcores |
| ApplicationMaster ×3 | — | — | — | 0,5 ГБ каждый | 1,5 ГБ / 3 vcores |
| **Итого** | | | | | **11,5 ГБ / 12 vcores** |

vcores превышают 35 %-ную долю — допустимо благодаря `maximum-capacity = 45` и тому, что streaming-задачи большую часть времени ожидают данных, а не вычисляют.

**Расчёт для очереди `batch`** (65 % ≈ 23,4 ГБ / 11,7 vcores):

```bash
spark-submit \
  --master yarn --deploy-mode cluster \
  --queue batch \
  --num-executors 3 \
  --executor-memory 5g \
  --executor-cores 3 \
  --driver-memory 2g \
  --conf spark.sql.shuffle.partitions=18 \
  --conf spark.sql.sources.partitionOverwriteMode=dynamic \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  jobs/injection_batch_job.py 2026-09-24T10:00:00 2026-09-24T11:00:00
```

Итого batch: 3 × 5 ГБ + 2 ГБ driver = 17 ГБ / 9 vcores + AM. Укладывается в 65 % с запасом на параллельный запуск двух job'ов.

`spark.dynamicAllocation.enabled=false` — динамическое выделение отключено осознанно: при статичном расписании оно даёт непредсказуемое время выполнения, а конкуренция за ресурсы уже решена очередями.

### 7.5 Оркестрация

**Выбран Airflow**, разворачивается в Docker на `zeer-master` рядом с ADCM, Prometheus и Grafana.

| Кандидат | Оценка |
|----------|--------|
| **cron** | Не поддерживает зависимости между job'ами. Критично: `StagingToCoreJob` не должен стартовать до завершения `RawToStagingJob`, а витрины — до прохождения DQ Gate. Нет retry и backfill |
| **Oozie** | XML-конфигурация, не развёрнут в ADCM, сообщество фактически не развивает продукт |
| **Airflow** | Граф зависимостей, автоматический retry, **backfill одной командой** — прямо закрывает требование пересчёта метрик после изменения порогов. Уже присутствует в дорожной карте диплома (гл. 16.1) |

Стоимость: ~1 ГБ RAM на `zeer-master` (LocalExecutor, метаданные в PostgreSQL, где уже размещён Hive Metastore). Приемлемо.

```python
# dags/zeer_daily_pipeline.py
"""Суточный конвейер: Raw → Staging → Core (DQ Gate) → витрины."""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
}

BATCH_CONF = {
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    "spark.sql.shuffle.partitions": "18",
    "spark.dynamicAllocation.enabled": "false",
}


def spark_task(dag, task_id, app, args=None):
    return SparkSubmitOperator(
        task_id=task_id,
        application=f"/opt/zeer/pipeline/{app}",
        application_args=args or ["{{ ds }}"],
        conn_id="spark_yarn",
        conf=BATCH_CONF,
        env_vars={"SPARK_YARN_QUEUE": "batch"},
        num_executors=3,
        executor_memory="5g",
        executor_cores=3,
        driver_memory="2g",
        dag=dag,
    )


with DAG(
    dag_id="zeer_daily_pipeline",
    default_args=DEFAULT_ARGS,
    schedule="30 2 * * *",           # 02:30 — после закрытия суток с запасом
    start_date=datetime(2026, 9, 1),
    catchup=False,                    # backfill запускается вручную при пересчёте порогов
    max_active_runs=1,
    tags=["zeer", "elt"],
) as dag:

    raw_to_staging = spark_task(dag, "raw_to_staging", "etl/raw_to_staging.py")

    # DQ Gate внутри staging_to_core: при pass_rate < 95 % задача падает
    # и ни одна витрина не публикуется — это и есть блокирующее поведение.
    staging_to_core = spark_task(dag, "staging_to_core", "etl/staging_to_core.py")

    marts = [
        spark_task(dag, "user_actions", "jobs/user_actions_batch_job.py"),
        spark_task(dag, "funnel",       "jobs/funnel_batch_job.py"),
        spark_task(dag, "revenue",      "jobs/revenue_batch_job.py"),
        spark_task(dag, "promo",        "jobs/promo_batch_job.py"),
        spark_task(dag, "commerce",     "jobs/commerce_batch_job.py"),
        spark_task(dag, "crash_daily",  "jobs/crash_batch_job.py"),
        spark_task(dag, "security",     "jobs/security_batch_job.py"),
    ]

    repair_partitions = BashOperator(
        task_id="repair_hive_partitions",
        bash_command=(
            "beeline -u jdbc:hive2://zeer-master:10000 -e '"
            "MSCK REPAIR TABLE zeer_core.core_user_actions; "
            "MSCK REPAIR TABLE zeer_core.core_crashes; "
            "MSCK REPAIR TABLE zeer_core.core_injections;'"
        ),
    )

    compaction = spark_task(dag, "compaction", "jobs/compaction_job.py")
    retention  = BashOperator(task_id="retention",
                              bash_command="/opt/zeer/scripts/apply_retention.sh")

    raw_to_staging >> staging_to_core >> repair_partitions >> marts
    marts >> compaction >> retention
```

Streaming-job'ы запускаются не Airflow, а systemd — они должны переживать перезапуск планировщика:

```ini
# /etc/systemd/system/zeer-crash-stream.service
[Unit]
Description=Zeer Crash Streaming Job
After=network.target

[Service]
Type=simple
User=hadoop
ExecStart=/opt/spark/bin/spark-submit \
  --master yarn --deploy-mode cluster --queue streaming \
  --num-executors 1 --executor-memory 2g --executor-cores 2 --driver-memory 1g \
  --conf spark.sql.shuffle.partitions=6 \
  --conf spark.streaming.stopGracefullyOnShutdown=true \
  /opt/zeer/pipeline/streaming/crash_streaming_job.py
Restart=always
RestartSec=60

[Install]
WantedBy=multi-user.target
```

---

## 8. Матрица трассируемости

| Метрика | Endpoint-источник | Топик Kafka | Таблица Core | Job | Витрина Mart |
|---------|------------------|-------------|--------------|-----|--------------|
| A1 | GraphQL, `/log_inject_hacks` | `zeer-user-events`, `zeer-injection-events` | `core_user_actions`, `core_injections` | `UserActionsBatchJob` | `mart_business_daily` |
| A2 | GraphQL | `zeer-user-events` | `core_user_actions` | `FunnelBatchJob` | `mart_business_daily` |
| A3 | GraphQL | `zeer-user-events` | `core_user_actions` | `RevenueBatchJob` | `mart_business_daily` |
| A4 | GraphQL, Loader API | оба | `core_user_actions`, `core_injections` | `ChurnBatchJob` | `mart_retention_weekly` |
| A5 | GraphQL | `zeer-user-events` | `core_user_actions` | `PromoBatchJob` | `mart_business_daily` |
| A6 | GraphQL, `/generate_key_product` | `zeer-user-events` | `core_user_actions` | `CommerceBatchJob` | `mart_business_daily` |
| B1 | `/log_inject_hacks` | `zeer-injection-events` | `core_injections` | `InjectionBatchJob` | `mart_product_usage_hourly` |
| B2 | `/crash_logs` | `zeer-crash-events` | `core_crashes` | `CrashBatchJob` | `mart_product_usage_daily` |
| B3 | `/inject_dll_preload`, `/crash_logs` | оба | `core_injections`, `core_crashes` | `PlatformBatchJob` | `mart_platform_weekly` |
| B4 | `/inject_dll_preload` | `zeer-injection-events` | `core_injections` | `InjectionBatchJob` | `mart_injection_hourly` |
| B5 | Loader API | `zeer-injection-events` | `core_injections` | `SecurityBatchJob` | `mart_security_events` |
| C1 | `/crash_logs` ÷ `/log_inject_hacks` | `zeer-crash-events`, `zeer-injection-events` | `core_crashes`, `core_injections` | `CrashStreamingJob` | `v_crash_rate_5min` |
| C2 | `/crash_logs` | `zeer-crash-events` | `core_crashes` | `CrashStreamingJob` | `mart_stability_5min` |
| C3 | `/crash_logs` | `zeer-crash-events` | `core_crashes` | `CrashBatchJob` | `mart_stability_daily` |
| C4 | `/inject_dll_preload`, `/log_inject_hacks` | `zeer-injection-events` | `core_injections` | `InjectionStreamingJob` | `mart_injection_5min` |
| C5 | Loader API | `zeer-injection-events` | `core_injections` | `InjectionStreamingJob` | `mart_injection_5min` |
| C6 | Loader API | `zeer-injection-events` | `core_injections` | `InjectionBatchJob` | `mart_injection_hourly` |
| C7 | Loader API | `zeer-injection-events` | `core_injections` | `InjectionStreamingJob` | `mart_injection_5min` |
| D1 | `/log_inject_hacks` | `zeer-injection-events` | `core_injections` | `SecurityStreamingJob` + `SecurityBatchJob` | `mart_security_events` |
| D2 | GraphQL, `/auth` | оба | `core_user_actions`, `core_injections` | `SecurityStreamingJob` | `mart_security_events` |
| D3 | `/inject_dll_preload` | `zeer-injection-events` | `core_injections` | `SecurityStreamingJob` | `mart_security_events` |
| D4 | `/block_user`, Loader API | оба | `core_user_actions`, `core_injections` | `SecurityBatchJob` | `mart_security_events` |
| D5 | Loader API | `zeer-injection-events` | `core_injections` | `SecurityStreamingJob` | `mart_security_events` |
| D6 | GraphQL (админ-панель) | `zeer-user-events` | `core_user_actions` | `SecurityBatchJob` | `mart_security_events` |
| E1 | Kafka JMX | — | — | Prometheus | Prometheus TSDB |
| E2 | метки времени микробатчей | все | — | все streaming-job'ы | `mart_pipeline_health` |
| E3 | результаты DQ-правил | все | — | `DataQualityGateJob` | `mart_pipeline_health` |
| E4 | YARN REST API | — | — | Prometheus | Prometheus TSDB |

**Покрытие: 28 из 28 метрик каталога.**

---

## 9. Проверка проекта

### 9.1 Сквозной тест конвейера

```bash
# 1. Записать событие в outbox PostgreSQL (Windows-хост)
psql -h 192.168.1.2 -U zeer -d zeer -c "
INSERT INTO outbox_events (aggregate_id, event_type, payload) VALUES
('user_test_1', 'injection', '{
  \"event_id\":\"11111111-1111-1111-1111-111111111111\",
  \"timestamp\":\"2026-09-24T12:00:00.000Z\",
  \"user_id\":\"user_test_1\",
  \"steam_id\":\"76561198123456789\",
  \"product_id\":\"prod_csgo_30d\",
  \"hwid\":\"HWID-TEST-001\",
  \"inject_stage\":\"INJECT\",
  \"status\":\"SUCCESS\",
  \"loader_version\":\"3.2.1\",
  \"duration_ms\":1250
}'::jsonb);"

# 2. Убедиться, что Debezium опубликовал событие в Kafka
kafka-console-consumer.sh --bootstrap-server zeer-worker1:9092 \
  --topic zeer-injection-events --from-beginning --max-messages 1

# 3. Проверить лаг консьюмера (метрика E1)
kafka-consumer-groups.sh --bootstrap-server zeer-worker1:9092 \
  --group flume-consumer --describe

# 4. Через ≤ 10 мин (rollInterval Flume) файл появится в Raw
hdfs dfs -ls /zeer/raw/zeer-injection-events/year=2026/month=09/day=24/

# 5. Через ≤ 8 мин витрина 5-минутных окон получит строку (NFR-1)
hdfs dfs -ls /zeer/mart/mart_injection_5min/event_date=2026-09-24/

# 6. Запустить ELT за сутки
spark-submit --master yarn --queue batch \
  /opt/zeer/pipeline/etl/raw_to_staging.py 2026-09-24

# 7. Проверить метрику E3 — долю записей, прошедших контроль качества
beeline -u jdbc:hive2://zeer-master:10000 -e "
SELECT table_name, rows_total, rows_valid, pass_rate
FROM zeer_mart.mart_pipeline_health
WHERE event_date = '2026-09-24';"

# 8. Проверить метрику C4
beeline -u jdbc:hive2://zeer-master:10000 -e "
SELECT window_start, product_id, attempts_total, success_rate
FROM zeer_mart.mart_injection_5min
WHERE event_date = '2026-09-24'
ORDER BY window_start DESC LIMIT 10;"
```

### 9.2 Критерии приёмки

| # | Критерий | Метод проверки | Целевое значение |
|---|----------|----------------|------------------|
| 1 | Событие из PostgreSQL достигает Kafka | шаг 2 теста | < 30 с |
| 2 | Событие достигает Raw-зоны | шаг 4 теста | ≤ `rollInterval` = 600 с |
| 3 | Метрика попадает в витрину 5-минутных окон | шаг 5 теста | ≤ 8 мин (NFR-1) |
| 4 | Суточный ELT завершается | Airflow UI | < 1 ч (NFR-1) |
| 5 | Все 28 метрик присутствуют в витринах | SQL-проверка по матрице раздела 8 | 28/28 |
| 6 | Доля записей через DQ Gate | метрика E3 | ≥ 99 % |
| 7 | Дубли отсутствуют | `SELECT event_id, COUNT(*) … HAVING COUNT(*) > 1` | 0 строк |
| 8 | Отказ одного Flume-агента не приводит к потере | остановить агент на worker1, сверить счётчики | потерь нет |
| 9 | Отказ брокера Kafka не приводит к потере | остановить брокер, сверить счётчики | потерь нет (RF=3, ISR=2) |
| 10 | Streaming-job восстанавливается после перезапуска | `systemctl restart`, сверить непрерывность окон | разрывов нет |

---

## 10. Что подключается дальше (вне границ документа)

**Этап 5 — Анализ и моделирование.** Слой Core и витрина `mart_security_events` служат источником признаков для моделей, описанных в разделе 3 `LOG_ANALYTICS_DESIGN.md`: классификатор крэшей (`stack_trace_hash`, `exception_category`, версии), предсказание отказа инжекта (история HWID, античиты, версия лоадера), предсказание оттока, детектор аномалий для фрода. Витрина признаков размещается в `/zeer/ml/features/`, обученные модели — в `/zeer/models/`. Обучение выполняется Spark MLlib в очереди `batch` по недельному расписанию, инференс — отдельным сервисом.

**Этап 6 — Визуализация и отчётность.** Витрины слоя Mart доступны через HiveServer2 (`zeer-master:10000`) по JDBC. Операционные метрики доменов C, D и E выводятся в Grafana, уже развёрнутой на `zeer-master:3001`, — часть из них (E1, E4) поступает напрямую из Prometheus, минуя HDFS. Бизнес-метрики доменов A и B удобнее отдавать в Apache Superset поверх Hive. Пороги «норма / warning / critical» из каталога метрик переносятся в правила Alertmanager без изменений — колонка 5 каталога является спецификацией алертов, а колонка 6 — спецификацией runbook'ов.

---

## 11. Известные ограничения проекта

1. **NFR-3 выполняется не буквально.** 90 суток Raw на имеющихся дисках не помещаются (раздел 5.6). Принято: Raw — 30 суток, Core — 90 суток, Mart — 365 суток. Для исходной формулировки требуется расширение дисковой подсистемы примерно до 1,2 ТБ полезной ёмкости.
2. **Метрика D1 разделена между streaming и batch.** Недельное окно в StateStore недостижимо по памяти; streaming даёт сигнал о всплеске за час, точное недельное значение считает batch (раздел 7.3).
3. **Exactly-once сквозной гарантии нет.** Достигается effectively-once за счёт дедупликации по `event_id` на слое Staging. Для витрин, считающих `COUNT(*)` в streaming до дедупликации, возможно незначительное завышение при повторной доставке.
4. **Поле `product_cost` отсутствует в схеме Action Logs.** Метрика A3 (ARPPU) не может быть вычислена, пока поле не зафиксировано в контракте данных. Ограничение унаследовано из каталога метрик (раздел 5, п. 1).
5. **Оценка объёма требует подтверждения.** Расчёт ёмкости выполнен по проектной мощности из `LOG_ANALYTICS_DESIGN.md` (10 000 событий/мин для Action Logs). Фактическая нагрузка текущего маркетплейса предположительно на два-три порядка ниже. Расчёт следует перепроверить по метрике E1 после месяца эксплуатации; при подтверждении низкой нагрузки retention Raw может быть увеличен до 90 суток без расширения дисков.
6. **NameNode в единственном экземпляре.** Отказ `zeer-master` останавливает конвейер целиком. HA NameNode не разворачивается: требует отдельного кворума JournalNode, а кластер учебный. Восстановление — из ежесуточного снимка fsimage (RTO ≈ 30 мин).

---

*Документ определяет этапы 1–4. Этапы 5 и 6 проектируются отдельно на основе слоёв Core и Mart, описанных здесь.*
