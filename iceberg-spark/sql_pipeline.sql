-- =============================================================================
-- Spark 4.1 Declarative Pipeline: SQL Example with Apache Iceberg
-- =============================================================================
--
-- This file demonstrates the SQL syntax for Spark 4.1 Declarative Pipelines
-- using Apache Iceberg tables.  It is the SQL equivalent of the Python
-- pipelines defined in batch_pipeline.py and streaming_pipeline.py.
--
-- Run this pipeline with (build the JAR from apache/iceberg main first – see README.md):
--
--   spark-pipelines run \
--       --pipeline-file sql_pipeline.sql \
--       --pipeline-conf spark.jars=/path/to/iceberg/iceberg-spark/iceberg-spark-runtime-4.1_2.13/build/libs/iceberg-spark-runtime-4.1_2.13-*-SNAPSHOT.jar \
--       --pipeline-conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
--       --pipeline-conf spark.sql.catalog.local.type=hadoop \
--       --pipeline-conf spark.sql.catalog.local.warehouse=/tmp/iceberg-spark-example \
--       --pipeline-conf spark.sql.defaultCatalog=local \
--       --pipeline-conf spark.sql.streaming.checkpointLocation=/tmp/iceberg-spark-checkpoints
--
-- =============================================================================
-- PART 1 – BATCH PIPELINE (e-commerce orders)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1a. Source: sample orders (inline seed data via VALUES)
-- ---------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW orders
COMMENT 'Sample e-commerce orders used as the pipeline source.'
TBLPROPERTIES ('format-version' = '2')
AS
SELECT
    order_id,
    customer_id,
    category,
    amount,
    event_time
FROM (
    VALUES
        (1L,  101L, 'electronics', 299.99,  TIMESTAMP '2024-01-15 10:00:00'),
        (2L,  102L, 'clothing',     49.99,  TIMESTAMP '2024-01-15 11:30:00'),
        (3L,  101L, 'electronics',  89.99,  TIMESTAMP '2024-01-15 14:00:00'),
        (4L,  103L, 'books',        19.99,  TIMESTAMP '2024-01-16 09:00:00'),
        (5L,  102L, 'clothing',     79.99,  TIMESTAMP '2024-01-16 10:15:00'),
        (6L,  104L, 'electronics', 499.99,  TIMESTAMP '2024-01-16 12:00:00'),
        (7L,  103L, 'books',        14.99,  TIMESTAMP '2024-01-17 08:00:00'),
        (8L,  101L, 'home',        129.99,  TIMESTAMP '2024-01-17 15:00:00'),
        (9L,  105L, 'electronics', 349.99,  TIMESTAMP '2024-01-17 16:30:00'),
        (10L, 104L, 'clothing',     59.99,  TIMESTAMP '2024-01-17 17:00:00'),
        -- Row with a null customer_id – filtered out in validated_orders
        (11L, NULL, 'books',         9.99,  TIMESTAMP '2024-01-17 18:00:00'),
        -- Row with a negative amount – filtered out in validated_orders
        (12L, 106L, 'home',         -5.00,  TIMESTAMP '2024-01-17 19:00:00')
) AS t(order_id, customer_id, category, amount, event_time);

-- ---------------------------------------------------------------------------
-- 1b. Cleaning: validated_orders
-- ---------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW validated_orders
COMMENT 'Cleaned orders: null customer_id and non-positive amounts removed; order_date added.'
PARTITIONED BY (order_date)
TBLPROPERTIES ('format-version' = '2')
AS
SELECT
    order_id,
    customer_id,
    category,
    amount,
    CAST(event_time AS DATE) AS order_date
FROM orders
WHERE
    amount > 0
    AND customer_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- 1c. Aggregation: daily_sales
-- ---------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW daily_sales
COMMENT 'Total revenue and order count per day and product category.'
PARTITIONED BY (order_date)
TBLPROPERTIES ('format-version' = '2')
AS
SELECT
    order_date,
    category,
    SUM(amount)   AS total_amount,
    COUNT(*)      AS order_count,
    AVG(amount)   AS avg_order_value
FROM validated_orders
GROUP BY order_date, category
ORDER BY order_date, category;

-- ---------------------------------------------------------------------------
-- 1d. Aggregation: customer_ltv
-- ---------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW customer_ltv
COMMENT 'Lifetime value and order frequency per customer.'
TBLPROPERTIES ('format-version' = '2')
AS
SELECT
    customer_id,
    SUM(amount)       AS lifetime_value,
    COUNT(*)          AS total_orders,
    MIN(order_date)   AS first_order_date,
    MAX(order_date)   AS last_order_date
FROM validated_orders
GROUP BY customer_id
ORDER BY lifetime_value DESC;

-- =============================================================================
-- PART 2 – STREAMING PIPELINE (IoT sensor events)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 2a. Streaming table: sensor_events
-- ---------------------------------------------------------------------------

CREATE STREAMING TABLE sensor_events
COMMENT 'Append-only Iceberg table collecting raw sensor events from all flows.'
PARTITIONED BY (event_date)
TBLPROPERTIES (
    'format-version'       = '2',
    'write.merge.mode'     = 'merge-on-read',
    'write.update.mode'    = 'merge-on-read',
    'write.delete.mode'    = 'merge-on-read'
);

-- ---------------------------------------------------------------------------
-- 2b. Flow: temperature_readings → sensor_events
-- ---------------------------------------------------------------------------

CREATE FLOW temperature_readings
COMMENT 'Simulated temperature readings from a rate source (5 events/s).'
AS INSERT INTO sensor_events
SELECT
    CAST(value % 5 AS STRING)              AS sensor_id,
    'temperature'                          AS event_type,
    CAST(20.0 + (value % 20) AS DOUBLE)   AS value,
    timestamp                              AS event_time,
    CAST(timestamp AS DATE)                AS event_date
FROM STREAM(
    -- Replace with a Kafka source in production:
    -- read_kafka(bootstrapServers => 'broker:9092', subscribe => 'temperature-events')
    TABLE(rate(rowsPerSecond => 5))
);

-- ---------------------------------------------------------------------------
-- 2c. Flow: humidity_readings → sensor_events
-- ---------------------------------------------------------------------------

CREATE FLOW humidity_readings
COMMENT 'Simulated humidity readings from a rate source (3 events/s).'
AS INSERT INTO sensor_events
SELECT
    CAST(value % 5 AS STRING)              AS sensor_id,
    'humidity'                             AS event_type,
    CAST(50.0 + (value % 40) AS DOUBLE)   AS value,
    timestamp                              AS event_time,
    CAST(timestamp AS DATE)                AS event_date
FROM STREAM(
    -- Replace with a Kafka source in production:
    -- read_kafka(bootstrapServers => 'broker:9092', subscribe => 'humidity-events')
    TABLE(rate(rowsPerSecond => 3))
);

-- ---------------------------------------------------------------------------
-- 2d. Materialized view: sensor_hourly_stats
-- ---------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW sensor_hourly_stats
COMMENT 'Hourly average and sample count per sensor and event type.'
TBLPROPERTIES ('format-version' = '2')
AS
SELECT
    sensor_id,
    event_type,
    DATE_TRUNC('HOUR', event_time) AS hour,
    AVG(value)                     AS avg_value,
    MIN(value)                     AS min_value,
    MAX(value)                     AS max_value,
    COUNT(*)                       AS sample_count
FROM sensor_events
GROUP BY sensor_id, event_type, DATE_TRUNC('HOUR', event_time)
ORDER BY sensor_id, event_type, hour;
