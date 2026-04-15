-- Flink SQL + Iceberg quickstart example.
-- Run in a Flink SQL Client session that has the Iceberg runtime JAR available.

CREATE CATALOG iceberg_catalog WITH (
  'type'='iceberg',
  'catalog-type'='hadoop',
  'warehouse'='file:///tmp/iceberg-flink-warehouse'
);

USE CATALOG iceberg_catalog;
CREATE DATABASE IF NOT EXISTS demo;
USE demo;

CREATE TABLE IF NOT EXISTS orders (
  order_id BIGINT,
  customer_id BIGINT,
  amount DECIMAL(10, 2),
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'format-version'='2',
  'write.format.default'='parquet'
);

INSERT INTO orders VALUES
  (1, 101, 25.00, TIMESTAMP '2026-01-01 00:00:01'),
  (2, 101, 18.50, TIMESTAMP '2026-01-01 00:00:03'),
  (3, 102, 99.99, TIMESTAMP '2026-01-01 00:00:08');

-- Batch-style aggregation from Iceberg table data.
SELECT customer_id, CAST(SUM(amount) AS DECIMAL(10, 2)) AS total_amount
FROM orders
GROUP BY customer_id
ORDER BY customer_id;
