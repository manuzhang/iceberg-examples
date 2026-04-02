package io.github.manuzhang.iceberg.examples

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Spark + Iceberg Batch Pipeline: Scala Example
 * ==============================================
 *
 * Demonstrates a four-step e-commerce analytics pipeline using Apache Spark
 * and Apache Iceberg tables as the storage layer.  The scenario mirrors
 * `batch_pipeline.py` from `iceberg-spark/`, showing how the same ideas
 * translate to the Spark Scala API.
 *
 * Pipeline graph:
 * {{{
 *   orders              (inline seed data → Iceberg table)
 *       └── validated_orders  (filter & enrich)
 *               ├── daily_sales     (daily totals per category)
 *               └── customer_ltv    (lifetime value per customer)
 * }}}
 *
 * Iceberg features demonstrated:
 *  - Format version 2 tables created via `CREATE OR REPLACE TABLE … AS SELECT` (CTAS)
 *  - Day-level partitioning
 *  - Snapshot history inspection (`*.snapshots` metadata table)
 *  - Time-travel reads by snapshot ID
 *
 * All tables are stored in a local HadoopCatalog (filesystem-backed,
 * no external service required).
 */
object BatchPipeline {

  def main(args: Array[String]): Unit = {
    val warehousePath = args.headOption.getOrElse("/tmp/iceberg-scala-example")

    val spark = SparkSession
      .builder()
      .appName("IcebergScalaBatchPipeline")
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", warehousePath)
      .config("spark.sql.defaultCatalog", "local")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      new BatchPipeline(spark).run()
    } finally {
      spark.stop()
    }
  }
}

/** Executes the four-step batch pipeline against the provided SparkSession. */
class BatchPipeline(spark: SparkSession) {

  import spark.implicits._

  // ---------------------------------------------------------------------------
  // Schema shared across pipeline stages
  // ---------------------------------------------------------------------------

  private val ordersSchema = StructType(
    Seq(
      StructField("order_id",   LongType,      nullable = false),
      StructField("customer_id", LongType,     nullable = true), // nullable – to demo filtering
      StructField("category",   StringType,    nullable = false),
      StructField("amount",     DoubleType,    nullable = false),
      StructField("event_time", TimestampType, nullable = true),
    )
  )

  // ---------------------------------------------------------------------------
  // Entry point
  // ---------------------------------------------------------------------------

  /** Run all pipeline stages in dependency order and print results. */
  def run(): Unit = {
    // Step 1 — seed data
    val ordersDF = createOrders()
    saveTable("orders", ordersDF, partitionCols = Seq("order_date"))
    println(s"[1/4] orders:           ${spark.read.table("orders").count()} rows")

    // Step 2 — data quality
    val validatedDF = validateOrders(spark.read.table("orders"))
    saveTable("validated_orders", validatedDF, partitionCols = Seq("order_date"))
    println(s"[2/4] validated_orders: ${spark.read.table("validated_orders").count()} rows")

    // Step 3a — daily revenue
    val dailySalesDF = computeDailySales(spark.read.table("validated_orders"))
    saveTable("daily_sales", dailySalesDF, partitionCols = Seq("order_date"))
    println(s"[3a/4] daily_sales:     ${spark.read.table("daily_sales").count()} rows")

    // Step 3b — customer lifetime value
    val ltvDF = computeCustomerLtv(spark.read.table("validated_orders"))
    saveTable("customer_ltv", ltvDF, partitionCols = Seq.empty)
    println(s"[3b/4] customer_ltv:    ${spark.read.table("customer_ltv").count()} rows")

    println("\n=== Daily Sales ===")
    spark.read.table("daily_sales").show(truncate = false)

    println("=== Customer Lifetime Value ===")
    spark.read.table("customer_ltv").show(truncate = false)

    // Showcase Iceberg-specific features
    showSnapshotHistory()
    demonstrateTimeTravel()
  }

  // ---------------------------------------------------------------------------
  // Step 1 — Source: sample orders
  // ---------------------------------------------------------------------------

  /**
   * Creates the seed orders DataFrame from inline data.
   *
   * In production this would read from an external Iceberg table:
   * {{{
   *   spark.read.table("raw_catalog.sales.orders")
   * }}}
   */
  def createOrders(): DataFrame = {
    val rows = Seq(
      Row(1L,   101L,  "electronics", 299.99, Timestamp.valueOf("2024-01-15 10:00:00")),
      Row(2L,   102L,  "clothing",     49.99, Timestamp.valueOf("2024-01-15 11:30:00")),
      Row(3L,   101L,  "electronics",  89.99, Timestamp.valueOf("2024-01-15 14:00:00")),
      Row(4L,   103L,  "books",        19.99, Timestamp.valueOf("2024-01-16 09:00:00")),
      Row(5L,   102L,  "clothing",     79.99, Timestamp.valueOf("2024-01-16 10:15:00")),
      Row(6L,   104L,  "electronics", 499.99, Timestamp.valueOf("2024-01-16 12:00:00")),
      Row(7L,   103L,  "books",        14.99, Timestamp.valueOf("2024-01-17 08:00:00")),
      Row(8L,   101L,  "home",        129.99, Timestamp.valueOf("2024-01-17 15:00:00")),
      Row(9L,   105L,  "electronics", 349.99, Timestamp.valueOf("2024-01-17 16:30:00")),
      Row(10L,  104L,  "clothing",     59.99, Timestamp.valueOf("2024-01-17 17:00:00")),
      // null customer_id — filtered out in validateOrders
      Row(11L,  null,  "books",         9.99, Timestamp.valueOf("2024-01-17 18:00:00")),
      // negative amount — filtered out in validateOrders
      Row(12L,  106L,  "home",         -5.00, Timestamp.valueOf("2024-01-17 19:00:00")),
    )

    spark
      .createDataFrame(spark.sparkContext.parallelize(rows), ordersSchema)
      .withColumn("order_date", to_date($"event_time"))
  }

  // ---------------------------------------------------------------------------
  // Step 2 — Cleaning: validated_orders
  // ---------------------------------------------------------------------------

  /**
   * Removes rows with null customer IDs or non-positive amounts, and drops
   * the raw `event_time` column in favour of the `order_date` partition key.
   */
  def validateOrders(orders: DataFrame): DataFrame =
    orders
      .filter($"amount" > 0)
      .filter($"customer_id".isNotNull)
      .select("order_id", "customer_id", "category", "amount", "order_date")

  // ---------------------------------------------------------------------------
  // Step 3a — Aggregation: daily_sales
  // ---------------------------------------------------------------------------

  /** Aggregates daily revenue and order count per product category. */
  def computeDailySales(validated: DataFrame): DataFrame =
    validated
      .groupBy("order_date", "category")
      .agg(
        sum("amount").alias("total_amount"),
        count("*").alias("order_count"),
        avg("amount").alias("avg_order_value"),
      )
      .orderBy("order_date", "category")

  // ---------------------------------------------------------------------------
  // Step 3b — Aggregation: customer_ltv
  // ---------------------------------------------------------------------------

  /** Computes lifetime purchase value and order frequency per customer. */
  def computeCustomerLtv(validated: DataFrame): DataFrame =
    validated
      .groupBy("customer_id")
      .agg(
        sum("amount").alias("lifetime_value"),
        count("*").alias("total_orders"),
        min("order_date").alias("first_order_date"),
        max("order_date").alias("last_order_date"),
      )
      .orderBy(desc("lifetime_value"))

  // ---------------------------------------------------------------------------
  // Iceberg feature demos
  // ---------------------------------------------------------------------------

  /** Prints the Iceberg snapshot history for the orders table. */
  private def showSnapshotHistory(): Unit = {
    println("\n=== Iceberg Snapshot History (orders) ===")
    spark
      .sql(
        "SELECT snapshot_id, committed_at, operation " +
          "FROM local.default.orders.snapshots"
      )
      .show(truncate = false)
  }

  /**
   * Reads the orders table at its most recent snapshot ID, demonstrating
   * Iceberg's time-travel capability.
   */
  private def demonstrateTimeTravel(): Unit = {
    val snapshotId = spark
      .sql(
        "SELECT snapshot_id FROM local.default.orders.snapshots " +
          "ORDER BY committed_at DESC LIMIT 1"
      )
      .as[Long]
      .head()

    println(s"\n=== Time Travel: orders at snapshot $snapshotId ===")
    spark.read
      .option("snapshot-id", snapshotId.toString)
      .format("iceberg")
      .load("local.default.orders")
      .show(truncate = false)
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /**
   * Writes a DataFrame to an Iceberg table using `CREATE OR REPLACE TABLE …
   * AS SELECT` (CTAS), which provides atomic overwrite semantics identical to
   * `@dp.materialized_view` in the Python Declarative Pipelines API.
   */
  private def saveTable(
      tableName: String,
      df: DataFrame,
      partitionCols: Seq[String],
  ): Unit = {
    val tmpView = s"_tmp_$tableName"
    df.createOrReplaceTempView(tmpView)

    val partitionClause =
      if (partitionCols.nonEmpty)
        s"PARTITIONED BY (${partitionCols.mkString(", ")})"
      else ""

    spark.sql(
      s"""CREATE OR REPLACE TABLE $tableName
         |USING iceberg
         |$partitionClause
         |TBLPROPERTIES ('format-version' = '2')
         |AS SELECT * FROM $tmpView
         |""".stripMargin
    )
  }
}
