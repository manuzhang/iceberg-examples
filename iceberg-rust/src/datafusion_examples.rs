// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Demonstrates Apache Iceberg + Apache DataFusion integration.
//!
//! This module shows how to combine [`iceberg`] (iceberg-rust) with
//! [`iceberg_datafusion`] to query and write Iceberg tables using
//! DataFusion SQL — the same query engine that powers Apache DataFusion Comet.
//!
//! # What is DataFusion Comet?
//!
//! [Apache DataFusion Comet](https://datafusion.apache.org/comet/) is a
//! high-performance accelerator for Apache Spark that replaces Spark's JVM
//! execution engine with native Rust + DataFusion kernels.  The Iceberg
//! integration shown here uses the same DataFusion execution layer that Comet
//! exposes, making it straightforward to share query logic between standalone
//! DataFusion pipelines and Comet-accelerated Spark jobs.
//!
//! # Examples
//!
//! * Creating an Iceberg catalog backed by the local filesystem
//! * Creating a table with a partition spec
//! * Writing rows via DataFusion `INSERT INTO` SQL
//! * Reading rows back with `SELECT` (plain and filtered)
//! * Running aggregation queries

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Transform, Type, UnboundPartitionSpec};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, Result, TableCreation};
use iceberg_datafusion::IcebergCatalogProvider;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Schema field IDs for the `orders` table
// ---------------------------------------------------------------------------

const ORDERS_SCHEMA_ID: i32 = 1;
const ID_FIELD_ID: i32 = 1;
const CATEGORY_FIELD_ID: i32 = 2;
const AMOUNT_FIELD_ID: i32 = 3;

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Demonstrates Iceberg + DataFusion integration end-to-end.
pub async fn demonstrate_datafusion_integration() -> Result<()> {
    println!("=== Iceberg + DataFusion (Comet) Integration Example ===");

    let dir = TempDir::new().map_err(|e| {
        iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            format!("failed to create temp dir: {e}"),
        )
    })?;
    let warehouse = dir.path().to_str().unwrap().to_string();

    let catalog = build_local_catalog(&warehouse).await?;
    let catalog = Arc::new(catalog);

    setup_namespace_and_table(catalog.clone(), &warehouse).await?;

    let ctx = register_datafusion_catalog(catalog).await?;

    insert_sample_data(&ctx).await?;

    query_all_rows(&ctx).await?;
    query_with_filter(&ctx).await?;
    aggregate_by_category(&ctx).await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Catalog helpers
// ---------------------------------------------------------------------------

/// Builds a `MemoryCatalog` backed by the local filesystem at `warehouse`.
async fn build_local_catalog(
    warehouse: &str,
) -> Result<impl Catalog> {
    MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "example_catalog",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.to_string())]),
        )
        .await
}

/// Creates the `sales` namespace and the `orders` table inside it.
async fn setup_namespace_and_table(
    catalog: Arc<impl Catalog>,
    warehouse: &str,
) -> Result<()> {
    let namespace = NamespaceIdent::new("sales".into());
    catalog.create_namespace(&namespace, HashMap::new()).await?;
    println!("  Created namespace: sales");

    let schema = build_orders_schema()?;

    // Partition by category (identity transform) so each category value
    // is stored in its own directory – a common Iceberg pattern.
    let partition_spec = UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_field(CATEGORY_FIELD_ID, "category", Transform::Identity)?
        .build();

    let table_location = format!("{warehouse}/sales/orders");
    let creation = TableCreation::builder()
        .name("orders".into())
        .location(table_location)
        .schema(schema)
        .partition_spec(partition_spec)
        .properties(HashMap::new())
        .build();

    catalog.create_table(&namespace, creation).await?;
    println!("  Created table: sales.orders (partitioned by category)");

    Ok(())
}

/// Registers the Iceberg catalog as a DataFusion catalog provider.
async fn register_datafusion_catalog(
    catalog: Arc<impl Catalog + 'static>,
) -> Result<SessionContext> {
    let df_catalog = IcebergCatalogProvider::try_new(catalog).await?;
    let ctx = SessionContext::new();
    ctx.register_catalog("iceberg", Arc::new(df_catalog));
    println!("  Registered Iceberg catalog with DataFusion (name: 'iceberg')");
    Ok(ctx)
}

// ---------------------------------------------------------------------------
// DataFusion SQL operations
// ---------------------------------------------------------------------------

/// Inserts sample rows into `iceberg.sales.orders`.
async fn insert_sample_data(ctx: &SessionContext) -> Result<()> {
    println!("\n--- Inserting sample data ---");

    let sql = "
        INSERT INTO iceberg.sales.orders
        VALUES
            (1, 'electronics', 299.99),
            (2, 'electronics', 149.50),
            (3, 'books',       19.99),
            (4, 'books',       34.95),
            (5, 'clothing',    59.00),
            (6, 'clothing',    89.95),
            (7, 'electronics', 599.00),
            (8, 'books',       12.49)
    ";

    let df = ctx.sql(sql).await.map_err(datafusion_err)?;
    let batches = df.collect().await.map_err(datafusion_err)?;

    let rows_inserted: u64 = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::UInt64Array>()
                .map(|a| (0..a.len()).map(|i| a.value(i)).sum::<u64>())
        })
        .sum();

    println!("  Inserted {} rows", rows_inserted);
    Ok(())
}

/// Selects all rows and prints them.
async fn query_all_rows(ctx: &SessionContext) -> Result<()> {
    println!("\n--- SELECT * FROM iceberg.sales.orders ---");

    let df = ctx
        .sql("SELECT id, category, amount FROM iceberg.sales.orders ORDER BY id")
        .await
        .map_err(datafusion_err)?;

    let batches = df.collect().await.map_err(datafusion_err)?;
    print_batches(&batches);
    Ok(())
}

/// Demonstrates predicate (filter) pushdown with a WHERE clause.
async fn query_with_filter(ctx: &SessionContext) -> Result<()> {
    println!("\n--- SELECT with predicate pushdown (category = 'electronics') ---");

    let df = ctx
        .sql(
            "SELECT id, category, amount
             FROM iceberg.sales.orders
             WHERE category = 'electronics'
             ORDER BY id",
        )
        .await
        .map_err(datafusion_err)?;

    let batches = df.collect().await.map_err(datafusion_err)?;
    print_batches(&batches);
    Ok(())
}

/// Demonstrates aggregation queries.
async fn aggregate_by_category(ctx: &SessionContext) -> Result<()> {
    println!("\n--- Aggregation: total amount and order count per category ---");

    let df = ctx
        .sql(
            "SELECT category,
                    COUNT(*) AS order_count,
                    SUM(amount) AS total_amount
             FROM iceberg.sales.orders
             GROUP BY category
             ORDER BY category",
        )
        .await
        .map_err(datafusion_err)?;

    let batches = df.collect().await.map_err(datafusion_err)?;
    print_batches(&batches);
    Ok(())
}

// ---------------------------------------------------------------------------
// Schema definition
// ---------------------------------------------------------------------------

/// Builds the `orders` table schema: id, category, amount.
fn build_orders_schema() -> Result<Schema> {
    Schema::builder()
        .with_schema_id(ORDERS_SCHEMA_ID)
        .with_fields(vec![
            NestedField::required(ID_FIELD_ID, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(CATEGORY_FIELD_ID, "category", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(AMOUNT_FIELD_ID, "amount", Type::Primitive(PrimitiveType::Double)).into(),
        ])
        .build()
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

/// Converts a [`datafusion::error::DataFusionError`] into an [`iceberg::Error`].
fn datafusion_err(e: datafusion::error::DataFusionError) -> iceberg::Error {
    iceberg::Error::new(
        iceberg::ErrorKind::Unexpected,
        format!("DataFusion error: {e}"),
    )
}

/// Prints record-batch results in a simple tabular format.
fn print_batches(batches: &[datafusion::arrow::record_batch::RecordBatch]) {
    use datafusion::arrow::array::{Array, Float64Array, Int32Array, StringArray};

    if batches.is_empty() {
        println!("  (no rows)");
        return;
    }

    let schema = batches[0].schema();
    let headers: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    println!("  {}", headers.join(" | "));
    println!("  {}", "-".repeat(headers.join(" | ").len()));

    for batch in batches {
        for row in 0..batch.num_rows() {
            let mut values = Vec::with_capacity(batch.num_columns());
            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                let val = if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
                    arr.value(row).to_string()
                } else if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
                    format!("{:.2}", arr.value(row))
                } else if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                    arr.value(row).to_string()
                } else if let Some(arr) = col
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::UInt64Array>()
                {
                    arr.value(row).to_string()
                } else {
                    format!("{:?}", col.data_type())
                };
                values.push(val);
            }
            println!("  {}", values.join(" | "));
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    async fn make_catalog_and_context() -> (Arc<impl Catalog>, SessionContext, TempDir) {
        let dir = TempDir::new().unwrap();
        let warehouse = dir.path().to_str().unwrap().to_string();
        let catalog = Arc::new(build_local_catalog(&warehouse).await.unwrap());
        setup_namespace_and_table(catalog.clone(), &warehouse)
            .await
            .unwrap();
        let ctx = register_datafusion_catalog(catalog.clone())
            .await
            .unwrap();
        (catalog, ctx, dir)
    }

    #[tokio::test]
    async fn test_demonstrate_datafusion_integration() {
        assert!(demonstrate_datafusion_integration().await.is_ok());
    }

    #[tokio::test]
    async fn test_orders_schema_field_count() {
        let schema = build_orders_schema().unwrap();
        assert_eq!(schema.as_struct().fields().len(), 3);
    }

    #[tokio::test]
    async fn test_insert_and_query_returns_rows() {
        let (_cat, ctx, _dir) = make_catalog_and_context().await;
        insert_sample_data(&ctx).await.unwrap();

        let df = ctx
            .sql("SELECT COUNT(*) FROM iceberg.sales.orders")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 8);
    }

    #[tokio::test]
    async fn test_filter_returns_correct_rows() {
        let (_cat, ctx, _dir) = make_catalog_and_context().await;
        insert_sample_data(&ctx).await.unwrap();

        let df = ctx
            .sql(
                "SELECT COUNT(*) FROM iceberg.sales.orders WHERE category = 'electronics'",
            )
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_aggregation_returns_correct_categories() {
        let (_cat, ctx, _dir) = make_catalog_and_context().await;
        insert_sample_data(&ctx).await.unwrap();

        let df = ctx
            .sql(
                "SELECT category FROM iceberg.sales.orders GROUP BY category ORDER BY category",
            )
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        use datafusion::arrow::array::Array;
        let categories = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(categories.len(), 3);
        assert_eq!(categories.value(0), "books");
        assert_eq!(categories.value(1), "clothing");
        assert_eq!(categories.value(2), "electronics");
    }
}
