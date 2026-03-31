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

//! Demonstrates Iceberg catalog and table operations using an in-memory catalog.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::io::MemoryStorageFactory;
use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
use iceberg::spec::{NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder, Type};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, Result, TableCreation, TableIdent};

/// Demonstrates catalog and table operations using an in-memory catalog.
pub async fn demonstrate_catalog_operations() -> Result<()> {
    println!("=== Catalog Operations Example ===");

    let catalog = build_memory_catalog().await?;

    demonstrate_namespace_operations(&catalog).await?;
    demonstrate_table_creation(&catalog).await?;
    demonstrate_table_listing(&catalog).await?;

    Ok(())
}

/// Builds an in-memory catalog backed by in-memory storage.
async fn build_memory_catalog() -> Result<impl Catalog> {
    MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(MemoryStorageFactory))
        .load(
            "example_catalog",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                "/tmp/iceberg-warehouse".to_string(),
            )]),
        )
        .await
}

async fn demonstrate_namespace_operations(catalog: &impl Catalog) -> Result<()> {
    println!("--- Namespace Operations ---");

    // Create namespaces
    let ns_default = NamespaceIdent::new("default".into());
    let ns_analytics = NamespaceIdent::new("analytics".into());

    catalog
        .create_namespace(&ns_default, HashMap::new())
        .await?;
    catalog
        .create_namespace(&ns_analytics, HashMap::new())
        .await?;

    // List namespaces
    let namespaces = catalog.list_namespaces(None).await?;
    println!("  Namespaces ({}):", namespaces.len());
    for ns in &namespaces {
        println!("    - {}", ns);
    }

    Ok(())
}

async fn demonstrate_table_creation(catalog: &impl Catalog) -> Result<()> {
    println!("--- Table Creation ---");

    let namespace = NamespaceIdent::new("default".into());

    // Create a users table
    let users_schema = build_users_schema()?;
    let users_table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("users".into())
                .schema(users_schema)
                .partition_spec(PartitionSpec::unpartition_spec())
                .sort_order(SortOrder::unsorted_order())
                .build(),
        )
        .await?;
    println!(
        "  Created table: {} (schema_id={})",
        users_table.identifier(),
        users_table.metadata().current_schema().schema_id()
    );

    // Create an events table
    let events_schema = build_events_schema()?;
    let events_table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("events".into())
                .schema(events_schema)
                .partition_spec(PartitionSpec::unpartition_spec())
                .sort_order(SortOrder::unsorted_order())
                .build(),
        )
        .await?;
    println!(
        "  Created table: {} (schema_id={})",
        events_table.identifier(),
        events_table.metadata().current_schema().schema_id()
    );

    // Display the users table schema
    println!("  Users table schema:");
    for field in users_table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
    {
        let required = if field.required { "required" } else { "optional" };
        println!("    {} (id={}, {}): {}", field.name, field.id, required, field.field_type);
    }

    Ok(())
}

async fn demonstrate_table_listing(catalog: &impl Catalog) -> Result<()> {
    println!("--- Table Listing ---");

    let namespace = NamespaceIdent::new("default".into());
    let tables = catalog.list_tables(&namespace).await?;

    println!("  Tables in 'default' namespace ({}):", tables.len());
    for table_ident in &tables {
        println!("    - {}", table_ident);
    }

    // Check table existence
    let users_ident = TableIdent::from_strs(["default", "users"])?;
    let exists = catalog.table_exists(&users_ident).await?;
    println!("  Table 'default.users' exists: {}", exists);

    let nonexistent = TableIdent::from_strs(["default", "nonexistent"])?;
    let not_exists = catalog.table_exists(&nonexistent).await?;
    println!("  Table 'default.nonexistent' exists: {}", not_exists);

    Ok(())
}

fn build_users_schema() -> Result<Schema> {
    Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "username", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "email", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(4, "age", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(5, "created_at", Type::Primitive(PrimitiveType::Timestamptz))
                .into(),
            NestedField::optional(6, "active", Type::Primitive(PrimitiveType::Boolean)).into(),
        ])
        .build()
}

fn build_events_schema() -> Result<Schema> {
    Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            NestedField::required(1, "event_id", Type::Primitive(PrimitiveType::Uuid)).into(),
            NestedField::required(2, "user_id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(3, "event_type", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(4, "event_time", Type::Primitive(PrimitiveType::Timestamptz))
                .into(),
            NestedField::optional(5, "payload", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_users_schema_field_count() {
        let schema = build_users_schema().unwrap();
        assert_eq!(schema.as_struct().fields().len(), 6);
    }

    #[test]
    fn test_events_schema_field_count() {
        let schema = build_events_schema().unwrap();
        assert_eq!(schema.as_struct().fields().len(), 5);
    }

    #[test]
    fn test_users_schema_required_fields() {
        let schema = build_users_schema().unwrap();
        // id, username, created_at are required
        assert!(schema.field_by_id(1).unwrap().required);
        assert!(schema.field_by_id(2).unwrap().required);
        assert!(schema.field_by_id(5).unwrap().required);
        // email, age, active are optional
        assert!(!schema.field_by_id(3).unwrap().required);
        assert!(!schema.field_by_id(4).unwrap().required);
        assert!(!schema.field_by_id(6).unwrap().required);
    }

    #[tokio::test]
    async fn test_catalog_operations() {
        assert!(demonstrate_catalog_operations().await.is_ok());
    }

    #[tokio::test]
    async fn test_table_exists_after_creation() {
        let catalog = build_memory_catalog().await.unwrap();
        let ns = NamespaceIdent::new("test_ns".into());
        catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

        let schema = build_users_schema().unwrap();
        catalog
            .create_table(
                &ns,
                TableCreation::builder()
                    .name("users".into())
                    .schema(schema)
                    .build(),
            )
            .await
            .unwrap();

        let ident = TableIdent::from_strs(["test_ns", "users"]).unwrap();
        assert!(catalog.table_exists(&ident).await.unwrap());
    }

    #[tokio::test]
    async fn test_table_listing() {
        let catalog = build_memory_catalog().await.unwrap();
        let ns = NamespaceIdent::new("listing_ns".into());
        catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

        let schema = build_events_schema().unwrap();
        catalog
            .create_table(
                &ns,
                TableCreation::builder()
                    .name("events".into())
                    .schema(schema)
                    .build(),
            )
            .await
            .unwrap();

        let tables = catalog.list_tables(&ns).await.unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "events");
    }
}
