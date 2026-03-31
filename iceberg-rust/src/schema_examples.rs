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

//! Demonstrates Iceberg schema creation and inspection using the Rust API.

use iceberg::spec::{ListType, MapType, NestedField, PrimitiveType, Schema, Type};
use iceberg::Result;

/// Demonstrates schema creation and field inspection.
pub fn demonstrate_schema_operations() -> Result<()> {
    println!("=== Schema Operations ===");

    let initial_schema = create_initial_schema()?;
    display_schema(&initial_schema, "Initial Schema");

    let full_schema = create_full_user_schema()?;
    display_schema(&full_schema, "Full User Schema");

    demonstrate_field_lookups(&full_schema);

    Ok(())
}

/// Creates a simple initial schema with a few fields.
fn create_initial_schema() -> Result<Schema> {
    Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            NestedField::required(1, "user_id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "username", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
}

/// Creates a comprehensive user schema with various field types.
fn create_full_user_schema() -> Result<Schema> {
    Schema::builder()
        .with_schema_id(2)
        .with_fields(vec![
            NestedField::required(1, "user_id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "username", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "full_name", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(4, "email", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(5, "age", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(6, "score", Type::Primitive(PrimitiveType::Double)).into(),
            NestedField::required(7, "created_at", Type::Primitive(PrimitiveType::Timestamptz))
                .into(),
            NestedField::optional(8, "last_login", Type::Primitive(PrimitiveType::Timestamptz))
                .into(),
            NestedField::optional(9, "active", Type::Primitive(PrimitiveType::Boolean)).into(),
            NestedField::optional(
                10,
                "tags",
                Type::List(ListType {
                    element_field: NestedField::required(
                        11,
                        "element",
                        Type::Primitive(PrimitiveType::String),
                    )
                    .into(),
                }),
            )
            .into(),
            NestedField::optional(
                12,
                "metadata",
                Type::Map(MapType {
                    key_field: NestedField::required(
                        13,
                        "key",
                        Type::Primitive(PrimitiveType::String),
                    )
                    .into(),
                    value_field: NestedField::optional(
                        14,
                        "value",
                        Type::Primitive(PrimitiveType::String),
                    )
                    .into(),
                }),
            )
            .into(),
        ])
        .build()
}

/// Displays schema information including all fields.
pub fn display_schema(schema: &Schema, title: &str) {
    println!("=== {} ===", title);
    println!("  Schema ID: {}", schema.schema_id());
    println!("  Highest Field ID: {}", schema.highest_field_id());
    println!("  Fields ({}):", schema.as_struct().fields().len());

    for field in schema.as_struct().fields() {
        let required = if field.required { "REQUIRED" } else { "OPTIONAL" };
        let doc = field
            .doc
            .as_deref()
            .map(|d| format!(" -- {}", d))
            .unwrap_or_default();
        println!(
            "    {} (id={}, type={}, {}){} ",
            field.name, field.id, field.field_type, required, doc
        );
    }
}

fn demonstrate_field_lookups(schema: &Schema) {
    println!("=== Field Lookups ===");

    // Look up field by ID
    if let Some(field) = schema.field_by_id(1) {
        println!("  Field by ID 1: {} ({:?})", field.name, field.field_type);
    }

    // Count required vs optional fields
    let required_count = schema
        .as_struct()
        .fields()
        .iter()
        .filter(|f| f.required)
        .count();
    let optional_count = schema.as_struct().fields().len() - required_count;
    println!("  Required fields: {}", required_count);
    println!("  Optional fields: {}", optional_count);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_schema_has_two_fields() {
        let schema = create_initial_schema().unwrap();
        assert_eq!(schema.as_struct().fields().len(), 2);
    }

    #[test]
    fn test_full_schema_has_expected_field_count() {
        let schema = create_full_user_schema().unwrap();
        assert_eq!(schema.as_struct().fields().len(), 11);
    }

    #[test]
    fn test_required_fields_are_required() {
        let schema = create_initial_schema().unwrap();
        let user_id = schema.field_by_id(1).expect("field 1 must exist");
        assert!(user_id.required);
        let username = schema.field_by_id(2).expect("field 2 must exist");
        assert!(username.required);
    }

    #[test]
    fn test_field_lookup_by_id() {
        let schema = create_full_user_schema().unwrap();
        assert!(schema.field_by_id(1).is_some());
        assert!(schema.field_by_id(999).is_none());
    }

    #[test]
    fn test_schema_id() {
        let schema = create_initial_schema().unwrap();
        assert_eq!(schema.schema_id(), 1);
    }

    #[test]
    fn test_highest_field_id() {
        let schema = create_initial_schema().unwrap();
        assert_eq!(schema.highest_field_id(), 2);
    }

    #[test]
    fn test_demonstrate_schema_operations() {
        assert!(demonstrate_schema_operations().is_ok());
    }
}
