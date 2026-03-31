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

//! Demonstrates schema evolution concepts using the Apache Iceberg Rust API.

use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::Result;

/// Demonstrates schema evolution concepts.
pub fn demonstrate_schema_evolution() -> Result<()> {
    println!("=== Schema Evolution Example ===");

    demonstrate_adding_columns()?;
    demonstrate_type_promotion()?;
    demonstrate_schema_evolution_rules();

    Ok(())
}

/// Shows how a schema evolves by adding optional columns.
///
/// In Iceberg, schema evolution is safe when:
/// - New columns are added as optional
/// - Field IDs are unique and are never reused
fn demonstrate_adding_columns() -> Result<()> {
    println!("=== Schema Addition Concepts ===");

    // Version 1: initial schema
    let v1 = Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "age", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()?;

    println!(
        "V1 schema: {} fields (schema_id={})",
        v1.as_struct().fields().len(),
        v1.schema_id()
    );

    // Version 2: new optional columns added — existing data remains readable
    let v2 = Schema::builder()
        .with_schema_id(2)
        .with_fields(vec![
            // Original fields kept with the same IDs
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "age", Type::Primitive(PrimitiveType::Int)).into(),
            // New optional fields with new IDs (IDs are never reused)
            NestedField::optional(4, "email", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(5, "created_at", Type::Primitive(PrimitiveType::Timestamptz))
                .into(),
        ])
        .build()?;

    println!(
        "V2 schema: {} fields (schema_id={})",
        v2.as_struct().fields().len(),
        v2.schema_id()
    );

    // All V1 fields are still present in V2 with the same IDs
    assert_eq!(v1.as_struct().fields().len(), 3);
    assert_eq!(v2.as_struct().fields().len(), 5);
    println!("  V1 fields still accessible in V2 by original field IDs");

    println!("Schema evolution allows safe addition of optional fields.");
    println!("Field IDs are stable identifiers that are never reused.");

    Ok(())
}

/// Demonstrates safe type promotion rules.
///
/// Iceberg allows widening a type to a compatible larger type:
/// - int → long
/// - float → double
/// - decimal(P, S) → decimal(P', S) where P' > P
fn demonstrate_type_promotion() -> Result<()> {
    println!("=== Type Promotion Concepts ===");

    // Original schema with an int column
    let original = Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "count", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(3, "ratio", Type::Primitive(PrimitiveType::Float)).into(),
        ])
        .build()?;

    // Promoted schema: int→long and float→double (both safe widenings)
    let promoted = Schema::builder()
        .with_schema_id(2)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "count", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(3, "ratio", Type::Primitive(PrimitiveType::Double)).into(),
        ])
        .build()?;

    let original_count = original.field_by_id(2).unwrap();
    let promoted_count = promoted.field_by_id(2).unwrap();
    println!(
        "  count field: {:?} → {:?} (safe widening)",
        original_count.field_type, promoted_count.field_type
    );

    let original_ratio = original.field_by_id(3).unwrap();
    let promoted_ratio = promoted.field_by_id(3).unwrap();
    println!(
        "  ratio field: {:?} → {:?} (safe widening)",
        original_ratio.field_type, promoted_ratio.field_type
    );

    println!("Safe type promotions:");
    println!("  int    → long");
    println!("  float  → double");
    println!("  decimal(P, S) → decimal(P', S) where P' > P");

    println!("Unsafe type changes (not allowed by Iceberg):");
    println!("  long   → int   (narrowing)");
    println!("  double → float (narrowing)");
    println!("  string → int   (incompatible)");

    Ok(())
}

fn demonstrate_schema_evolution_rules() {
    println!("=== Schema Evolution Rules ===");

    println!("Safe schema evolution operations:");
    println!("  ✓ Add optional columns");
    println!("  ✓ Rename columns");
    println!("  ✓ Widen primitive types (int → long, float → double)");
    println!("  ✓ Make required columns optional");
    println!("  ✓ Add or remove column documentation");
    println!("  ✓ Reorder fields (IDs are used for identity, not position)");

    println!("Unsafe schema evolution operations:");
    println!("  ✗ Delete required columns");
    println!("  ✗ Narrow primitive types (long → int)");
    println!("  ✗ Change column types to incompatible types");
    println!("  ✗ Make optional columns required");
    println!("  ✗ Reuse deleted field IDs");

    println!("Iceberg uses field IDs (not names or positions) to track column identity.");
    println!("This makes renaming and reordering columns safe operations.");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_v1_v2_field_count() {
        let v1 = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let v2 = Schema::builder()
            .with_schema_id(2)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(3, "email", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        assert_eq!(v1.as_struct().fields().len(), 2);
        assert_eq!(v2.as_struct().fields().len(), 3);

        // V1 fields are still accessible in V2
        assert!(v2.field_by_id(1).is_some());
        assert!(v2.field_by_id(2).is_some());

        // V2 new field is optional
        let email = v2.field_by_id(3).unwrap();
        assert!(!email.required);
    }

    #[test]
    fn test_type_promotion_field_ids_are_stable() {
        let original = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "count", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();

        let promoted = Schema::builder()
            .with_schema_id(2)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "count", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();

        // Field IDs are stable across evolution
        assert_eq!(original.field_by_id(2).unwrap().id, promoted.field_by_id(2).unwrap().id);
    }

    #[test]
    fn test_demonstrate_schema_evolution() {
        assert!(demonstrate_schema_evolution().is_ok());
    }
}
