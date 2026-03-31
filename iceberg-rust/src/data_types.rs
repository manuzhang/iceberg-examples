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

//! Demonstrates Iceberg data types available in the Rust API.

use iceberg::spec::{ListType, MapType, NestedField, PrimitiveType, StructType, Type};

/// Demonstrates all primitive and nested Iceberg data types.
pub fn demonstrate_data_types() {
    println!("=== Data Types Examples ===");

    demonstrate_primitive_types();
    demonstrate_nested_types();
}

fn demonstrate_primitive_types() {
    println!("Primitive Types:");

    let primitives: Vec<(&str, PrimitiveType)> = vec![
        ("boolean", PrimitiveType::Boolean),
        ("int", PrimitiveType::Int),
        ("long", PrimitiveType::Long),
        ("float", PrimitiveType::Float),
        ("double", PrimitiveType::Double),
        ("decimal(10, 2)", PrimitiveType::Decimal { precision: 10, scale: 2 }),
        ("date", PrimitiveType::Date),
        ("time", PrimitiveType::Time),
        ("timestamp", PrimitiveType::Timestamp),
        ("timestamptz", PrimitiveType::Timestamptz),
        ("string", PrimitiveType::String),
        ("uuid", PrimitiveType::Uuid),
        ("fixed(16)", PrimitiveType::Fixed(16)),
        ("binary", PrimitiveType::Binary),
    ];

    for (name, t) in &primitives {
        println!("  {}: {:?}", name, t);
    }
}

fn demonstrate_nested_types() {
    println!("Nested Types:");

    // List<string>
    let list_type = Type::List(ListType {
        element_field: NestedField::required(1, "element", Type::Primitive(PrimitiveType::String))
            .into(),
    });
    println!("  list<string>: {}", list_type);

    // Map<string, long>
    let map_type = Type::Map(MapType {
        key_field: NestedField::required(2, "key", Type::Primitive(PrimitiveType::String)).into(),
        value_field: NestedField::optional(3, "value", Type::Primitive(PrimitiveType::Long)).into(),
    });
    println!("  map<string, long>: {}", map_type);

    // Struct<id: long, name: optional string>
    let struct_type = Type::Struct(StructType::new(vec![
        NestedField::required(4, "id", Type::Primitive(PrimitiveType::Long)).into(),
        NestedField::optional(5, "name", Type::Primitive(PrimitiveType::String)).into(),
    ]));
    println!("  struct<id: long, name: optional string>: {}", struct_type);

    // Nested list of structs: list<struct<id: long>>
    let nested_type = Type::List(ListType {
        element_field: NestedField::required(
            6,
            "element",
            Type::Struct(StructType::new(vec![
                NestedField::required(7, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])),
        )
        .into(),
    });
    println!("  list<struct<id: long>>: {}", nested_type);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_primitive_types_are_primitive() {
        let long_type = Type::Primitive(PrimitiveType::Long);
        assert!(long_type.is_primitive());

        let string_type = Type::Primitive(PrimitiveType::String);
        assert!(string_type.is_primitive());
    }

    #[test]
    fn test_decimal_precision_and_scale() {
        let decimal = PrimitiveType::Decimal { precision: 18, scale: 4 };
        if let PrimitiveType::Decimal { precision, scale } = decimal {
            assert_eq!(precision, 18);
            assert_eq!(scale, 4);
        } else {
            panic!("Expected Decimal type");
        }
    }

    #[test]
    fn test_fixed_length() {
        let fixed = PrimitiveType::Fixed(32);
        if let PrimitiveType::Fixed(len) = fixed {
            assert_eq!(len, 32);
        } else {
            panic!("Expected Fixed type");
        }
    }

    #[test]
    fn test_nested_types_are_not_primitive() {
        let list_type = Type::List(ListType {
            element_field: NestedField::required(
                1,
                "element",
                Type::Primitive(PrimitiveType::String),
            )
            .into(),
        });
        assert!(!list_type.is_primitive());
    }
}
