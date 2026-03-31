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

//! Apache Iceberg Rust examples.
//!
//! Demonstrates schema definition, data types, schema evolution, and
//! catalog operations using the [`iceberg`] crate.

mod catalog_examples;
mod data_types;
mod schema_evolution;
mod schema_examples;

#[tokio::main]
async fn main() -> iceberg::Result<()> {
    println!("Starting Apache Iceberg Rust Examples...\n");

    schema_examples::demonstrate_schema_operations()?;
    println!();

    data_types::demonstrate_data_types();
    println!();

    schema_evolution::demonstrate_schema_evolution()?;
    println!();

    catalog_examples::demonstrate_catalog_operations().await?;

    println!("\nAll examples completed successfully!");
    Ok(())
}
