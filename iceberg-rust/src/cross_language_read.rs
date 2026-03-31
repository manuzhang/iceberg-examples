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

//! Demonstrates reading an Iceberg table written by the Java example.
//!
//! Run the Java write example first:
//! ```
//! cd ../iceberg-java && ./gradlew runCrossLanguageWrite
//! ```
//! Then run this example via `cargo run` (from `iceberg-rust/`).

use futures::TryStreamExt;
use iceberg::{io::FileIO, table::StaticTable, Result, TableIdent};

use arrow_cast::pretty::pretty_format_batches;

/// Location of the Iceberg table written by the Java example.
pub const TABLE_LOCATION: &str = "/tmp/iceberg-java-rust-example";

/// Returns the path to the current metadata JSON file by reading the
/// `version-hint.text` file that Iceberg writes on every commit.
///
/// Returns `None` if the table does not exist or the hint cannot be parsed.
fn current_metadata_path(table_location: &str) -> Option<String> {
    let hint_path = format!("{table_location}/metadata/version-hint.text");
    let version: u32 = std::fs::read_to_string(&hint_path)
        .ok()?
        .trim()
        .parse()
        .ok()?;
    Some(format!("{table_location}/metadata/v{version}.metadata.json"))
}

/// Reads the Iceberg table written by the Java example and prints its contents.
///
/// If the table has not yet been written (i.e. the Java example has not been
/// run), this function prints a helpful message and returns `Ok(())`.
pub async fn demonstrate_cross_language_read() -> Result<()> {
    println!("=== Cross-Language Read Example ===");

    let Some(metadata_path) = current_metadata_path(TABLE_LOCATION) else {
        println!("  Iceberg table not found at: {TABLE_LOCATION}");
        println!("  Run the Java write example first:");
        println!("    cd ../iceberg-java && ./gradlew runCrossLanguageWrite");
        return Ok(());
    };

    // Use the local-filesystem FileIO.  It normalises file:/, file://, and bare
    // absolute paths, so it handles whatever path format Java stored in the
    // Iceberg manifest files.
    let file_io = FileIO::new_with_fs();

    let table_ident = TableIdent::from_strs(["default", "cross_language_example"])?;
    let table =
        StaticTable::from_metadata_file(&metadata_path, table_ident, file_io).await?;

    println!("  Format version : {}", table.metadata().format_version() as u8);
    println!("  Schema:");
    for field in table.metadata().current_schema().as_struct().fields() {
        let req = if field.required { "required" } else { "optional" };
        println!("    - {} ({}): {}", field.name, req, field.field_type);
    }

    // Scan the table and collect all Arrow record batches.
    let scan = table.scan().build()?;
    let batches: Vec<_> = scan.to_arrow().await?.try_collect().await?;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("  Total rows: {total_rows}");

    if !batches.is_empty() {
        println!("\n  Table data:");
        let formatted = pretty_format_batches(&batches)
            .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;
        println!("{formatted}");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_location_is_set() {
        assert!(!TABLE_LOCATION.is_empty());
    }

    /// Verifies that the read function handles a missing table gracefully
    /// (returns Ok rather than panicking or returning an Err).
    #[tokio::test]
    async fn test_read_gracefully_handles_missing_table() {
        // Remove any previously written table so the test is deterministic.
        let _ = std::fs::remove_dir_all(TABLE_LOCATION);
        let result = demonstrate_cross_language_read().await;
        assert!(result.is_ok(), "should handle missing table gracefully: {result:?}");
    }
}
