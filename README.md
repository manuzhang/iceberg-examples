# Iceberg Examples

A collection of examples demonstrating the [Apache Iceberg](https://iceberg.apache.org/) API across
multiple languages.

## Sub-projects

| Directory | Language | Description |
|-----------|----------|-------------|
| [`iceberg-java/`](iceberg-java/) | Java | Examples using the Apache Iceberg Java API |
| [`iceberg-rust/`](iceberg-rust/) | Rust | Examples using the Apache Iceberg Rust API |

## Cross-Language Example

The repository includes an end-to-end example that shows Iceberg's language-agnostic table format
in action: a Java program writes a `users` Iceberg table to the local filesystem, and the Rust
program reads it back.

```bash
# 1. Write the table from Java
cd iceberg-java
./gradlew runCrossLanguageWrite

# 2. Read it from Rust
cd ../iceberg-rust
cargo run
```

## Learning Resources

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Iceberg Java API Quickstart](https://iceberg.apache.org/docs/latest/java-api-quickstart/)
- [Apache Iceberg Rust Documentation](https://rust.iceberg.apache.org/)
- [Iceberg Table Format Specification](https://iceberg.apache.org/spec/)

## License

This project is licensed under the same terms as the Apache Iceberg project.
