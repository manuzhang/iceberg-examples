package io.github.manuzhang.iceberg.examples;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class demonstrating basic Apache Iceberg operations.
 * This example shows how to create tables, work with schemas, and interact with the Iceberg catalog.
 */
public class IcebergExamples {
  
  private static final Logger LOG = LoggerFactory.getLogger(IcebergExamples.class);
  
  public static void main(String[] args) {
   LOG.info("Starting Iceberg Examples...");
   
   IcebergExamples examples = new IcebergExamples();
   
   try {
   examples.schemaOperations();
   examples.dataTypeExamples();
   LOG.info("All examples completed successfully!");
   } catch (Exception e) {
   LOG.error("Error running examples: {}", e.getMessage(), e);
   System.exit(1);
   }
  }
  
  /**
   * Demonstrates various Iceberg data types and their usage.
   */
  public void dataTypeExamples() {
   LOG.info("=== Data Types Examples ===");
   
   // Primitive types
   LOG.info("Primitive Types:");
   LOG.info("  Boolean: {}", Types.BooleanType.get());
   LOG.info("  Integer: {}", Types.IntegerType.get());
   LOG.info("  Long: {}", Types.LongType.get());
   LOG.info("  Float: {}", Types.FloatType.get());
   LOG.info("  Double: {}", Types.DoubleType.get());
   LOG.info("  String: {}", Types.StringType.get());
   LOG.info("  Date: {}", Types.DateType.get());
   LOG.info("  Time: {}", Types.TimeType.get());
   LOG.info("  Timestamp: {}", Types.TimestampType.withZone());
   LOG.info("  TimestampNTZ: {}", Types.TimestampType.withoutZone());
   LOG.info("  Binary: {}", Types.BinaryType.get());
   LOG.info("  UUID: {}", Types.UUIDType.get());
   
   // Complex types
   LOG.info("Complex Types:");
   LOG.info("  Decimal(10,2): {}", Types.DecimalType.of(10, 2));
   LOG.info("  Fixed(16): {}", Types.FixedType.ofLength(16));
   
   // Collection types
   LOG.info("Collection Types:");
   LOG.info("  List<String>: {}", Types.ListType.ofRequired(1, Types.StringType.get()));
   LOG.info("  Map<String,Long>: {}", Types.MapType.ofRequired(1, 2, Types.StringType.get(), Types.LongType.get()));
   
   // Struct type
   Types.StructType structType = Types.StructType.of(
   Types.NestedField.required(1, "id", Types.LongType.get()),
   Types.NestedField.optional(2, "name", Types.StringType.get())
   );
   LOG.info("  Struct: {}", structType);
  }
  
  /**
   * Demonstrates schema operations and evolution.
   */
  public void schemaOperations() {
   LOG.info("=== Schema Operations ===");
   
   // Create a basic schema
   Schema initialSchema = new Schema(
   Types.NestedField.required(1, "user_id", Types.LongType.get()),
   Types.NestedField.required(2, "username", Types.StringType.get())
   );
   
   LOG.info("Initial schema: {}", initialSchema);
   
   // Demonstrate schema evolution (conceptual - would require table operations)
   Schema evolvedSchema = new Schema(
   Types.NestedField.required(1, "user_id", Types.LongType.get()),
   Types.NestedField.required(2, "username", Types.StringType.get()),
   Types.NestedField.optional(3, "full_name", Types.StringType.get()),
   Types.NestedField.optional(4, "last_login", Types.TimestampType.withZone())
   );
   
   LOG.info("Evolved schema: {}", evolvedSchema);
   
   // Show schema field details
   LOG.info("Schema fields:");
   evolvedSchema.columns().forEach(column -> {
   LOG.info("  Field: {} (ID: {}, Type: {}, Required: {})", 
     column.name(), 
     column.fieldId(), 
     column.type(), 
     column.isRequired()
   );
   });
  }
}
