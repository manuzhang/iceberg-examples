package io.github.manuzhang.iceberg.examples;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example demonstrating schema evolution capabilities in Apache Iceberg.
 * Shows how to safely evolve table schemas by adding, renaming, and modifying columns.
 */
public class SchemaEvolutionExample {
  
  private static final Logger LOG = LoggerFactory.getLogger(SchemaEvolutionExample.class);
  
  public static void main(String[] args) {
      LOG.info("Starting Schema Evolution Example...");
      
      SchemaEvolutionExample example = new SchemaEvolutionExample();
      
      try {
          example.demonstrateSchemaEvolution();
          LOG.info("Schema evolution example completed successfully!");
      } catch (Exception e) {
          LOG.error("Error in schema evolution: {}", e.getMessage(), e);
          System.exit(1);
      }
  }
  
  /**
   * Demonstrates various schema evolution concepts.
   */
  public void demonstrateSchemaEvolution() {
      LOG.info("=== Schema Evolution Example ===");
      
      // Create initial schema
      Schema initialSchema = createInitialSchema();
      displaySchema(initialSchema, "Initial Schema");
      
      // Demonstrate schema evolution concepts
      demonstrateSchemaAddition(initialSchema);
      demonstrateTypePromotion();
      demonstrateSchemaRules();
  }
  
  /**
   * Creates an initial schema for demonstration.
   */
  private Schema createInitialSchema() {
      return new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "name", Types.StringType.get()),
          Types.NestedField.optional(3, "age", Types.IntegerType.get())
      );
  }
  
  /**
   * Demonstrates conceptual schema addition.
   */
  private void demonstrateSchemaAddition(Schema initialSchema) {
      LOG.info("=== Schema Addition Concepts ===");
      
      // Create an evolved schema (conceptual)
      Schema evolvedSchema = new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "name", Types.StringType.get()),
          Types.NestedField.optional(3, "age", Types.IntegerType.get()),
          // New fields added
          Types.NestedField.optional(4, "email", Types.StringType.get()),
          Types.NestedField.optional(5, "created_at", Types.TimestampType.withZone()),
          Types.NestedField.optional(6, "metadata", Types.MapType.ofRequired(7, 8,
              Types.StringType.get(), 
              Types.StringType.get()
          ))
      );
      
      displaySchema(evolvedSchema, "Evolved Schema with New Fields");
      
      LOG.info("Schema evolution allows safe addition of optional fields");
      LOG.info("Field IDs must be unique and are never reused");
  }
  
  /**
   * Demonstrates type promotion concepts.
   */
  private void demonstrateTypePromotion() {
      LOG.info("=== Type Promotion Concepts ===");
      
      // Original schema with integer age
      Schema originalSchema = new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "age", Types.IntegerType.get())
      );
      
      // Promoted schema with long age (safe promotion)
      Schema promotedSchema = new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "age", Types.LongType.get())
      );
      
      LOG.info("Original age field type: {}", originalSchema.findType("age"));
      LOG.info("Promoted age field type: {}", promotedSchema.findType("age"));
      
      LOG.info("Safe type promotions:");
      LOG.info("  int → long");
      LOG.info("  float → double");
      LOG.info("  decimal(P,S) → decimal(P',S) where P' > P");
      
      LOG.info("Unsafe type changes (not allowed):");
      LOG.info("  long → int");
      LOG.info("  double → float");
      LOG.info("  string → int");
  }
  
  /**
   * Displays the schema information.
   */
  private void displaySchema(Schema schema, String title) {
      LOG.info("=== {} ===", title);
      
      LOG.info("Schema ID: {}", schema.schemaId());
      LOG.info("Columns:");
      
      schema.columns().forEach(column -> {
          String requiredStatus = column.isRequired() ? "REQUIRED" : "OPTIONAL";
          String docString = column.doc() != null ? " - " + column.doc() : "";
          
          LOG.info("  {} (ID: {}, Type: {}, Status: {}){}", 
              column.name(), 
              column.fieldId(), 
              column.type(), 
              requiredStatus,
              docString
          );
      });
      
      LOG.info("Total columns: {}", schema.columns().size());
  }
  
  /**
   * Demonstrates schema compatibility and evolution rules.
   */
  public void demonstrateSchemaRules() {
      LOG.info("=== Schema Evolution Rules ===");
      
      LOG.info("Safe schema evolution operations:");
      LOG.info("  ✓ Add optional columns");
      LOG.info("  ✓ Rename columns");
      LOG.info("  ✓ Widen primitive types (int → long, float → double)");
      LOG.info("  ✓ Make required columns optional");
      LOG.info("  ✓ Add or remove column documentation");
      
      LOG.info("Unsafe schema evolution operations:");
      LOG.info("  ✗ Delete required columns");
      LOG.info("  ✗ Narrow primitive types (long → int)");
      LOG.info("  ✗ Change column types to incompatible types");
      LOG.info("  ✗ Make optional columns required");
      
      LOG.info("Schema evolution in Iceberg is designed to maintain backward compatibility");
      LOG.info("and ensure that existing data remains readable with evolved schemas.");
  }
}
