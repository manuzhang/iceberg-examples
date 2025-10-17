package io.github.manuzhang.iceberg.examples;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Iceberg examples.
 * These tests demonstrate basic functionality without requiring external dependencies.
 */
class IcebergExamplesTest {
  
  private IcebergExamples examples;
  
  @BeforeEach
  void setUp() {
   examples = new IcebergExamples();
  }
  
  @Test
  @DisplayName("Test basic schema creation")
  void testSchemaCreation() {
   // Create a test schema
   Schema schema = new Schema(
   Types.NestedField.required(1, "id", Types.LongType.get()),
   Types.NestedField.required(2, "name", Types.StringType.get()),
   Types.NestedField.optional(3, "age", Types.IntegerType.get())
   );
   
   // Verify schema properties
   assertNotNull(schema);
   assertEquals(3, schema.columns().size());
   
   // Verify field properties
   assertTrue(schema.findField("id").isRequired());
   assertTrue(schema.findField("name").isRequired());
   assertFalse(schema.findField("age").isRequired());
  }
  
  @Test
  @DisplayName("Test schema field types")
  void testSchemaFieldTypes() {
   Schema schema = new Schema(
   Types.NestedField.required(1, "id", Types.LongType.get()),
   Types.NestedField.required(2, "name", Types.StringType.get()),
   Types.NestedField.optional(3, "age", Types.IntegerType.get()),
   Types.NestedField.optional(4, "active", Types.BooleanType.get()),
   Types.NestedField.optional(5, "timestamp", Types.TimestampType.withZone())
   );
   
   // Test field types
   assertEquals(Types.LongType.get(), schema.findType("id"));
   assertEquals(Types.StringType.get(), schema.findType("name"));
   assertEquals(Types.IntegerType.get(), schema.findType("age"));
   assertEquals(Types.BooleanType.get(), schema.findType("active"));
   assertEquals(Types.TimestampType.withZone(), schema.findType("timestamp"));
  }
  
  @Test
  @DisplayName("Test schema evolution concepts")
  void testSchemaEvolution() {
   // Initial schema
   Schema v1Schema = new Schema(
   Types.NestedField.required(1, "id", Types.LongType.get()),
   Types.NestedField.required(2, "name", Types.StringType.get())
   );
   
   // Evolved schema (conceptual - in practice this would be done through table operations)
   Schema v2Schema = new Schema(
   Types.NestedField.required(1, "id", Types.LongType.get()),
   Types.NestedField.required(2, "name", Types.StringType.get()),
   Types.NestedField.optional(3, "email", Types.StringType.get()),
   Types.NestedField.optional(4, "created_at", Types.TimestampType.withZone())
   );
   
   // Verify evolution
   assertEquals(2, v1Schema.columns().size());
   assertEquals(4, v2Schema.columns().size());
   
   // Verify backward compatibility - all v1 fields exist in v2
   assertNotNull(v2Schema.findField("id"));
   assertNotNull(v2Schema.findField("name"));
   
   // Verify new fields are optional
   assertFalse(v2Schema.findField("email").isRequired());
   assertFalse(v2Schema.findField("created_at").isRequired());
  }
  
  @Test
  @DisplayName("Test field ID uniqueness")
  void testFieldIdUniqueness() {
   Schema schema = new Schema(
   Types.NestedField.required(1, "id", Types.LongType.get()),
   Types.NestedField.required(2, "name", Types.StringType.get()),
   Types.NestedField.optional(3, "age", Types.IntegerType.get())
   );
   
   // Verify all field IDs are unique
   long distinctIds = schema.columns().stream()
   .mapToInt(Types.NestedField::fieldId)
   .distinct()
   .count();
   
   assertEquals(schema.columns().size(), distinctIds);
  }
  
  @Test
  @DisplayName("Test examples class instantiation")
  void testExamplesInstantiation() {
   assertNotNull(examples);
   assertTrue(examples instanceof IcebergExamples);
  }
}
