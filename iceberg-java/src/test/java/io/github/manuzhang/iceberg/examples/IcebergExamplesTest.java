package io.github.manuzhang.iceberg.examples;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.EdgeAlgorithm;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for Iceberg examples. These tests demonstrate basic functionality without requiring
 * external dependencies.
 */
public class IcebergExamplesTest {

  private IcebergExamples examples;

  @Before
  public void setUp() {
    examples = new IcebergExamples();
  }

  @Test
  public void testSchemaCreation() {
    // Create a test schema
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "age", Types.IntegerType.get()));

    // Verify schema properties
    assertNotNull(schema);
    assertEquals(3, schema.columns().size());

    // Verify field properties
    assertTrue(schema.findField("id").isRequired());
    assertTrue(schema.findField("name").isRequired());
    assertFalse(schema.findField("age").isRequired());
  }

  @Test
  public void testSchemaFieldTypes() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "age", Types.IntegerType.get()),
            Types.NestedField.optional(4, "active", Types.BooleanType.get()),
            Types.NestedField.optional(5, "timestamp", Types.TimestampType.withZone()));

    // Test field types
    assertEquals(Types.LongType.get(), schema.findType("id"));
    assertEquals(Types.StringType.get(), schema.findType("name"));
    assertEquals(Types.IntegerType.get(), schema.findType("age"));
    assertEquals(Types.BooleanType.get(), schema.findType("active"));
    assertEquals(Types.TimestampType.withZone(), schema.findType("timestamp"));
  }

  @Test
  public void testSchemaEvolution() {
    // Initial schema
    Schema v1Schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));

    // Evolved schema (conceptual - in practice this would be done through table
    // operations)
    Schema v2Schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "email", Types.StringType.get()),
            Types.NestedField.optional(4, "created_at", Types.TimestampType.withZone()));

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
  public void testFieldIdUniqueness() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "age", Types.IntegerType.get()));

    // Verify all field IDs are unique
    long distinctIds =
        schema.columns().stream().mapToInt(Types.NestedField::fieldId).distinct().count();

    assertEquals(schema.columns().size(), distinctIds);
  }

  @Test
  public void testExamplesInstantiation() {
    assertNotNull(examples);
    assertTrue(examples instanceof IcebergExamples);
  }

  @Test
  public void testTimestampNanoTypes() {
    Types.TimestampNanoType tsWithZone = Types.TimestampNanoType.withZone();
    Types.TimestampNanoType tsWithoutZone = Types.TimestampNanoType.withoutZone();

    assertNotNull(tsWithZone);
    assertNotNull(tsWithoutZone);
    assertTrue(tsWithZone.shouldAdjustToUTC());
    assertFalse(tsWithoutZone.shouldAdjustToUTC());
    assertNotEquals(tsWithZone, tsWithoutZone);
  }

  @Test
  public void testVariantType() {
    Types.VariantType variantType = Types.VariantType.get();

    assertNotNull(variantType);
    assertTrue(variantType.isVariantType());
    assertEquals(variantType, Types.VariantType.get()); // singleton
  }

  @Test
  public void testGeospatialTypes() {
    Types.GeometryType geometryCrs84 = Types.GeometryType.crs84();
    Types.GeometryType geometryCustom = Types.GeometryType.of("EPSG:4326");
    Types.GeographyType geographyCrs84 = Types.GeographyType.crs84();
    Types.GeographyType geographySpherical =
        Types.GeographyType.of("OGC:CRS84", EdgeAlgorithm.SPHERICAL);

    assertNotNull(geometryCrs84);
    assertNotNull(geometryCustom);
    assertNotNull(geographyCrs84);
    assertNotNull(geographySpherical);

    // crs84() stores null internally (the default CRS); custom CRS stores the given string
    assertNull(geometryCrs84.crs());
    assertEquals("EPSG:4326", geometryCustom.crs());
    // crs84() stores null internally for both crs and algorithm
    assertNull(geographyCrs84.crs());
    assertNull(geographyCrs84.algorithm());
    assertEquals(EdgeAlgorithm.SPHERICAL, geographySpherical.algorithm());
  }

  @Test
  public void testDefaultColumnValues() {
    Types.NestedField fieldWithDefaults =
        Types.NestedField.optional("status")
            .withId(1)
            .ofType(Types.StringType.get())
            .withInitialDefault("active")
            .withWriteDefault("active")
            .build();

    assertNotNull(fieldWithDefaults);
    assertEquals("active", fieldWithDefaults.initialDefault());
    assertEquals("active", fieldWithDefaults.writeDefault());
    assertFalse(fieldWithDefaults.isRequired());
  }

  @Test
  public void testV3Schema() {
    Schema v3Schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "event_time", Types.TimestampNanoType.withZone()),
            Types.NestedField.optional(3, "payload", Types.VariantType.get()),
            Types.NestedField.optional(4, "location", Types.GeographyType.crs84()),
            Types.NestedField.optional("status")
                .withId(5)
                .ofType(Types.StringType.get())
                .withInitialDefault("active")
                .withWriteDefault("active")
                .build());

    assertEquals(5, v3Schema.columns().size());
    assertEquals(Types.TimestampNanoType.withZone(), v3Schema.findType("event_time"));
    assertTrue(v3Schema.findType("payload").isVariantType());
    assertEquals("active", v3Schema.findField("status").initialDefault());
  }

  @Test
  public void testTableFormatV3Example() {
    TableFormatV3Example v3Example = new TableFormatV3Example();
    assertNotNull(v3Example);
    v3Example.demonstrateV3DataTypes();
    v3Example.demonstrateDefaultValues();
    v3Example.demonstrateV3Schema();
  }
}
