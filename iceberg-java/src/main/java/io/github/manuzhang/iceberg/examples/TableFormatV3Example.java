package io.github.manuzhang.iceberg.examples;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.EdgeAlgorithm;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example demonstrating Apache Iceberg Table Format Version 3 (spec v3) features. Iceberg spec v3
 * introduces new data types, default column values, and other enhancements compared to v2.
 *
 * <p>Key v3 additions demonstrated here:
 *
 * <ul>
 *   <li>Nanosecond-precision timestamp types ({@code timestamptz_ns}, {@code timestamp_ns})
 *   <li>Variant type for semi-structured / schema-less data
 *   <li>Geospatial types: Geometry and Geography
 *   <li>Default column values (initial default and write default)
 * </ul>
 */
public class TableFormatV3Example {

  private static final Logger LOG = LoggerFactory.getLogger(TableFormatV3Example.class);

  public static void main(String[] args) {
    LOG.info("Starting Table Format V3 Example...");

    TableFormatV3Example example = new TableFormatV3Example();

    try {
      example.demonstrateV3DataTypes();
      example.demonstrateDefaultValues();
      example.demonstrateV3Schema();
      LOG.info("Table Format V3 example completed successfully!");
    } catch (Exception e) {
      LOG.error("Error in Table Format V3 example: {}", e.getMessage(), e);
      System.exit(1);
    }
  }

  /**
   * Demonstrates the new primitive types introduced in Iceberg spec v3.
   *
   * <p>v3 adds these types on top of the v1/v2 type system:
   *
   * <ul>
   *   <li>{@code timestamptz_ns} – UTC timestamp with nanosecond precision
   *   <li>{@code timestamp_ns} – Local timestamp with nanosecond precision
   *   <li>{@code variant} – Semi-structured data (similar to JSON)
   *   <li>{@code geometry} – Planar geometric shapes (ISO/OGC WKB encoding)
   *   <li>{@code geography} – Spherical / geodetic shapes (ISO/OGC WKB encoding)
   * </ul>
   */
  public void demonstrateV3DataTypes() {
    LOG.info("=== Iceberg v3 Data Types ===");

    // --- Nanosecond timestamps (v3) ---
    // Iceberg v1/v2 offered microsecond-precision timestamps.
    // v3 adds nanosecond-precision variants for high-frequency event data.
    Types.TimestampNanoType tsWithZone = Types.TimestampNanoType.withZone();
    Types.TimestampNanoType tsWithoutZone = Types.TimestampNanoType.withoutZone();

    LOG.info("Nanosecond Timestamps (new in v3):");
    LOG.info("  timestamptz_ns (with zone):    {}", tsWithZone);
    LOG.info("  timestamp_ns   (without zone): {}", tsWithoutZone);

    // Comparison with v1/v2 microsecond timestamps:
    LOG.info("  vs. timestamptz (microseconds, v1/v2): {}", Types.TimestampType.withZone());
    LOG.info("  vs. timestamp   (microseconds, v1/v2): {}", Types.TimestampType.withoutZone());

    // --- Variant type (v3) ---
    // The variant type stores semi-structured data (e.g. JSON documents) without
    // requiring a fixed schema. Values are self-describing at read time.
    Types.VariantType variantType = Types.VariantType.get();
    LOG.info("Variant Type (new in v3):");
    LOG.info("  variant: {}", variantType);

    // --- Geospatial types (v3) ---
    // geometry: planar shapes referenced by a CRS (Coordinate Reference System)
    // geography: spherical / geodetic shapes with an optional edge interpolation algorithm
    Types.GeometryType geometryCrs84 = Types.GeometryType.crs84();
    Types.GeometryType geometryCustom = Types.GeometryType.of("EPSG:4326");
    Types.GeographyType geographyCrs84 = Types.GeographyType.crs84();
    Types.GeographyType geographySpherical =
        Types.GeographyType.of("OGC:CRS84", EdgeAlgorithm.SPHERICAL);

    LOG.info("Geometry Type (new in v3, planar shapes):");
    LOG.info("  geometry(OGC:CRS84):  {}", geometryCrs84);
    LOG.info("  geometry(EPSG:4326):  {}", geometryCustom);
    LOG.info("  default CRS: {}", Types.GeometryType.DEFAULT_CRS);

    LOG.info("Geography Type (new in v3, spherical shapes):");
    LOG.info("  geography(OGC:CRS84):                  {}", geographyCrs84);
    LOG.info("  geography(OGC:CRS84, spherical): {}", geographySpherical);
    LOG.info("  Edge algorithms: SPHERICAL, VINCENTY, THOMAS, ANDOYER, KARNEY");
  }

  /**
   * Demonstrates default column values, a new concept introduced in Iceberg spec v3.
   *
   * <p>Two kinds of defaults are supported:
   *
   * <ul>
   *   <li><b>Initial default</b> – The value used when reading old data files that were written
   *       before the column existed. It makes previously written data appear as if the column was
   *       always present with this value.
   *   <li><b>Write default</b> – The value automatically supplied by a write engine when a new row
   *       is written without an explicit value for the column.
   * </ul>
   */
  public void demonstrateDefaultValues() {
    LOG.info("=== Default Column Values (v3) ===");

    // Build a field with both an initial default and a write default.
    // The builder API is used when defaults need to be specified.
    Types.NestedField statusField =
        Types.NestedField.optional("status")
            .withId(10)
            .ofType(Types.StringType.get())
            .withDoc("User account status")
            .withInitialDefault("active") // retroactively applied to pre-existing rows
            .withWriteDefault("active") // used when writer omits this field
            .build();

    Types.NestedField retriesField =
        Types.NestedField.optional("retry_count")
            .withId(11)
            .ofType(Types.IntegerType.get())
            .withDoc("Number of retries")
            .withInitialDefault(0)
            .withWriteDefault(0)
            .build();

    Types.NestedField scoreField =
        Types.NestedField.optional("score")
            .withId(12)
            .ofType(Types.DoubleType.get())
            .withDoc("User score")
            .withInitialDefault(0.0)
            .withWriteDefault(0.0)
            .build();

    Types.NestedField enabledField =
        Types.NestedField.optional("notifications_enabled")
            .withId(13)
            .ofType(Types.BooleanType.get())
            .withDoc("Whether notifications are enabled")
            .withInitialDefault(true)
            .withWriteDefault(true)
            .build();

    LOG.info("Fields with default values:");
    logFieldWithDefaults(statusField);
    logFieldWithDefaults(retriesField);
    logFieldWithDefaults(scoreField);
    logFieldWithDefaults(enabledField);

    LOG.info("Default value semantics:");
    LOG.info("  initialDefault - value read from old files that pre-date the column's addition");
    LOG.info("  writeDefault   - value written when the producer omits the field");
    LOG.info("  Both are optional: a field may have neither, one, or both defaults");
  }

  /** Demonstrates a complete v3 schema combining the new types and default values. */
  public void demonstrateV3Schema() {
    LOG.info("=== Sample v3 Schema ===");

    // A schema for a sensor-event table that makes use of multiple v3 features.
    Schema sensorSchema =
        new Schema(
            // Core identity fields (compatible with v1/v2)
            Types.NestedField.required(1, "event_id", Types.UUIDType.get()),
            Types.NestedField.required(2, "sensor_id", Types.LongType.get()),

            // Nanosecond timestamp for high-frequency sensor data (v3)
            Types.NestedField.required(3, "event_time", Types.TimestampNanoType.withZone()),

            // Flexible payload stored as variant (v3)
            Types.NestedField.optional(4, "payload", Types.VariantType.get()),

            // Sensor location stored as a geographic point (v3)
            Types.NestedField.optional(5, "location", Types.GeographyType.crs84()),

            // Fields with write defaults (v3)
            Types.NestedField.optional("severity")
                .withId(6)
                .ofType(Types.StringType.get())
                .withInitialDefault("info")
                .withWriteDefault("info")
                .build(),
            Types.NestedField.optional("processed")
                .withId(7)
                .ofType(Types.BooleanType.get())
                .withInitialDefault(false)
                .withWriteDefault(false)
                .build());

    LOG.info("Sensor event schema (v3):");
    sensorSchema
        .columns()
        .forEach(
            col -> {
              String defaults = "";
              if (col.initialDefault() != null || col.writeDefault() != null) {
                defaults =
                    String.format(
                        " [initialDefault=%s, writeDefault=%s]",
                        col.initialDefault(), col.writeDefault());
              }
              LOG.info(
                  "  {} (ID: {}, Type: {}, Required: {}){}",
                  col.name(),
                  col.fieldId(),
                  col.type(),
                  col.isRequired(),
                  defaults);
            });

    LOG.info("Total columns: {}", sensorSchema.columns().size());

    LOG.info("=== Iceberg Format Version 3 Overview ===");
    LOG.info("Key additions in table spec v3 compared to v2:");
    LOG.info("  + nanosecond-precision timestamp types (timestamptz_ns, timestamp_ns)");
    LOG.info("  + variant type for semi-structured / schema-less data");
    LOG.info("  + geometry and geography types for geospatial data");
    LOG.info("  + default column values (initialDefault, writeDefault)");
    LOG.info("  + row lineage tracking for change-data-capture (CDC)");
    LOG.info("  + binary deletion vectors for efficient row-level deletes");
    LOG.info("To create a v3 table, set the table property:");
    LOG.info("  format-version = 3");
  }

  private void logFieldWithDefaults(Types.NestedField field) {
    LOG.info(
        "  {} (ID: {}, Type: {}) initialDefault={}, writeDefault={}",
        field.name(),
        field.fieldId(),
        field.type(),
        field.initialDefault(),
        field.writeDefault());
  }
}
