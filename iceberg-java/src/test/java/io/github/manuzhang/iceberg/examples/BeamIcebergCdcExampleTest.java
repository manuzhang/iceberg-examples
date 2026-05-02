package io.github.manuzhang.iceberg.examples;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;

/** Unit tests for the Apache Beam + Iceberg CDC example. */
public class BeamIcebergCdcExampleTest {

  @Test
  public void testCustomerSchema() {
    Schema schema = BeamIcebergCdcExample.CUSTOMER_SCHEMA;

    assertEquals(4, schema.getFieldCount());
    assertTrue(schema.hasField("customer_id"));
    assertTrue(schema.hasField("name"));
    assertTrue(schema.hasField("email"));
    assertTrue(schema.hasField("signup_time"));
  }

  @Test
  public void testCustomerSchemaTypes() {
    Schema schema = BeamIcebergCdcExample.CUSTOMER_SCHEMA;

    assertEquals(Schema.FieldType.INT64, schema.getField("customer_id").getType());
    assertEquals(Schema.FieldType.STRING, schema.getField("name").getType());
    assertEquals(Schema.FieldType.STRING, schema.getField("email").getType());
    assertEquals(Schema.FieldType.STRING, schema.getField("signup_time").getType());
  }

  @Test
  public void testCustomerCreation() {
    Row row =
        BeamIcebergCdcExample.customer(
            1003L, "Charlie", "charlie@example.com", "2026-05-02T09:05:00Z");

    assertEquals(Long.valueOf(1003L), row.getInt64("customer_id"));
    assertEquals("Charlie", row.getString("name"));
    assertEquals("charlie@example.com", row.getString("email"));
    assertEquals("2026-05-02T09:05:00Z", row.getString("signup_time"));
  }

  @Test
  public void testIcebergCatalogConfig() {
    IcebergCatalogConfig config =
        BeamIcebergCdcExample.icebergCatalogConfig(
            "http://catalog:8181", "file:///tmp/warehouse");

    assertEquals("rest", config.getCatalogName());
    assertEquals(
        "org.apache.iceberg.rest.RESTCatalog",
        config.getCatalogProperties().get("catalog-impl"));
    assertEquals("http://catalog:8181", config.getCatalogProperties().get("uri"));
    assertEquals("file:///tmp/warehouse", config.getCatalogProperties().get("warehouse"));
    assertTrue(config.getConfigProperties().isEmpty());
  }
}
