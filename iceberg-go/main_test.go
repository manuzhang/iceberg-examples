package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunPrintsSchemaAndPartitionSpec(t *testing.T) {
	var out bytes.Buffer
	if err := run(&out); err != nil {
		t.Fatalf("run() returned error: %v", err)
	}

	got := out.String()
	for _, want := range []string{
		"Iceberg Go table model",
		"schema id: 1",
		"- 1 order_id long required=true",
		"- 3 order_date date required=true",
		"partition spec id: 0",
		"- 1000 order_day day(3)",
		"- 1001 customer_bucket bucket[16](2)",
		"partition type: struct<1000: order_day: optional int, 1001: customer_bucket: optional int>",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q\nfull output:\n%s", want, got)
		}
	}
}

func TestPartitionSpecBindsToSchema(t *testing.T) {
	schema := exampleSchema()
	spec, err := examplePartitionSpec(schema)
	if err != nil {
		t.Fatalf("examplePartitionSpec() returned error: %v", err)
	}

	if spec.NumFields() != 2 {
		t.Fatalf("spec.NumFields() = %d, want 2", spec.NumFields())
	}
	if field := spec.Field(0); field.SourceID() != 3 || field.FieldID != 1000 {
		t.Fatalf("first partition field = %+v, want source id 3 and field id 1000", field)
	}
	if field := spec.Field(1); field.SourceID() != 2 || field.FieldID != 1001 {
		t.Fatalf("second partition field = %+v, want source id 2 and field id 1001", field)
	}
}

func TestRestCatalogConfigFromEnv(t *testing.T) {
	env := map[string]string{
		"ICEBERG_REST_URI":            "http://localhost:8181",
		"ICEBERG_REST_WAREHOUSE":      "file:///tmp/warehouse",
		"ICEBERG_REST_NAMESPACE":      "examples",
		"ICEBERG_REST_TABLE":          "orders",
		"ICEBERG_REST_TABLE_LOCATION": "file:///tmp/warehouse/examples/orders",
		"ICEBERG_REST_TOKEN":          "token",
		"ICEBERG_REST_CREDENTIAL":     "client:secret",
		"ICEBERG_REST_PREFIX":         "prod",
	}

	cfg, err := restCatalogConfigFromEnv(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("restCatalogConfigFromEnv() returned error: %v", err)
	}

	if cfg.URI != "http://localhost:8181" ||
		cfg.Warehouse != "file:///tmp/warehouse" ||
		cfg.Namespace != "examples" ||
		cfg.TableName != "orders" ||
		cfg.TableLocation != "file:///tmp/warehouse/examples/orders" ||
		cfg.Token != "token" ||
		cfg.Credential != "client:secret" ||
		cfg.Prefix != "prod" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestRestCatalogConfigDefaults(t *testing.T) {
	cfg, err := restCatalogConfigFromEnv(func(key string) string {
		if key == "ICEBERG_REST_URI" {
			return "http://localhost:8181"
		}
		return ""
	})
	if err != nil {
		t.Fatalf("restCatalogConfigFromEnv() returned error: %v", err)
	}
	if cfg.Namespace != "default" {
		t.Fatalf("cfg.Namespace = %q, want default", cfg.Namespace)
	}
	if cfg.TableName != "go_rest_orders" {
		t.Fatalf("cfg.TableName = %q, want go_rest_orders", cfg.TableName)
	}
}

func TestRestCatalogConfigRequiresURI(t *testing.T) {
	if _, err := restCatalogConfigFromEnv(func(string) string { return "" }); err == nil {
		t.Fatal("restCatalogConfigFromEnv() returned nil error, want required URI error")
	}
}

func TestExampleArrowTable(t *testing.T) {
	rows, err := exampleArrowTable(exampleSchema())
	if err != nil {
		t.Fatalf("exampleArrowTable() returned error: %v", err)
	}
	defer rows.Release()

	if rows.NumRows() != 3 {
		t.Fatalf("rows.NumRows() = %d, want 3", rows.NumRows())
	}
	if rows.Schema().NumFields() != 4 {
		t.Fatalf("rows.Schema().NumFields() = %d, want 4", rows.Schema().NumFields())
	}
}
