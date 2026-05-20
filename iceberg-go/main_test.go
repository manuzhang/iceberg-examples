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
	if field := spec.Field(0); field.SourceID != 3 || field.FieldID != 1000 {
		t.Fatalf("first partition field = %+v, want source id 3 and field id 1000", field)
	}
	if field := spec.Field(1); field.SourceID != 2 || field.FieldID != 1001 {
		t.Fatalf("second partition field = %+v, want source id 2 and field id 1001", field)
	}
}
