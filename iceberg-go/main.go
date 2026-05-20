package main

import (
	"context"
	"fmt"
	"io"
	"os"

	iceberg "github.com/apache/iceberg-go"
)

func exampleSchema() *iceberg.Schema {
	return iceberg.NewSchemaWithIdentifiers(
		1,
		[]int{1},
		iceberg.NestedField{
			ID:       1,
			Name:     "order_id",
			Type:     iceberg.PrimitiveTypes.Int64,
			Required: true,
			Doc:      "Stable order identifier",
		},
		iceberg.NestedField{
			ID:       2,
			Name:     "customer_id",
			Type:     iceberg.PrimitiveTypes.String,
			Required: true,
		},
		iceberg.NestedField{
			ID:       3,
			Name:     "order_date",
			Type:     iceberg.PrimitiveTypes.Date,
			Required: true,
		},
		iceberg.NestedField{
			ID:       4,
			Name:     "total_cents",
			Type:     iceberg.PrimitiveTypes.Int64,
			Required: false,
		},
	)
}

func examplePartitionSpec(schema *iceberg.Schema) (iceberg.PartitionSpec, error) {
	orderDayFieldID := iceberg.PartitionDataIDStart
	customerBucketFieldID := iceberg.PartitionDataIDStart + 1

	return iceberg.NewPartitionSpecOpts(
		iceberg.AddPartitionFieldByName(
			"order_date",
			"order_day",
			iceberg.DayTransform{},
			schema,
			&orderDayFieldID,
		),
		iceberg.AddPartitionFieldByName(
			"customer_id",
			"customer_bucket",
			iceberg.BucketTransform{NumBuckets: 16},
			schema,
			&customerBucketFieldID,
		),
	)
}

func run(w io.Writer) error {
	schema := exampleSchema()
	spec, err := examplePartitionSpec(schema)
	if err != nil {
		return err
	}

	fmt.Fprintf(w, "Iceberg Go table model\n")
	fmt.Fprintf(w, "schema id: %d\n", schema.ID)
	fmt.Fprintf(w, "identifier field ids: %v\n", schema.IdentifierFieldIDs)
	fmt.Fprintf(w, "fields:\n")
	for _, field := range schema.Fields() {
		fmt.Fprintf(w, "- %d %s %s required=%t\n", field.ID, field.Name, field.Type, field.Required)
	}

	fmt.Fprintf(w, "partition spec id: %d\n", spec.ID())
	fmt.Fprintf(w, "partition fields:\n")
	for _, field := range spec.Fields() {
		fmt.Fprintf(w, "- %d %s %s(%d)\n", field.FieldID, field.Name, field.Transform, field.SourceID())
	}

	partitionType := spec.PartitionType(schema)
	fmt.Fprintf(w, "partition type: %s\n", partitionType)

	return nil
}

func main() {
	var err error
	if len(os.Args) > 1 && os.Args[1] == "rest-catalog" {
		var cfg restCatalogConfig
		cfg, err = restCatalogConfigFromEnv(os.Getenv)
		if err == nil {
			err = runRestCatalogExample(context.Background(), os.Stdout, cfg)
		}
	} else {
		err = run(os.Stdout)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "iceberg-go example failed: %v\n", err)
		os.Exit(1)
	}
}
