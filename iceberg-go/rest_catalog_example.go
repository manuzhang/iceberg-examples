package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	iceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
)

type restCatalogConfig struct {
	URI           string
	Warehouse     string
	Namespace     string
	TableName     string
	TableLocation string
	Token         string
	Credential    string
	Prefix        string
}

func restCatalogConfigFromEnv(getenv func(string) string) (restCatalogConfig, error) {
	cfg := restCatalogConfig{
		URI:           strings.TrimSpace(getenv("ICEBERG_REST_URI")),
		Warehouse:     strings.TrimSpace(getenv("ICEBERG_REST_WAREHOUSE")),
		Namespace:     strings.TrimSpace(getenv("ICEBERG_REST_NAMESPACE")),
		TableName:     strings.TrimSpace(getenv("ICEBERG_REST_TABLE")),
		TableLocation: strings.TrimSpace(getenv("ICEBERG_REST_TABLE_LOCATION")),
		Token:         strings.TrimSpace(getenv("ICEBERG_REST_TOKEN")),
		Credential:    strings.TrimSpace(getenv("ICEBERG_REST_CREDENTIAL")),
		Prefix:        strings.TrimSpace(getenv("ICEBERG_REST_PREFIX")),
	}
	if cfg.URI == "" {
		return cfg, errors.New("ICEBERG_REST_URI is required")
	}
	if cfg.Namespace == "" {
		cfg.Namespace = "default"
	}
	if cfg.TableName == "" {
		cfg.TableName = "go_rest_orders"
	}

	return cfg, nil
}

func runRestCatalogExample(ctx context.Context, w io.Writer, cfg restCatalogConfig) error {
	opts := []rest.Option{}
	if cfg.Warehouse != "" {
		opts = append(opts, rest.WithWarehouseLocation(cfg.Warehouse))
	}
	if cfg.Token != "" {
		opts = append(opts, rest.WithOAuthToken(cfg.Token))
	}
	if cfg.Credential != "" {
		opts = append(opts, rest.WithCredential(cfg.Credential))
	}
	if cfg.Prefix != "" {
		opts = append(opts, rest.WithPrefix(cfg.Prefix))
	}

	cat, err := rest.NewCatalog(ctx, "rest", cfg.URI, opts...)
	if err != nil {
		return err
	}

	namespace := catalog.ToIdentifier(cfg.Namespace)
	if err := cat.CreateNamespace(ctx, namespace, iceberg.Properties{}); err != nil &&
		!errors.Is(err, catalog.ErrNamespaceAlreadyExists) {
		return err
	}

	identifier := append(namespace, cfg.TableName)
	schema := exampleSchema()
	spec, err := examplePartitionSpec(schema)
	if err != nil {
		return err
	}

	createOpts := []catalog.CreateTableOpt{
		catalog.WithPartitionSpec(&spec),
		catalog.WithProperties(iceberg.Properties{
			"write.format.default": "parquet",
		}),
	}
	if cfg.TableLocation != "" {
		createOpts = append(createOpts, catalog.WithLocation(cfg.TableLocation))
	}

	tbl, err := cat.CreateTable(ctx, identifier, schema, createOpts...)
	if errors.Is(err, catalog.ErrTableAlreadyExists) {
		tbl, err = cat.LoadTable(ctx, identifier)
	}
	if err != nil {
		return err
	}

	rows, err := exampleArrowTable(schema)
	if err != nil {
		return err
	}
	defer rows.Release()

	tbl, err = tbl.AppendTable(ctx, rows, rows.NumRows(), iceberg.Properties{
		"app": "iceberg-go-rest-catalog-example",
	})
	if err != nil {
		return err
	}

	readTable, err := tbl.Scan(
		table.WithSelectedFields("order_id", "customer_id", "order_date", "total_cents"),
		table.WithLimit(20),
	).ToArrowTable(ctx)
	if err != nil {
		return err
	}
	defer readTable.Release()

	fmt.Fprintf(w, "REST catalog table: %s\n", strings.Join(identifier, "."))
	fmt.Fprintf(w, "metadata location: %s\n", tbl.MetadataLocation())
	if snapshot := tbl.CurrentSnapshot(); snapshot != nil {
		fmt.Fprintf(w, "current snapshot id: %d\n", snapshot.SnapshotID)
	}
	fmt.Fprintf(w, "appended rows: %d\n", rows.NumRows())
	fmt.Fprintf(w, "read rows: %d\n", readTable.NumRows())
	fmt.Fprintf(w, "read schema: %s\n", readTable.Schema())

	return nil
}

func exampleArrowTable(schema *iceberg.Schema) (arrow.Table, error) {
	arrowSchema, err := table.SchemaToArrowSchema(schema, nil, true, false)
	if err != nil {
		return nil, err
	}

	return array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{`[
		{"order_id": 1, "customer_id": "alice", "order_date": "2026-05-20", "total_cents": 1999},
		{"order_id": 2, "customer_id": "bob", "order_date": "2026-05-20", "total_cents": 2999},
		{"order_id": 3, "customer_id": "carol", "order_date": "2026-05-21", "total_cents": null}
	]`})
}
