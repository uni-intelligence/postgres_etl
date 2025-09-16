# `etl` - Benchmarks

Performance benchmarks for the ETL system to measure and track replication performance across different scenarios and configurations.

## Available Benchmarks

- **table_copies**: Measures performance of initial table copying operations

## Prerequisites

Before running benchmarks, ensure you have:

- A Postgres database set up
- A publication created with the tables you want to benchmark
- For BigQuery benchmarks: GCP project, dataset, and service account key file
- For Delta Lake benchmarks: Accessible storage URI (e.g., `s3://bucket/path`) and any required object store credentials

## Quick Start

### 1. Prepare Your Environment

First, clean up any existing replication slots:

```bash
cargo bench --bench table_copies -- --log-target terminal prepare \
  --host localhost --port 5432 --database bench \
  --username postgres --password mypass
```

### 2. Run Basic Benchmark (Null Destination)

Test with fastest performance using a null destination that discards data:

```bash
cargo bench --bench table_copies -- --log-target terminal run \
  --host localhost --port 5432 --database bench \
  --username postgres --password mypass \
  --publication-name bench_pub \
  --table-ids 1,2,3 \
  --destination null
```

### 3. Run BigQuery Benchmark

Test with real BigQuery destination:

```bash
cargo bench --bench table_copies --features bigquery -- --log-target terminal run \
  --host localhost --port 5432 --database bench \
  --username postgres --password mypass \
  --publication-name bench_pub \
  --table-ids 1,2,3 \
  --destination big-query \
  --bq-project-id my-gcp-project \
  --bq-dataset-id my_dataset \
  --bq-sa-key-file /path/to/service-account-key.json
```

### 4. Run Delta Lake Benchmark

Benchmark against a Delta Lake table store:

```bash
cargo bench --bench table_copies -- --log-target terminal run \
  --host localhost --port 5432 --database bench \
  --username postgres --password mypass \
  --publication-name bench_pub \
  --table-ids 1,2,3 \
  --destination delta-lake \
  --delta-base-uri s3://my-bucket/my-warehouse \
  --delta-storage-option endpoint=http://localhost:9010 \
  --delta-storage-option access_key_id=minio \
  --delta-storage-option secret_access_key=minio-secret
```

## Command Reference

### Common Parameters

| Parameter            | Description                              | Default     |
| -------------------- | ---------------------------------------- | ----------- |
| `--host`             | Postgres host                            | `localhost` |
| `--port`             | Postgres port                            | `5432`      |
| `--database`         | Database name                            | `bench`     |
| `--username`         | Postgres username                        | `postgres`  |
| `--password`         | Postgres password                        | (optional)  |
| `--publication-name` | Publication to replicate from            | `bench_pub` |
| `--table-ids`        | Comma-separated table IDs to replicate   | (required)  |
| `--destination`      | Destination type (`null`, `big-query`, or `delta-lake`) | `null`      |

### Performance Tuning Parameters

| Parameter                  | Description                       | Default  |
| -------------------------- | --------------------------------- | -------- |
| `--batch-max-size`         | Maximum batch size                | `100000` |
| `--batch-max-fill-ms`      | Maximum batch fill time (ms)      | `10000`  |
| `--max-table-sync-workers` | Max concurrent table sync workers | `8`      |

### BigQuery Parameters

| Parameter                 | Description                   | Required for BigQuery |
| ------------------------- | ----------------------------- | --------------------- |
| `--bq-project-id`         | GCP project ID                | Yes                   |
| `--bq-dataset-id`         | BigQuery dataset ID           | Yes                   |
| `--bq-sa-key-file`        | Service account key file path | Yes                   |
| `--bq-max-staleness-mins` | Max staleness in minutes      | No                    |

### Delta Lake Parameters

| Parameter                 | Description                                      | Required for Delta Lake |
| ------------------------- | ------------------------------------------------ | ----------------------- |
| `--delta-base-uri`        | Base URI for Delta tables (e.g., `s3://bucket`)  | Yes                     |
| `--delta-storage-option`  | Extra storage option in `key=value` form. Repeat per option. | No |

### Logging Options

| Parameter               | Description                         |
| ----------------------- | ----------------------------------- |
| `--log-target terminal` | Colorized terminal output (default) |
| `--log-target file`     | Write logs to `logs/` directory     |

Set `RUST_LOG` environment variable to control log levels (default: `info`):

```bash
RUST_LOG=debug cargo bench --bench table_copies -- run ...
```

## Complete Examples

### Production-like Testing with File Logging

```bash
cargo bench --bench table_copies -- --log-target file run \
  --host localhost --port 5432 --database bench \
  --username postgres --password mypass \
  --publication-name bench_pub \
  --table-ids 1,2,3 \
  --destination null
```

### High-throughput BigQuery Test

```bash
cargo bench --bench table_copies --features bigquery -- --log-target terminal run \
  --host localhost --port 5432 --database bench \
  --username postgres --password mypass \
  --publication-name bench_pub \
  --table-ids 1,2,3,4,5 \
  --destination big-query \
  --bq-project-id my-gcp-project \
  --bq-dataset-id my_dataset \
  --bq-sa-key-file /path/to/service-account-key.json \
  --batch-max-size 50000 \
  --max-table-sync-workers 16
```

The benchmark will measure the time it takes to complete the initial table copy phase for all specified tables.

## Local Docker Environment

Start a ready-to-benchmark Postgres instance seeded with TPC-H data via Docker Compose:

```bash
cd etl-benchmarks
docker compose up postgres tpch-seeder
```

The `tpch-seeder` service builds a lightweight image (see `Dockerfile.tpch-seeder`) that bundles the [`go-tpc`](https://github.com/pingcap/go-tpc) binary and runs the TPC-H loader after Postgres becomes healthy. Adjust credentials, port mapping, scale factor, or the go-tpc version by exporting `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_PORT`, `TPCH_SCALE_FACTOR`, or `GO_TPC_VERSION` before launching Compose. Pass `--build` (or `--pull`) when changing `GO_TPC_VERSION` so Compose rebuilds the seeder image.

To add an S3-compatible target for Delta Lake benchmarking, enable the optional `minio` profile:

```bash
docker compose --profile minio up postgres tpch-seeder minio minio-setup
```

This exposes MinIO on `http://localhost:9010` (console on `http://localhost:9011`) with credentials `minio-admin` / `minio-admin-password` and creates the bucket defined by `MINIO_BUCKET` (default `delta-dev-and-test`).
