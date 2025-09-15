#!/usr/bin/env bash
set -eo pipefail

# Table Copies Benchmark Script
#
# This script runs the table copies benchmark using hyperfine with configurable destinations.
#
# Supported destinations:
# - null: Discards all data (fastest, default)
# - big-query: Streams data to Google BigQuery
# - delta-lake: Writes to a Delta Lake table store (delta-rs)
#
# Environment Variables:
#   Database Configuration:
#     POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_PORT, POSTGRES_HOST
#   
#   Benchmark Configuration:
#     HYPERFINE_RUNS, PUBLICATION_NAME, BATCH_MAX_SIZE, BATCH_MAX_FILL_MS, MAX_TABLE_SYNC_WORKERS
#     DESTINATION (null, big-query, or delta-lake)
#     LOG_TARGET (terminal or file) - Where to send logs (default: terminal)
#     DRY_RUN (true/false) - Show commands without executing them
#   
#   BigQuery Configuration (required when DESTINATION=big-query):
#     BQ_PROJECT_ID - Google Cloud project ID
#     BQ_DATASET_ID - BigQuery dataset ID
#     BQ_SA_KEY_FILE - Path to service account key JSON file
#     BQ_MAX_STALENESS_MINS - Optional staleness setting
#     BQ_MAX_CONCURRENT_STREAMS - Optional concurrent stream hint
#
#   Delta Lake Configuration (required when DESTINATION=delta-lake):
#     DELTA_BASE_URI - Base URI for the Delta Lake tables, e.g. s3://bucket/path
#     DELTA_STORAGE_OPTIONS - Optional comma-separated key=value pairs for object store options
#
# Examples:
#   # Run with null destination and terminal logs (default)
#   ./etl-benchmarks/scripts/benchmark.sh
#
#   # Run with file logging
#   LOG_TARGET=file ./etl-benchmarks/scripts/benchmark.sh
#
#   # Dry run to see commands that would be executed
#   DRY_RUN=true ./etl-benchmarks/scripts/benchmark.sh
#
#   # Run with BigQuery destination
#   DESTINATION=big-query \
#   BQ_PROJECT_ID=my-project \
#   BQ_DATASET_ID=my_dataset \
#   BQ_SA_KEY_FILE=/path/to/sa-key.json \
#   ./etl-benchmarks/scripts/benchmark.sh
#
#   # Run with Delta Lake destination (MinIO example)
#   DESTINATION=delta-lake \
#   DELTA_BASE_URI=s3://delta-dev-and-test/bench \
#   DELTA_STORAGE_OPTIONS="endpoint=http://localhost:9010,access_key_id=minio,secret_access_key=minio-password,allow_http=true" \
#   ./etl-benchmarks/scripts/benchmark.sh

# Check if hyperfine is installed
if ! [ -x "$(command -v hyperfine)" ]; then
  echo >&2 "❌ Error: hyperfine is not installed."
  echo >&2 "Please install it first. You can find installation instructions at:"
  echo >&2 "    https://github.com/sharkdp/hyperfine"
  exit 1
fi

# Check if psql is installed
if ! [ -x "$(command -v psql)" ]; then
  echo >&2 "❌ Error: Postgres client (psql) is not installed."
  echo >&2 "Please install it using your system's package manager."
  exit 1
fi

# Database configuration with defaults (matching prepare_tpcc.sh)
DB_USER="${POSTGRES_USER:=postgres}"
DB_PASSWORD="${POSTGRES_PASSWORD:=postgres}"
DB_NAME="${POSTGRES_DB:=bench}"
DB_PORT="${POSTGRES_PORT:=5430}"
DB_HOST="${POSTGRES_HOST:=localhost}"

# Benchmark configuration
RUNS="${HYPERFINE_RUNS:=1}"
PUBLICATION_NAME="${PUBLICATION_NAME:=bench_pub}"
BATCH_MAX_SIZE="${BATCH_MAX_SIZE:=1000000}"
BATCH_MAX_FILL_MS="${BATCH_MAX_FILL_MS:=10000}"
MAX_TABLE_SYNC_WORKERS="${MAX_TABLE_SYNC_WORKERS:=8}"

# Logging configuration
LOG_TARGET="${LOG_TARGET:=terminal}"  # terminal or file

# Destination configuration
DESTINATION="${DESTINATION:=null}"  # null, big-query, or delta-lake
BQ_PROJECT_ID="${BQ_PROJECT_ID:=}"
BQ_DATASET_ID="${BQ_DATASET_ID:=}"
BQ_SA_KEY_FILE="${BQ_SA_KEY_FILE:=}"
BQ_MAX_STALENESS_MINS="${BQ_MAX_STALENESS_MINS:=}"
BQ_MAX_CONCURRENT_STREAMS="${BQ_MAX_CONCURRENT_STREAMS:=}"
DELTA_BASE_URI="${DELTA_BASE_URI:=}"
DELTA_STORAGE_OPTIONS="${DELTA_STORAGE_OPTIONS:=}"

IFS=',' read -r -a DELTA_STORAGE_OPTIONS_ARRAY <<< "${DELTA_STORAGE_OPTIONS}"

# Optional dry-run mode
DRY_RUN="${DRY_RUN:=false}"

echo "🏁 Running table copies benchmark with hyperfine..."
echo "📊 Configuration:"
echo "   Database: ${DB_NAME}@${DB_HOST}:${DB_PORT}"
echo "   User: ${DB_USER}"
echo "   Runs: ${RUNS}"
echo "   Publication: ${PUBLICATION_NAME}"
echo "   Batch size: ${BATCH_MAX_SIZE}"
echo "   Batch fill time: ${BATCH_MAX_FILL_MS}ms"
echo "   Workers: ${MAX_TABLE_SYNC_WORKERS}"
echo "   Log target: ${LOG_TARGET}"
echo "   Destination: ${DESTINATION}"
if [[ "${DESTINATION}" == "big-query" ]]; then
  echo "   BigQuery Project: ${BQ_PROJECT_ID}"
  echo "   BigQuery Dataset: ${BQ_DATASET_ID}"
  echo "   BigQuery SA Key: ${BQ_SA_KEY_FILE}"
  if [[ -n "${BQ_MAX_STALENESS_MINS}" ]]; then
    echo "   BigQuery Max Staleness: ${BQ_MAX_STALENESS_MINS} mins"
  fi
  if [[ -n "${BQ_MAX_CONCURRENT_STREAMS}" ]]; then
    echo "   BigQuery Max Concurrent Streams: ${BQ_MAX_CONCURRENT_STREAMS}"
  fi
elif [[ "${DESTINATION}" == "delta-lake" ]]; then
  echo "   Delta Lake Base URI: ${DELTA_BASE_URI}"
  if [[ -n "${DELTA_STORAGE_OPTIONS}" ]]; then
    echo "   Delta Lake Storage Options: ${DELTA_STORAGE_OPTIONS}"
  fi
fi

# Get table IDs from the database for TPC-C tables
echo "🔍 Querying table IDs from database..."
TPCC_TABLE_IDS=$(PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -U "${DB_USER}" -p "${DB_PORT}" -d "${DB_NAME}" -tAc "
  select string_agg(oid::text, ',')
  from pg_class
  where relname in ('customer', 'district', 'item', 'new_order', 'order_line', 'orders', 'stock', 'warehouse')
    and relkind = 'r';
" 2>/dev/null || echo "")

if [[ -z "${TPCC_TABLE_IDS}" ]]; then
  echo "❌ Error: Could not retrieve table IDs from database. Make sure TPC-C tables exist."
  echo "💡 Run './etl-benchmarks/scripts/prepare_tpcc.sh' first to create the tables."
  exit 1
fi

echo "✅ Found table IDs: ${TPCC_TABLE_IDS}"

# Validate BigQuery configuration if using BigQuery destination
if [[ "${DESTINATION}" == "big-query" ]]; then
  if [[ -z "${BQ_PROJECT_ID}" ]]; then
    echo "❌ Error: BQ_PROJECT_ID environment variable is required when using BigQuery destination."
    exit 1
  fi
  if [[ -z "${BQ_DATASET_ID}" ]]; then
    echo "❌ Error: BQ_DATASET_ID environment variable is required when using BigQuery destination."
    exit 1
  fi
  if [[ -z "${BQ_SA_KEY_FILE}" ]]; then
    echo "❌ Error: BQ_SA_KEY_FILE environment variable is required when using BigQuery destination."
    exit 1
  fi
  if [[ ! -f "${BQ_SA_KEY_FILE}" ]]; then
    echo "❌ Error: BigQuery service account key file does not exist: ${BQ_SA_KEY_FILE}"
    exit 1
  fi
elif [[ "${DESTINATION}" == "delta-lake" ]]; then
  if [[ -z "${DELTA_BASE_URI}" ]]; then
    echo "❌ Error: DELTA_BASE_URI environment variable is required when using Delta Lake destination."
    exit 1
  fi
  for opt in "${DELTA_STORAGE_OPTIONS_ARRAY[@]}"; do
    [[ -z "${opt}" ]] && continue
    if [[ "${opt}" != *=* ]]; then
      echo "❌ Error: Invalid DELTA_STORAGE_OPTIONS entry '${opt}'. Expected key=value format."
      exit 1
    fi
  done
fi

# Determine if we need BigQuery features
FEATURES_FLAG=""
case "${DESTINATION}" in
  big-query)
    FEATURES_FLAG="--features bigquery"
    ;;
  delta-lake)
    FEATURES_FLAG="--features deltalake"
    ;;
esac

# Validate destination option
if [[ "${DESTINATION}" != "null" && "${DESTINATION}" != "big-query" && "${DESTINATION}" != "delta-lake" ]]; then
  echo "❌ Error: Invalid destination '${DESTINATION}'. Supported values: null, big-query, delta-lake"
  exit 1
fi

# Validate log target option
if [[ "${LOG_TARGET}" != "terminal" && "${LOG_TARGET}" != "file" ]]; then
  echo "❌ Error: Invalid log target '${LOG_TARGET}'. Supported values: terminal, file"
  exit 1
fi

# Build the prepare command
PREPARE_CMD="cargo bench --bench table_copies ${FEATURES_FLAG} -- --log-target ${LOG_TARGET} prepare --host ${DB_HOST} --port ${DB_PORT} --database ${DB_NAME} --username ${DB_USER}"
if [[ -n "${DB_PASSWORD}" && "${DB_PASSWORD}" != "" ]]; then
  PREPARE_CMD="${PREPARE_CMD} --password ${DB_PASSWORD}"
fi

# Build the run command
RUN_CMD="cargo bench --bench table_copies ${FEATURES_FLAG} -- --log-target ${LOG_TARGET} run --host ${DB_HOST} --port ${DB_PORT} --database ${DB_NAME} --username ${DB_USER}"
if [[ -n "${DB_PASSWORD}" && "${DB_PASSWORD}" != "" ]]; then
  RUN_CMD="${RUN_CMD} --password ${DB_PASSWORD}"
fi
RUN_CMD="${RUN_CMD} --publication-name ${PUBLICATION_NAME} --batch-max-size ${BATCH_MAX_SIZE} --batch-max-fill-ms ${BATCH_MAX_FILL_MS} --max-table-sync-workers ${MAX_TABLE_SYNC_WORKERS} --table-ids ${TPCC_TABLE_IDS}"

# Add destination-specific options
RUN_CMD="${RUN_CMD} --destination ${DESTINATION}"
if [[ "${DESTINATION}" == "big-query" ]]; then
  RUN_CMD="${RUN_CMD} --bq-project-id ${BQ_PROJECT_ID}"
  RUN_CMD="${RUN_CMD} --bq-dataset-id ${BQ_DATASET_ID}"
  RUN_CMD="${RUN_CMD} --bq-sa-key-file ${BQ_SA_KEY_FILE}"
  if [[ -n "${BQ_MAX_STALENESS_MINS}" ]]; then
    RUN_CMD="${RUN_CMD} --bq-max-staleness-mins ${BQ_MAX_STALENESS_MINS}"
  fi
  if [[ -n "${BQ_MAX_CONCURRENT_STREAMS}" ]]; then
    RUN_CMD="${RUN_CMD} --bq-max-concurrent-streams ${BQ_MAX_CONCURRENT_STREAMS}"
  fi
elif [[ "${DESTINATION}" == "delta-lake" ]]; then
  RUN_CMD="${RUN_CMD} --delta-base-uri ${DELTA_BASE_URI}"
  for opt in "${DELTA_STORAGE_OPTIONS_ARRAY[@]}"; do
    [[ -z "${opt}" ]] && continue
    RUN_CMD="${RUN_CMD} --delta-storage-option ${opt}"
  done
fi

echo ""
echo "🚀 Starting benchmark..."

# Show commands in dry-run mode
if [[ "${DRY_RUN}" == "true" ]]; then
  echo ""
  echo "📝 Commands that would be executed:"
  echo "   Prepare: ${PREPARE_CMD}"
  echo "   Run:     ${RUN_CMD}"
  echo ""
  echo "ℹ️  This was a dry run. Set DRY_RUN=false to actually run the benchmark."
  exit 0
fi

# Run hyperfine
hyperfine \
  --runs "${RUNS}" \
  --show-output \
  --prepare "${PREPARE_CMD}" \
  "${RUN_CMD}"

echo "✨ Benchmark complete!"
