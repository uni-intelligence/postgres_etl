use std::sync::Arc;

use super::schema::postgres_to_delta_schema;
use deltalake::{DeltaOps, DeltaResult, DeltaTable, open_table};
use etl::types::TableSchema;

/// Client for connecting to Delta Lake tables.
#[derive(Clone)]
pub struct DeltaLakeClient {}

impl DeltaLakeClient {
    /// Create a new client.
    pub fn new() -> Self {
        Self {}
    }

    /// Returns true if a Delta table exists at the given uri/path.
    pub async fn table_exists(&self, table_uri: &str) -> bool {
        open_table(table_uri).await.is_ok()
    }

    /// Create a Delta table at `table_uri` if it doesn't exist, using the provided Postgres schema.
    pub async fn create_table_if_missing(
        &self,
        table_uri: &str,
        table_schema: &TableSchema,
    ) -> DeltaResult<Arc<DeltaTable>> {
        if let Ok(table) = open_table(table_uri).await {
            return Ok(Arc::new(table));
        }

        let delta_schema = postgres_to_delta_schema(table_schema)?;

        let ops = DeltaOps::try_from_uri(table_uri).await?;
        let table = ops
            .create()
            // TODO(abhi): Figure out how to avoid the clone
            .with_columns(delta_schema.fields().map(|field| field.clone()))
            .await?;

        Ok(Arc::new(table))
    }

    /// Open a Delta table at `table_uri`.
    pub async fn open_table(&self, table_uri: &str) -> DeltaResult<Arc<DeltaTable>> {
        let table = open_table(table_uri).await?;
        Ok(Arc::new(table))
    }
}
