#![allow(dead_code)]
#![cfg(feature = "deltalake")]

use deltalake::{DeltaResult, DeltaTable};
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::TableName;
use etl_destinations::deltalake::{DeltaDestinationConfig, DeltaLakeClient, DeltaLakeDestination};
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use uuid::Uuid;

/// Environment variable name for the minio endpoint URL.
const MINIO_ENDPOINT_ENV_NAME: &str = "TESTS_MINIO_ENDPOINT";
/// Environment variable name for the minio access key.
const MINIO_ACCESS_KEY_ENV_NAME: &str = "TESTS_MINIO_ACCESS_KEY";
/// Environment variable name for the minio secret key.
const MINIO_SECRET_KEY_ENV_NAME: &str = "TESTS_MINIO_SECRET_KEY";
/// Environment variable name for the minio bucket name.
const MINIO_BUCKET_ENV_NAME: &str = "TESTS_MINIO_BUCKET";

/// Default values for local development with docker-compose setup
const DEFAULT_MINIO_ENDPOINT: &str = "http://localhost:9010";
const DEFAULT_MINIO_ACCESS_KEY: &str = "minio-admin";
const DEFAULT_MINIO_SECRET_KEY: &str = "minio-admin-password";
const DEFAULT_MINIO_BUCKET: &str = "delta-dev-and-test";

/// Generates a unique warehouse path for test isolation.
///
/// Creates a random warehouse path prefixed with "etl_tests_" to ensure
/// each test run uses a fresh location and avoid conflicts.
fn random_warehouse_path() -> String {
    let uuid = Uuid::new_v4().simple().to_string();
    format!("etl_tests_{uuid}")
}

/// Delta Lake database connection for testing using minio S3-compatible storage.
///
/// Provides a unified interface for Delta Lake operations in tests, automatically
/// handling setup of test warehouse locations using minio as the object storage backend.
#[allow(unused)]
pub struct MinioDeltaLakeDatabase {
    warehouse_path: String,
    s3_base_uri: String,
    endpoint: String,
    access_key: String,
    secret_key: String,
    bucket: String,
}

#[allow(unused)]
impl MinioDeltaLakeDatabase {
    /// Creates a new Delta Lake database instance.
    ///
    /// Sets up a [`DeltaLakeDatabase`] that connects to minio S3-compatible storage
    /// using either environment variables or default values for local docker-compose setup.
    pub async fn new() -> Self {
        // Register S3 handlers for Delta Lake
        deltalake::aws::register_handlers(None);
        let endpoint = env::var(MINIO_ENDPOINT_ENV_NAME)
            .unwrap_or_else(|_| DEFAULT_MINIO_ENDPOINT.to_string());
        let access_key = env::var(MINIO_ACCESS_KEY_ENV_NAME)
            .unwrap_or_else(|_| DEFAULT_MINIO_ACCESS_KEY.to_string());
        let secret_key = env::var(MINIO_SECRET_KEY_ENV_NAME)
            .unwrap_or_else(|_| DEFAULT_MINIO_SECRET_KEY.to_string());
        let bucket =
            env::var(MINIO_BUCKET_ENV_NAME).unwrap_or_else(|_| DEFAULT_MINIO_BUCKET.to_string());

        let warehouse_path = random_warehouse_path();
        let s3_base_uri = format!("s3://{}/{}", bucket, warehouse_path);

        Self {
            warehouse_path,
            s3_base_uri,
            endpoint,
            access_key,
            secret_key,
            bucket,
        }
    }

    /// Creates a [`DeltaLakeDestination`] configured for this database instance.
    ///
    /// Returns a destination suitable for ETL operations, configured with
    /// the test warehouse location and appropriate storage options for MinIO.
    pub async fn build_destination<S>(&self, store: S) -> DeltaLakeDestination<S>
    where
        S: StateStore + SchemaStore + Send + Sync,
    {
        // Create storage options HashMap with AWS-compatible settings for MinIO
        let mut storage_options = HashMap::new();
        storage_options.insert("endpoint".to_string(), self.endpoint.clone());
        storage_options.insert("access_key_id".to_string(), self.access_key.clone());
        storage_options.insert("secret_access_key".to_string(), self.secret_key.clone());
        storage_options.insert("allow_http".to_string(), "true".to_string());
        storage_options.insert(
            "virtual_hosted_style_request".to_string(),
            "false".to_string(),
        );

        let config = DeltaDestinationConfig {
            base_uri: self.s3_base_uri.clone(),
            storage_options: Some(storage_options),
            partition_columns: None,
            optimize_after_commits: None,
        };

        DeltaLakeDestination::new(store, config)
    }

    /// Returns the S3 URI for a specific table.
    ///
    /// Generates the full S3 path where a table's Delta Lake files would be stored.
    pub fn get_table_uri(&self, table_name: &TableName) -> String {
        format!("{}/{}", self.s3_base_uri, table_name.name)
    }

    pub async fn load_table(&self, table_name: &TableName) -> DeltaResult<Arc<DeltaTable>> {
        let mut storage_options = HashMap::new();
        storage_options.insert("endpoint".to_string(), self.endpoint.clone());
        storage_options.insert("access_key_id".to_string(), self.access_key.clone());
        storage_options.insert("secret_access_key".to_string(), self.secret_key.clone());
        storage_options.insert("allow_http".to_string(), "true".to_string());
        storage_options.insert(
            "virtual_hosted_style_request".to_string(),
            "false".to_string(),
        );

        let client = DeltaLakeClient::new(Some(storage_options));
        client.open_table(&self.get_table_uri(table_name)).await
    }

    /// Returns the warehouse path for this database instance.
    pub fn warehouse_path(&self) -> &str {
        &self.warehouse_path
    }

    pub fn delete_warehouse(&self) {
        // TODO(abhi): Implement cleanup of S3 objects if needed
    }

    /// Returns the S3 base URI for this database instance.
    pub fn s3_base_uri(&self) -> &str {
        &self.s3_base_uri
    }
}

impl Drop for MinioDeltaLakeDatabase {
    /// Cleans up the test warehouse when the database instance is dropped.
    ///
    /// Note: For now, we rely on minio's lifecycle policies or manual cleanup
    /// to remove test data. In a production test environment, you might want
    /// to implement explicit cleanup here.
    fn drop(&mut self) {
        self.delete_warehouse();
    }
}

/// Sets up a Delta Lake database connection for testing.
pub async fn setup_delta_connection() -> MinioDeltaLakeDatabase {
    MinioDeltaLakeDatabase::new().await
}
