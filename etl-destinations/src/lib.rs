//! ETL destination implementations.
//!
//! Provides implementations of the ETL destination trait for various data warehouses
//! and analytics platforms, enabling data replication from Postgres to cloud services.

#[cfg(feature = "arrow")]
pub mod arrow;
#[cfg(feature = "bigquery")]
pub mod bigquery;
#[cfg(feature = "deltalake")]
pub mod deltalake;
#[cfg(feature = "iceberg")]
pub mod iceberg;
