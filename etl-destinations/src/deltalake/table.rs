use std::num::NonZeroU64;

use deltalake::parquet::{
    basic::Compression,
    file::properties::{WriterProperties, WriterVersion},
};

const DEFAULT_PARQUET_VERSION: WriterVersion = WriterVersion::PARQUET_1_0;
const DEFAULT_COMPRESSION: Compression = Compression::SNAPPY;
const DEFAULT_COMPACT_AFTER_COMMITS: u64 = 100;

/// Configuration for a Delta table
#[derive(Debug, Clone)]
pub struct DeltaTableConfig {
    /// Whether the table is append-only, i.e no updates or deletes are allowed
    pub append_only: bool,
    /// Parquet version to use for the table
    pub parquet_version: WriterVersion,
    /// Compression to use for the table
    pub compression: Compression,
    /// Columns to use for Z-ordering
    pub z_order_columns: Option<Vec<String>>,
    /// Run OPTIMIZE every N commits (None = disabled)
    pub compact_after_commits: Option<NonZeroU64>,
    /// Run Z-ordering every N commits (None = disabled)
    pub z_order_after_commits: Option<NonZeroU64>,
}

impl From<DeltaTableConfig> for WriterProperties {
    fn from(value: DeltaTableConfig) -> Self {
        let mut builder = WriterProperties::builder();
        builder = builder.set_writer_version(value.parquet_version);
        builder = builder.set_compression(value.compression);
        builder.build()
    }
}

impl Default for DeltaTableConfig {
    fn default() -> Self {
        Self {
            append_only: false,
            parquet_version: DEFAULT_PARQUET_VERSION,
            // good default
            compression: DEFAULT_COMPRESSION,
            z_order_columns: None,
            compact_after_commits: Some(NonZeroU64::new(DEFAULT_COMPACT_AFTER_COMMITS).unwrap()),
            z_order_after_commits: None,
        }
    }
}
