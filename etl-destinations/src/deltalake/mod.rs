mod config;
mod core;
pub(crate) mod events;
pub(crate) mod expr;
mod operations;
mod schema;

pub use core::{DeltaDestinationConfig, DeltaLakeDestination};
pub use schema::TableRowEncoder;
