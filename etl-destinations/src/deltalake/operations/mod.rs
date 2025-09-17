mod append;
mod delete;
mod merge;
mod optimize;

pub use append::append_to_table;
pub use delete::delete_from_table;
pub use merge::merge_to_table;
pub use optimize::{compact_table, zorder_table};
