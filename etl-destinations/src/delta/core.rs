use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::{Event, TableId, TableRow};
use etl::Destination;
use etl::error::EtlError;
use etl::types::{Event, TableId, TableRow};
use etl::Destination;
use etl::error::EtlError;

use crate::delta::DeltaLakeClient;

struct DeltaLakeDestination<S> {
    client: DeltaLakeClient,
    store: S,
}


impl<S> DeltaLakeDestination<S>
where
    S: StateStore + SchemaStore,
{}