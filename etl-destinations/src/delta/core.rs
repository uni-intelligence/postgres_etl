use etl::destination::Destination;
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;

use crate::delta::DeltaLakeClient;

struct DeltaLakeDestination<S> {
    client: DeltaLakeClient,
    store: S,
}

impl<S> DeltaLakeDestination<S> where S: StateStore + SchemaStore {}
