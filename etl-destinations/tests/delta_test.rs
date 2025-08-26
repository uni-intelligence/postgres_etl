#![cfg(feature = "deltalake")]

use etl::config::BatchConfig;
use etl::state::table::TableReplicationPhaseType;
use etl::test_utils::database::{spawn_source_database, test_table_name};
use etl::test_utils::notify::NotifyingStore;
use etl::test_utils::pipeline::{create_pipeline, create_pipeline_with};
use etl::test_utils::test_destination_wrapper::TestDestinationWrapper;
use etl::test_utils::test_schema::{TableSelection, insert_mock_data, setup_test_database_schema};
use etl::types::{EventType, PipelineId};
use etl_telemetry::tracing::init_test_tracing;
use rand::random;

use crate::support::delta::setup_delta_connection;

#[tokio::test(flavor = "multi_thread")]
async fn table_copy_and_streaming_with_restart() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    let delta_database = setup_delta_connection().await;

    // Insert initial test data.
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        1..=2,
        false,
    )
    .await;

    let store = NotifyingStore::new();
    let raw_destination = delta_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    // Start pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // Register notifications for table copy completion.
    let users_state_notify = store
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;
    let orders_state_notify = store
        .notify_on_table_state(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // TODO: Query Delta tables directly to verify data replication
    // For now, we verify that the pipeline completed successfully

    // We restart the pipeline and check that we can process events since we have loaded the table
    // schema from the destination.
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    // We expect 2 insert events for each table (4 total).
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 4)])
        .await;

    // Insert additional data.
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        3..=4,
        false,
    )
    .await;

    event_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // TODO: Query Delta tables directly to verify final data state
}

#[tokio::test(flavor = "multi_thread")]
async fn table_insert_update_delete() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    let delta_database = setup_delta_connection().await;

    let store = NotifyingStore::new();
    let raw_destination = delta_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    // Start pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // Register notifications for table copy completion.
    let users_state_notify = store
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;

    // Wait for the first insert.
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;

    // Insert a row.
    database
        .insert_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"user_1", &1],
        )
        .await
        .unwrap();

    event_notify.notified().await;

    // TODO: Query Delta table to verify insert

    // Wait for the update.
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Update, 1)])
        .await;

    // Update the row.
    database
        .update_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"user_10", &10],
        )
        .await
        .unwrap();

    event_notify.notified().await;

    // TODO: Query Delta table to verify update

    // Wait for the delete.
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Delete, 1)])
        .await;

    // Delete the row.
    database
        .delete_values(
            database_schema.users_schema().name.clone(),
            &["name"],
            &["'user_10'"],
            "",
        )
        .await
        .unwrap();

    event_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // TODO: Query Delta table to verify deletion
}

#[tokio::test(flavor = "multi_thread")]
async fn table_subsequent_updates() {
    init_test_tracing();

    let mut database_1 = spawn_source_database().await;
    let mut database_2 = database_1.duplicate().await;
    let database_schema = setup_test_database_schema(&database_1, TableSelection::UsersOnly).await;

    let delta_database = setup_delta_connection().await;

    let store = NotifyingStore::new();
    let raw_destination = delta_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    // Start pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database_1.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // Register notifications for table copy completion.
    let users_state_notify = store
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;

    // Wait for the first insert and two updates.
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1), (EventType::Update, 2)])
        .await;

    // Insert a row.
    database_1
        .insert_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"user_1", &1],
        )
        .await
        .unwrap();

    // Create two transactions A and B on separate connections to make sure that the updates are
    // ordered correctly.
    let transaction_a = database_1.begin_transaction().await;
    transaction_a
        .update_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"user_3", &3],
        )
        .await
        .unwrap();
    transaction_a.commit_transaction().await;
    let transaction_b = database_2.begin_transaction().await;
    transaction_b
        .update_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"user_2", &2],
        )
        .await
        .unwrap();
    transaction_b.commit_transaction().await;

    event_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // TODO: Query Delta table to verify final state shows user_2 data
}

#[tokio::test(flavor = "multi_thread")]
async fn table_truncate_with_batching() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    let delta_database = setup_delta_connection().await;

    let store = NotifyingStore::new();
    let raw_destination = delta_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    // Start pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline_with(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
        // We use a batch size > 1, so that we can make sure that interleaved truncate statements
        // work well with multiple batches of events.
        Some(BatchConfig {
            max_size: 10,
            max_fill_ms: 1000,
        }),
    );

    // Register notifications for table copy completion.
    let users_state_notify = store
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;
    let orders_state_notify = store
        .notify_on_table_state(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    // Wait for the 8 inserts (4 per table + 4 after truncate) and 2 truncates (1 per table).
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 8), (EventType::Truncate, 2)])
        .await;

    // Insert 2 rows per each table.
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        1..=2,
        false,
    )
    .await;

    // We truncate both tables.
    database
        .truncate_table(database_schema.users_schema().name.clone())
        .await
        .unwrap();
    database
        .truncate_table(database_schema.orders_schema().name.clone())
        .await
        .unwrap();

    // Insert 2 extra rows per each table.
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        3..=4,
        false,
    )
    .await;

    event_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // TODO: Query Delta tables directly to verify only post-truncate data exists
}

#[tokio::test(flavor = "multi_thread")]
async fn table_creation_and_schema_evolution() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let delta_database = setup_delta_connection().await;
    let table_name = test_table_name("delta_schema_test");
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[("name", "text"), ("age", "int4"), ("active", "bool")],
        )
        .await
        .unwrap();

    let store = NotifyingStore::new();
    let raw_destination = delta_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    let publication_name = "test_pub_delta".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        store.clone(),
        destination.clone(),
    );

    let table_sync_done_notification = store
        .notify_on_table_state(table_id, TableReplicationPhaseType::SyncDone)
        .await;

    pipeline.start().await.unwrap();

    table_sync_done_notification.notified().await;

    // Insert some test data
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 2)])
        .await;

    database
        .insert_values(
            table_name.clone(),
            &["name", "age", "active"],
            &[&"Alice", &25, &true],
        )
        .await
        .unwrap();

    database
        .insert_values(
            table_name.clone(),
            &["name", "age", "active"],
            &[&"Bob", &30, &false],
        )
        .await
        .unwrap();

    event_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // TODO: Verify Delta table was created with correct schema
    // TODO: Verify data was inserted correctly
    // TODO: Test schema evolution scenarios (adding columns, etc.)
}

// Note: More comprehensive tests for different data types, arrays, etc. would be added here
// following the patterns from bigquery_test.rs once the basic Delta Lake functionality is implemented
