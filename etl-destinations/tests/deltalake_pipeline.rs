#![cfg(feature = "deltalake")]

use deltalake::datafusion::prelude::SessionContext;
use etl::config::BatchConfig;
use etl::state::table::TableReplicationPhaseType;
use etl::test_utils::database::{spawn_source_database, test_table_name};
use etl::test_utils::notify::NotifyingStore;
use etl::test_utils::pipeline::{create_pipeline, create_pipeline_with};
use etl::test_utils::test_destination_wrapper::TestDestinationWrapper;
use etl::test_utils::test_schema::{TableSelection, insert_mock_data, setup_test_database_schema};
use etl::types::{EventType, PipelineId, TableName};
use etl_telemetry::tracing::init_test_tracing;
use rand::random;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use etl::types::PgNumeric;
use std::str::FromStr;

use deltalake::arrow::util::pretty::pretty_format_batches;
use deltalake::{DeltaResult, DeltaTableError};
use insta::assert_snapshot;

use crate::support::deltalake::{MinioDeltaLakeDatabase, setup_delta_connection};

mod support;

pub async fn snapshot_table_string(
    database: &MinioDeltaLakeDatabase,
    table_name: &TableName,
) -> DeltaResult<String> {
    let table = database.load_table(table_name).await?;
    let snapshot = table.snapshot()?;
    let schema = snapshot.schema();

    let mut out = String::new();
    out.push_str("# Schema\n");
    for field in schema.fields() {
        out.push_str(&format!(
            "- {}: {:?} nullable={}\n",
            field.name(),
            field.data_type(),
            field.is_nullable()
        ));
    }

    out.push_str("\n# Data\n");
    let ctx = SessionContext::new();
    ctx.register_table("snapshot_table", table)?;
    let batches = ctx
        .sql("SELECT * FROM snapshot_table ORDER BY id")
        .await?
        .collect()
        .await?;
    if batches.is_empty() {
        out.push_str("<empty>\n");
    } else {
        let formatted = pretty_format_batches(&batches).map_err(DeltaTableError::generic)?;
        out.push_str(&formatted.to_string());
        out.push('\n');
    }

    Ok(out)
}

macro_rules! assert_table_snapshot {
    ($name:expr, $database:expr, $table_name:expr) => {
        let snapshot_str = snapshot_table_string($database, $table_name)
            .await
            .expect("Should snapshot table");
        assert_snapshot!($name, snapshot_str, stringify!($table_name));
    };
}

#[tokio::test(flavor = "multi_thread")]
async fn append_only_ignores_updates_and_deletes() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    let delta_database = setup_delta_connection().await;

    let store = NotifyingStore::new();

    // Configure append_only for the users table only
    let mut table_config = std::collections::HashMap::new();
    table_config.insert(
        database_schema.users_schema().name.name.clone(),
        etl_destinations::deltalake::DeltaTableConfig {
            append_only: true,
            ..Default::default()
        },
    );

    let raw_destination = delta_database
        .build_destination_with_config(store.clone(), table_config)
        .await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    let pipeline_id: PipelineId = rand::random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let users_state_notify = store
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();
    users_state_notify.notified().await;

    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1), (EventType::Update, 2), (EventType::Delete, 1)])
        .await;

    database
        .insert_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"append_user", &10],
        )
        .await
        .unwrap();

    // Perform updates that should be ignored
    database
        .update_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"append_user_v2", &20],
        )
        .await
        .unwrap();

    database
        .update_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"append_user_final", &30],
        )
        .await
        .unwrap();

    // And a delete that should be ignored
    database
        .delete_values(
            database_schema.users_schema().name.clone(),
            &["name"],
            &["'append_user_final'"],
            "",
        )
        .await
        .unwrap();

    event_notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let users_table = &database_schema.users_schema().name;
    assert_table_snapshot!(
        "append_only_ignores_updates_and_deletes",
        &delta_database,
        users_table
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn upsert_merge_validation() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    let delta_database = setup_delta_connection().await;

    let store = NotifyingStore::new();
    let raw_destination = delta_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let users_state_notify = store
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();
    users_state_notify.notified().await;

    // Expect 1 insert and 2 updates to be coalesced via merge into latest state
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1), (EventType::Update, 2)])
        .await;

    // Insert one user
    database
        .insert_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"snap_user", &10],
        )
        .await
        .unwrap();

    // Two subsequent updates to simulate upsert/merge collapsing
    database
        .update_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"snap_user_v2", &20],
        )
        .await
        .unwrap();

    database
        .update_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"snap_user_final", &30],
        )
        .await
        .unwrap();

    event_notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let users_table = &database_schema.users_schema().name;
    assert_table_snapshot!("upsert_merge_validation", &delta_database, users_table);
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_with_delete_validation() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    let delta_database = setup_delta_connection().await;

    let store = NotifyingStore::new();
    let raw_destination = delta_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let users_state_notify = store
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();
    users_state_notify.notified().await;

    // Expect 2 inserts and 1 delete
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 2), (EventType::Delete, 1)])
        .await;

    // Two rows
    database
        .insert_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"d_user_a", &11],
        )
        .await
        .unwrap();
    database
        .insert_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"d_user_b", &12],
        )
        .await
        .unwrap();

    // Delete one of them (by name)
    database
        .delete_values(
            database_schema.users_schema().name.clone(),
            &["name"],
            &["'d_user_a'"],
            "",
        )
        .await
        .unwrap();

    event_notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let users_table = &database_schema.users_schema().name;
    assert_table_snapshot!("merge_with_delete_validation", &delta_database, users_table);
}

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

    let users_table = &database_schema.users_schema().name;
    let orders_table = &database_schema.orders_schema().name;

    assert_table_snapshot!(
        "table_copy_and_streaming_with_restart_users_table_1",
        &delta_database,
        users_table
    );
    assert_table_snapshot!(
        "table_copy_and_streaming_with_restart_orders_table_1",
        &delta_database,
        orders_table
    );

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

    assert_table_snapshot!(
        "table_copy_and_streaming_with_restart_users_table_2",
        &delta_database,
        users_table
    );
    assert_table_snapshot!(
        "table_copy_and_streaming_with_restart_orders_table_2",
        &delta_database,
        orders_table
    );
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

    let users_table = &database_schema.users_schema().name;

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

    assert_table_snapshot!(
        "table_insert_update_delete_1_insert",
        &delta_database,
        users_table
    );

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

    assert_table_snapshot!(
        "table_insert_update_delete_2_update",
        &delta_database,
        users_table
    );

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

    assert_table_snapshot!(
        "table_insert_update_delete_3_delete",
        &delta_database,
        users_table
    );
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

    let users_table = &database_schema.users_schema().name;

    assert_table_snapshot!(
        "table_subsequent_updates_insert",
        &delta_database,
        users_table
    );
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

    let users_table = &database_schema.users_schema().name;
    let orders_table = &database_schema.orders_schema().name;

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

    assert_table_snapshot!(
        "table_truncate_with_batching_users_table",
        &delta_database,
        users_table
    );
    assert_table_snapshot!(
        "table_truncate_with_batching_orders_table",
        &delta_database,
        orders_table
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn decimal_precision_scale_mapping() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let delta_database = setup_delta_connection().await;
    let table_name = test_table_name("decimal_precision_test");

    let columns = vec![
        ("id", "bigint primary key"),
        ("price", "numeric(10,2)"),     // NUMERIC(10,2) -> DECIMAL(10,2)
        ("percentage", "numeric(5,4)"), // NUMERIC(5,4) -> DECIMAL(5,4)
        ("large_number", "numeric(18,6)"), // NUMERIC(18,6) -> DECIMAL(18,6)
        ("currency", "numeric(15,3)"),  // NUMERIC(15,3) -> DECIMAL(15,3)
    ];

    let table_id = database
        .create_table(table_name.clone(), false, &columns)
        .await
        .unwrap();

    let store = NotifyingStore::new();
    let raw_destination = delta_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    let publication_name = "test_pub_decimal".to_string();
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

    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 2)])
        .await;

    database
        .insert_values(
            table_name.clone(),
            &["id", "price", "percentage", "large_number", "currency"],
            &[
                &1i64,
                &PgNumeric::from_str("123.45").unwrap(), // NUMERIC(10,2)
                &PgNumeric::from_str("0.9876").unwrap(), // NUMERIC(5,4)
                &PgNumeric::from_str("1234567.123456").unwrap(), // NUMERIC(18,6)
                &PgNumeric::from_str("9999.999").unwrap(), // NUMERIC(15,3)
            ],
        )
        .await
        .unwrap();

    database
        .insert_values(
            table_name.clone(),
            &["id", "price", "percentage", "large_number", "currency"],
            &[
                &2i64,
                &PgNumeric::from_str("999.99").unwrap(), // NUMERIC(10,2)
                &PgNumeric::from_str("0.0001").unwrap(), // NUMERIC(5,4)
                &PgNumeric::from_str("999999.999999").unwrap(), // NUMERIC(18,6)
                &PgNumeric::from_str("12345.678").unwrap(), // NUMERIC(15,3)
            ],
        )
        .await
        .unwrap();

    event_notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let table_name_ref = &table_name;
    assert_table_snapshot!(
        "decimal_precision_scale_mapping",
        &delta_database,
        table_name_ref
    );
}

/// Test comprehensive data type mapping from Postgres to Delta Lake
#[tokio::test(flavor = "multi_thread")]
async fn data_type_mapping() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let delta_database = setup_delta_connection().await;
    let table_name = test_table_name("comprehensive_types");

    let columns = vec![
        ("id", "bigint primary key"), // Manually define id column without sequence
        ("name", "text"),             // TEXT -> STRING
        ("age", "int4"),              // INT4 -> INTEGER
        ("height", "float8"),         // FLOAT8 -> DOUBLE
        ("active", "bool"),           // BOOL -> BOOLEAN
        ("birth_date", "date"),       // DATE -> DATE
        ("created_at", "timestamp"),  // TIMESTAMP -> TIMESTAMP_NTZ (no timezone)
        ("updated_at", "timestamptz"), // TIMESTAMPTZ -> TIMESTAMP (with timezone)
        ("profile_data", "bytea"),    // BYTEA -> BINARY
        ("salary", "numeric(10,2)"),  // NUMERIC -> DECIMAL
    ];

    let table_id = database
        .create_table(
            table_name.clone(),
            false, // Don't create automatic BIGSERIAL id column to avoid sequence conflicts
            &columns,
        )
        .await
        .unwrap();

    let store = NotifyingStore::new();
    let raw_destination = delta_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    let publication_name = "test_pub_types".to_string();
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

    // Insert test data with various types
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;

    let birth_date = NaiveDate::from_ymd_opt(1993, 1, 15).unwrap();
    let created_at =
        NaiveDateTime::parse_from_str("2023-01-01 12:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
    let updated_at = DateTime::parse_from_rfc3339("2023-01-01T12:00:00+00:00")
        .unwrap()
        .with_timezone(&Utc);
    let profile_data = b"Hello".to_vec();

    database
        .insert_values(
            table_name.clone(),
            &[
                "id",
                "name",
                "age",
                "height",
                "active",
                "birth_date",
                "created_at",
                "updated_at",
                "profile_data",
                "salary",
            ],
            &[
                &1i64,
                &"John Doe",
                &30i32,
                &5.9f64,
                &true,
                &birth_date,
                &created_at,
                &updated_at,
                &profile_data,
                &PgNumeric::from_str("12345.6789").unwrap(),
            ],
        )
        .await
        .unwrap();

    event_notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let table_name_ref = &table_name;
    assert_table_snapshot!("data_type_mapping", &delta_database, table_name_ref);
}

/// Test CDC deduplication and conflict resolution
#[tokio::test(flavor = "multi_thread")]
async fn test_cdc_deduplication_and_conflict_resolution() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    let delta_database = setup_delta_connection().await;

    let store = NotifyingStore::new();
    let raw_destination = delta_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let users_state_notify = store
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();
    users_state_notify.notified().await;

    let users_table = &database_schema.users_schema().name;

    // Test scenario: Insert, multiple updates, and final delete for the same row
    // This tests the last-wins deduplication logic
    let event_notify = destination
        .wait_for_events_count(vec![
            (EventType::Insert, 1),
            (EventType::Update, 3),
            (EventType::Delete, 1),
        ])
        .await;

    // Insert a row
    database
        .insert_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"test_user", &20],
        )
        .await
        .unwrap();

    // Multiple rapid updates to test deduplication
    database
        .update_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"test_user_v2", &21],
        )
        .await
        .unwrap();

    database
        .update_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"test_user_v3", &22],
        )
        .await
        .unwrap();

    database
        .update_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"test_user_final", &23],
        )
        .await
        .unwrap();

    // Delete the row
    database
        .delete_values(
            database_schema.users_schema().name.clone(),
            &["name"],
            &["'test_user_final'"],
            "",
        )
        .await
        .unwrap();

    event_notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    assert_table_snapshot!(
        "test_cdc_deduplication_and_conflict_resolution",
        &delta_database,
        users_table
    );
}

/// Test large transaction handling and batching behavior
#[tokio::test(flavor = "multi_thread")]
async fn test_large_transaction_batching() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    let delta_database = setup_delta_connection().await;

    let store = NotifyingStore::new();
    let raw_destination = delta_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline_with(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
        Some(BatchConfig {
            max_size: 5, // Small batch size to force multiple batches
            max_fill_ms: 1000,
        }),
    );

    let users_state_notify = store
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();
    users_state_notify.notified().await;

    // Insert many rows in a single transaction to test batching
    let insert_count = 20;
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, insert_count)])
        .await;

    let transaction = database.begin_transaction().await;
    for i in 1..=insert_count {
        transaction
            .insert_values(
                database_schema.users_schema().name.clone(),
                &["name", "age"],
                &[&format!("batch_user_{i}"), &(20 + i as i32)],
            )
            .await
            .unwrap();
    }
    transaction.commit_transaction().await;

    event_notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let users_table = &database_schema.users_schema().name;
    assert_table_snapshot!(
        "test_large_transaction_batching",
        &delta_database,
        users_table
    );
}
