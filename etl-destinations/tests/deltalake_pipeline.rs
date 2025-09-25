#![cfg(feature = "deltalake")]

use deltalake::datafusion::prelude::SessionContext;
use etl::config::BatchConfig;
use etl::state::table::TableReplicationPhaseType;
use etl::test_utils::database::{spawn_source_database, test_table_name};
use etl::test_utils::notify::NotifyingStore;
use etl::test_utils::pipeline::{create_pipeline, create_pipeline_with};
use etl::test_utils::test_destination_wrapper::TestDestinationWrapper;
use etl::test_utils::test_schema::{TableSelection, insert_mock_data, setup_test_database_schema};
use etl::types::{EventType, PipelineId, ToSql};
use etl_telemetry::tracing::init_test_tracing;
use rand::random;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use etl::types::PgNumeric;
use serde_json::json;
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

use deltalake::arrow::util::pretty::pretty_format_batches;
use deltalake::{DeltaResult, DeltaTable, DeltaTableError};
use insta::assert_snapshot;

use crate::support::deltalake::setup_delta_connection;

mod support;

pub async fn snapshot_table_string(table_name: &str, table: DeltaTable) -> DeltaResult<String> {
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
    ctx.register_table(table_name, Arc::new(table))?;
    let batches = ctx
        .sql(&format!("SELECT * FROM {table_name} ORDER BY id"))
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
    ($name:expr, $table:expr) => {
        let snapshot_str = snapshot_table_string($name, $table)
            .await
            .expect("Should snapshot table");
        assert_snapshot!($name, snapshot_str, stringify!($table));
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
        Arc::new(etl_destinations::deltalake::DeltaTableConfig {
            append_only: true,
            ..Default::default()
        }),
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
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();
    users_state_notify.notified().await;

    let event_notify = destination
        .wait_for_events_count(vec![
            (EventType::Insert, 1),
            (EventType::Update, 2),
            (EventType::Delete, 1),
        ])
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

    let users_table = delta_database
        .load_table(&database_schema.users_schema().name)
        .await
        .unwrap();
    assert_table_snapshot!("append_only_ignores_updates_and_deletes", users_table);
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
        .notify_on_table_state_type(
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

    let users_table = delta_database
        .load_table(&database_schema.users_schema().name)
        .await
        .unwrap();
    assert_table_snapshot!("upsert_merge_validation", users_table);
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
        .notify_on_table_state_type(
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

    let users_table = delta_database
        .load_table(&database_schema.users_schema().name)
        .await
        .unwrap();
    assert_table_snapshot!("merge_with_delete_validation", users_table);
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
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;
    let orders_state_notify = store
        .notify_on_table_state_type(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let mut users_table = delta_database
        .load_table(&database_schema.users_schema().name)
        .await
        .unwrap();
    let mut orders_table = delta_database
        .load_table(&database_schema.orders_schema().name)
        .await
        .unwrap();

    assert_table_snapshot!(
        "table_copy_and_streaming_with_restart_users_table_1",
        users_table.clone()
    );
    assert_table_snapshot!(
        "table_copy_and_streaming_with_restart_orders_table_1",
        orders_table.clone()
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

    users_table.load().await.unwrap();
    orders_table.load().await.unwrap();

    assert_table_snapshot!(
        "table_copy_and_streaming_with_restart_users_table_2",
        users_table
    );
    assert_table_snapshot!(
        "table_copy_and_streaming_with_restart_orders_table_2",
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
        .notify_on_table_state_type(
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

    let mut users_table = delta_database
        .load_table(&database_schema.users_schema().name)
        .await
        .unwrap();

    assert_table_snapshot!("table_insert_update_delete_1_insert", users_table.clone());

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

    users_table.load().await.unwrap();

    assert_table_snapshot!("table_insert_update_delete_2_update", users_table.clone());

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

    users_table.load().await.unwrap();

    assert_table_snapshot!("table_insert_update_delete_3_delete", users_table);
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
        .notify_on_table_state_type(
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

    let users_table = delta_database
        .load_table(&database_schema.users_schema().name)
        .await
        .unwrap();

    assert_table_snapshot!("table_subsequent_updates_insert", users_table.clone());
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
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;
    let orders_state_notify = store
        .notify_on_table_state_type(
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

    let users_table = delta_database
        .load_table(&database_schema.users_schema().name)
        .await
        .unwrap();
    let orders_table = delta_database
        .load_table(&database_schema.orders_schema().name)
        .await
        .unwrap();

    assert_table_snapshot!("table_truncate_with_batching_users_table", users_table);
    assert_table_snapshot!("table_truncate_with_batching_orders_table", orders_table);
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
        .notify_on_table_state_type(table_id, TableReplicationPhaseType::SyncDone)
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

    let table = delta_database.load_table(&table_name).await.unwrap();
    assert_table_snapshot!("decimal_precision_scale_mapping", table);
}

/// Test comprehensive data type mapping from Postgres to Delta Lake
#[tokio::test(flavor = "multi_thread")]
async fn data_type_mapping() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let delta_database = setup_delta_connection().await;
    let table_name = test_table_name("comprehensive_types");

    let columns = vec![
        ("id", "bigint primary key"),
        ("bool_col", "boolean"),
        ("bpchar_col", "char(5)"),
        ("varchar_col", "varchar(255)"),
        ("name_col", "name"),
        ("text_col", "text"),
        ("int2_col", "smallint"),
        ("int4_col", "integer"),
        ("int8_col", "bigint"),
        ("float4_col", "real"),
        ("float8_col", "double precision"),
        ("numeric_col", "numeric(10,2)"),
        ("date_col", "date"),
        ("time_col", "time"),
        ("timestamp_col", "timestamp"),
        ("timestamptz_col", "timestamptz"),
        ("uuid_col", "uuid"),
        ("json_col", "json"),
        ("jsonb_col", "jsonb"),
        ("oid_col", "oid"),
        ("bytea_col", "bytea"),
        ("bool_array_col", "boolean[]"),
        ("bpchar_array_col", "char(5)[]"),
        ("varchar_array_col", "varchar(255)[]"),
        ("name_array_col", "name[]"),
        ("text_array_col", "text[]"),
        ("int2_array_col", "smallint[]"),
        ("int4_array_col", "integer[]"),
        ("int8_array_col", "bigint[]"),
        ("float4_array_col", "real[]"),
        ("float8_array_col", "double precision[]"),
        ("numeric_array_col", "numeric(10,2)[]"),
        ("date_array_col", "date[]"),
        ("time_array_col", "time[]"),
        ("timestamp_array_col", "timestamp[]"),
        ("timestamptz_array_col", "timestamptz[]"),
        ("uuid_array_col", "uuid[]"),
        ("json_array_col", "json[]"),
        ("jsonb_array_col", "jsonb[]"),
        ("oid_array_col", "oid[]"),
        ("bytea_array_col", "bytea[]"),
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
        .notify_on_table_state_type(table_id, TableReplicationPhaseType::SyncDone)
        .await;

    pipeline.start().await.unwrap();
    table_sync_done_notification.notified().await;

    // Insert test data with various types
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;

    let id_value = 1i64;
    let bool_value = true;
    let bpchar_value = "fixed".to_string();
    let varchar_value = "varchar sample".to_string();
    let name_value = "pg_name_value".to_string();
    let text_value = "text field content".to_string();
    let int2_value = 42i16;
    let int4_value = 4242i32;
    let int8_value = 4242_4242i64;
    let float4_value = 1.25f32;
    let float8_value = 9.875f64;
    let numeric_value = PgNumeric::from_str("12345.67").unwrap();
    let date_value = NaiveDate::from_ymd_opt(1993, 1, 15).unwrap();
    let time_value = NaiveTime::from_hms_micro_opt(10, 11, 12, 123_456).unwrap();
    let timestamp_value = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
        NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
    );
    let timestamptz_value = DateTime::<Utc>::from_naive_utc_and_offset(timestamp_value, Utc);
    let uuid_value = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let json_value = json!({"kind": "json"});
    let jsonb_value = json!({"kind": "jsonb"});
    let oid_value = 424_242u32;
    let bytea_value = b"Hello Delta".to_vec();

    let bool_array = vec![true, false, true];
    let bpchar_array = vec!["one".to_string(), "two".to_string()];
    let varchar_array = vec!["alpha".to_string(), "beta".to_string()];
    let name_array = vec!["first_name".to_string(), "second_name".to_string()];
    let text_array = vec!["text one".to_string(), "text two".to_string()];
    let int2_array = vec![1i16, 2i16, 3i16];
    let int4_array = vec![10i32, 20i32];
    let int8_array = vec![100i64, 200i64];
    let float4_array = vec![1.5f32, 2.5f32];
    let float8_array = vec![3.5f64, 4.5f64];
    let numeric_array = vec![
        PgNumeric::from_str("10.10").unwrap(),
        PgNumeric::from_str("20.20").unwrap(),
    ];
    let date_array = vec![
        NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
        NaiveDate::from_ymd_opt(2020, 12, 31).unwrap(),
    ];
    let time_array = vec![
        NaiveTime::from_hms_micro_opt(1, 2, 3, 0).unwrap(),
        NaiveTime::from_hms_micro_opt(4, 5, 6, 789_000).unwrap(),
    ];
    let timestamp_array = vec![
        NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2021, 3, 14).unwrap(),
            NaiveTime::from_hms_opt(1, 59, 26).unwrap(),
        ),
        NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2022, 6, 30).unwrap(),
            NaiveTime::from_hms_opt(23, 0, 0).unwrap(),
        ),
    ];
    let timestamptz_array: Vec<DateTime<Utc>> = timestamp_array
        .iter()
        .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(*dt, Utc))
        .collect();
    let uuid_array = vec![
        Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap(),
        Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap(),
    ];
    let json_array = vec![json!({"idx": 1}), json!({"idx": 2})];
    let jsonb_array = vec![json!({"code": "a"}), json!({"code": "b"})];
    let oid_array = vec![7_000u32, 7_001u32];
    let bytea_array = vec![b"bytes1".to_vec(), b"bytes2".to_vec()];

    let column_names: Vec<&str> = columns.iter().map(|(name, _)| *name).collect();
    let values: Vec<&(dyn ToSql + Sync)> = vec![
        &id_value,
        &bool_value,
        &bpchar_value,
        &varchar_value,
        &name_value,
        &text_value,
        &int2_value,
        &int4_value,
        &int8_value,
        &float4_value,
        &float8_value,
        &numeric_value,
        &date_value,
        &time_value,
        &timestamp_value,
        &timestamptz_value,
        &uuid_value,
        &json_value,
        &jsonb_value,
        &oid_value,
        &bytea_value,
        &bool_array,
        &bpchar_array,
        &varchar_array,
        &name_array,
        &text_array,
        &int2_array,
        &int4_array,
        &int8_array,
        &float4_array,
        &float8_array,
        &numeric_array,
        &date_array,
        &time_array,
        &timestamp_array,
        &timestamptz_array,
        &uuid_array,
        &json_array,
        &jsonb_array,
        &oid_array,
        &bytea_array,
    ];

    database
        .insert_values(table_name.clone(), &column_names, &values)
        .await
        .unwrap();

    event_notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let table = delta_database.load_table(&table_name).await.unwrap();
    assert_table_snapshot!("data_type_mapping", table);
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
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();
    users_state_notify.notified().await;

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

    let users_table = delta_database
        .load_table(&database_schema.users_schema().name)
        .await
        .unwrap();

    assert_table_snapshot!(
        "test_cdc_deduplication_and_conflict_resolution",
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
    let batch_size = 5;
    let mut pipeline = create_pipeline_with(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
        Some(BatchConfig {
            max_size: batch_size, // Small batch size to force multiple batches
            max_fill_ms: 1000,
        }),
    );

    let users_state_notify = store
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();
    users_state_notify.notified().await;

    // Insert many rows in a single transaction to test batching
    let insert_count: usize = 20;
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, insert_count as u64)])
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

    let users_table = delta_database
        .load_table(&database_schema.users_schema().name)
        .await
        .unwrap();
    assert_table_snapshot!("test_large_transaction_batching", users_table.clone());
    let commits = users_table.history(None).await.unwrap().collect::<Vec<_>>();
    // Due to the batch timeout, in practice, there will be more commits than the batch size.
    assert!(commits.len() >= (insert_count / batch_size));
}
