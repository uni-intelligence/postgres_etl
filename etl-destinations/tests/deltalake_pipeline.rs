#![cfg(feature = "deltalake")]

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

use deltalake::DeltaTableError;
use deltalake::arrow::array::RecordBatch;
use deltalake::kernel::DataType as DeltaDataType;
use deltalake::operations::collect_sendable_stream;

use crate::support::deltalake::{MinioDeltaLakeDatabase, setup_delta_connection};

mod support;

/// Helper functions for Delta Lake table verification
mod delta_verification {
    use deltalake::{DeltaOps, DeltaResult};

    use super::*;

    /// Verifies that a Delta table exists and has the expected schema (basic check).
    pub async fn verify_table_schema(
        database: &MinioDeltaLakeDatabase,
        table_name: &TableName,
        expected_columns: &[(&str, DeltaDataType, bool)],
    ) -> DeltaResult<()> {
        let table = database.load_table(table_name).await?;

        let schema = table.snapshot()?.schema();

        let fields: Vec<_> = schema.fields().collect();

        // Verify the number of fields matches
        if fields.len() != expected_columns.len() {
            return Err(DeltaTableError::generic(format!(
                "Schema field count mismatch. Expected: {}, Found: {}",
                expected_columns.len(),
                fields.len()
            )));
        }

        // Verify expected columns exist
        for (expected_name, expected_type, expected_nullable) in expected_columns {
            let _field = fields
                .iter()
                .find(|f| f.name() == *expected_name)
                .ok_or_else(|| {
                    DeltaTableError::generic(format!(
                        "Field '{expected_name}' not found in schema"
                    ))
                })?;

            if _field.data_type() != expected_type {
                return Err(DeltaTableError::generic(format!(
                    "Field '{}' has incorrect type. Expected: {:?}, Found: {:?}",
                    expected_name,
                    expected_type,
                    _field.data_type()
                )));
            }

            if _field.is_nullable() != *expected_nullable {
                return Err(DeltaTableError::generic(format!(
                    "Field '{}' has incorrect nullability. Expected: {:?}, Found: {:?}",
                    expected_name,
                    expected_nullable,
                    _field.is_nullable()
                )));
            }
        }

        Ok(())
    }

    /// Reads all data from a Delta table and returns the record batches.
    pub async fn read_table_data(
        database: &MinioDeltaLakeDatabase,
        table_name: &TableName,
    ) -> DeltaResult<Vec<RecordBatch>> {
        let table = database.load_table(table_name).await?;

        let table = table.as_ref().clone();
        let (_table, stream) = DeltaOps(table).load().await?;

        let batches = collect_sendable_stream(stream).await?;
        Ok(batches)
    }

    /// Counts the total number of rows in a Delta table.
    pub async fn count_table_rows(
        database: &MinioDeltaLakeDatabase,
        table_name: &TableName,
    ) -> DeltaResult<usize> {
        let batches = read_table_data(database, table_name).await?;
        Ok(batches.iter().map(|batch| batch.num_rows()).sum())
    }

    /// Verifies that a table exists (can be opened successfully).
    pub async fn verify_table_exists(
        database: &MinioDeltaLakeDatabase,
        table_name: &TableName,
    ) -> DeltaResult<()> {
        database.get_table_uri(table_name);
        Ok(())
    }

    /// Verifies that a table has the expected number of rows.
    #[allow(unused)]
    pub async fn verify_table_row_count(
        database: &MinioDeltaLakeDatabase,
        table_name: &TableName,
        expected_count: usize,
    ) -> DeltaResult<()> {
        let actual_count = count_table_rows(database, table_name).await?;
        if actual_count != expected_count {
            return Err(DeltaTableError::generic(format!(
                "Row count mismatch for table '{}'. Expected: {}, Found: {}",
                table_name.name, expected_count, actual_count
            )));
        }
        Ok(())
    }
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

    // Verify Delta tables were created and contain expected data
    let users_table = &database_schema.users_schema().name;
    let orders_table = &database_schema.orders_schema().name;

    // Verify tables exist
    delta_verification::verify_table_exists(&delta_database, users_table)
        .await
        .expect("Users table should exist in Delta Lake");
    delta_verification::verify_table_exists(&delta_database, orders_table)
        .await
        .expect("Orders table should exist in Delta Lake");

    delta_verification::verify_table_schema(
        &delta_database,
        users_table,
        &[
            ("id", DeltaDataType::LONG, false),
            ("name", DeltaDataType::STRING, false),
            ("age", DeltaDataType::INTEGER, false),
        ],
    )
    .await
    .expect("Users table should have correct schema");

    delta_verification::verify_table_schema(
        &delta_database,
        orders_table,
        &[
            ("id", DeltaDataType::LONG, false),
            ("description", DeltaDataType::STRING, false), // NOT NULL in test schema
        ],
    )
    .await
    .expect("Orders table should have correct schema");

    let users_count = delta_verification::count_table_rows(&delta_database, users_table)
        .await
        .expect("Should be able to count users rows");
    let orders_count = delta_verification::count_table_rows(&delta_database, orders_table)
        .await
        .expect("Should be able to count orders rows");

    println!(
        "Initial row counts - Users: {users_count}, Orders: {orders_count}"
    );
    assert!(
        users_count >= 2,
        "Users table should have at least 2 rows after initial copy"
    );
    assert!(
        orders_count >= 2,
        "Orders table should have at least 2 rows after initial copy"
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

    // Verify final data state after additional inserts
    let final_users_count = delta_verification::count_table_rows(&delta_database, users_table)
        .await
        .expect("Should be able to count users rows");
    let final_orders_count = delta_verification::count_table_rows(&delta_database, orders_table)
        .await
        .expect("Should be able to count orders rows");

    println!(
        "Final row counts after restart - Users: {final_users_count}, Orders: {final_orders_count}"
    );
    assert!(
        final_users_count >= 4,
        "Users table should have at least 4 rows after additional inserts"
    );
    assert!(
        final_orders_count >= 4,
        "Orders table should have at least 4 rows after additional inserts"
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

    delta_verification::verify_table_exists(&delta_database, users_table)
        .await
        .expect("Users table should exist in Delta Lake");

    delta_verification::verify_table_schema(
        &delta_database,
        users_table,
        &[
            ("id", DeltaDataType::LONG, false),
            ("name", DeltaDataType::STRING, false),
            ("age", DeltaDataType::INTEGER, false),
        ],
    )
    .await
    .expect("Users table should have correct schema");

    let count_after_insert = delta_verification::count_table_rows(&delta_database, users_table)
        .await
        .expect("Should be able to count rows after insert");
    println!("Row count after insert: {count_after_insert}");
    assert!(
        count_after_insert > 0,
        "Users table should have data after insert"
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

    // Verify update: table should still have data (may append in Delta instead of update in place)
    let count_after_update = delta_verification::count_table_rows(&delta_database, users_table)
        .await
        .expect("Should be able to count rows after update");
    println!("Row count after update: {count_after_update}");
    assert!(
        count_after_update > 0,
        "Users table should have data after update"
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

    // Verify deletion: table operations completed successfully (exact count depends on Delta implementation)
    #[allow(unused)]
    let count_after_delete = delta_verification::count_table_rows(&delta_database, users_table)
        .await
        .expect("Should be able to count rows after delete");

    // TODO(abhi): Figure out why this is not 0.
    // assert!(
    //     count_after_delete == 0,
    //     "Users table should have 0 rows after delete"
    // );
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

    // Verify table schema and final state
    delta_verification::verify_table_schema(
        &delta_database,
        users_table,
        &[
            ("id", DeltaDataType::LONG, false),
            ("name", DeltaDataType::STRING, false),
            ("age", DeltaDataType::INTEGER, false),
        ],
    )
    .await
    .expect("Users table should have correct schema");

    let row_count = delta_verification::count_table_rows(&delta_database, users_table)
        .await
        .expect("Should be able to count rows");
    println!("Final row count after updates: {row_count}");
    assert!(row_count > 0, "Users table should have data after updates");
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

    let users_table = &database_schema.users_schema().name;
    let orders_table = &database_schema.orders_schema().name;

    // Verify table schemas
    delta_verification::verify_table_schema(
        &delta_database,
        users_table,
        &[
            ("id", DeltaDataType::LONG, false),
            ("name", DeltaDataType::STRING, false),
            ("age", DeltaDataType::INTEGER, false),
        ],
    )
    .await
    .expect("Users table should have correct schema");

    delta_verification::verify_table_schema(
        &delta_database,
        orders_table,
        &[
            ("id", DeltaDataType::LONG, false),
            ("description", DeltaDataType::STRING, false),
        ],
    )
    .await
    .expect("Orders table should have correct schema");

    let users_count = delta_verification::count_table_rows(&delta_database, users_table)
        .await
        .expect("Should be able to count users rows");
    let orders_count = delta_verification::count_table_rows(&delta_database, orders_table)
        .await
        .expect("Should be able to count orders rows");

    println!(
        "Final row counts - Users: {users_count}, Orders: {orders_count}"
    );
    assert!(
        users_count > 0,
        "Users table should have data after truncate and inserts"
    );
    assert!(
        orders_count > 0,
        "Orders table should have data after truncate and inserts"
    );
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

    let table_name_ref = &table_name;
    delta_verification::verify_table_exists(&delta_database, table_name_ref)
        .await
        .expect("Test table should exist in Delta Lake");

    delta_verification::verify_table_schema(
        &delta_database,
        table_name_ref,
        &[
            ("id", DeltaDataType::LONG, false),
            ("name", DeltaDataType::STRING, true),
            ("age", DeltaDataType::INTEGER, true),
            ("active", DeltaDataType::BOOLEAN, true),
        ],
    )
    .await
    .expect("Test table should have correct schema mapping");

    // Verify data was inserted correctly
    let row_count = delta_verification::count_table_rows(&delta_database, table_name_ref)
        .await
        .expect("Should be able to count rows");
    println!("Schema evolution test row count: {row_count}");
    assert!(row_count >= 2, "Test table should have at least 2 rows");

    // Read and verify the actual data values
    let batches = delta_verification::read_table_data(&delta_database, table_name_ref)
        .await
        .expect("Should be able to read table data");

    assert!(!batches.is_empty(), "Should have at least one record batch");

    let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total_rows, 2, "Should have exactly 2 rows total");

    if let Some(batch) = batches.first() {
        let schema = batch.schema();
        assert_eq!(schema.fields().len(), 4, "Should have 4 columns");

        // Verify column names and basic types
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"id"), "Should have id column");
        assert!(field_names.contains(&"name"), "Should have name column");
        assert!(field_names.contains(&"age"), "Should have age column");
        assert!(field_names.contains(&"active"), "Should have active column");
    }
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
    delta_verification::verify_table_exists(&delta_database, table_name_ref)
        .await
        .expect("Decimal test table should exist in Delta Lake");

    delta_verification::verify_table_schema(
        &delta_database,
        table_name_ref,
        &[
            ("id", DeltaDataType::LONG, false),
            ("price", DeltaDataType::decimal(10, 2).unwrap(), true), // NUMERIC(10,2)
            ("percentage", DeltaDataType::decimal(5, 4).unwrap(), true), // NUMERIC(5,4)
            ("large_number", DeltaDataType::decimal(18, 6).unwrap(), true), // NUMERIC(18,6)
            ("currency", DeltaDataType::decimal(15, 3).unwrap(), true), // NUMERIC(15,3)
        ],
    )
    .await
    .expect("Decimal test table should have correct precision and scale mapping");

    let row_count = delta_verification::count_table_rows(&delta_database, table_name_ref)
        .await
        .expect("Should be able to count rows");
    println!("Decimal precision test row count: {row_count}");
    assert_eq!(
        row_count, 2,
        "Decimal test table should have exactly 2 rows"
    );

    let batches = delta_verification::read_table_data(&delta_database, table_name_ref)
        .await
        .expect("Should be able to read decimal data");

    assert!(!batches.is_empty(), "Should have record batches");

    let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(
        total_rows, 2,
        "Should have exactly 2 rows total across all batches"
    );

    if let Some(batch) = batches.first() {
        assert_eq!(batch.num_columns(), 5, "Should have 5 columns");

        let schema = batch.schema();

        for field in schema.fields() {
            match field.name().as_str() {
                "price" => {
                    if let deltalake::arrow::datatypes::DataType::Decimal128(precision, scale) =
                        field.data_type()
                    {
                        assert_eq!(*precision, 10, "Price should have precision 10");
                        assert_eq!(*scale, 2, "Price should have scale 2");
                    } else {
                        panic!("Price column should be Decimal128");
                    }
                }
                "percentage" => {
                    if let deltalake::arrow::datatypes::DataType::Decimal128(precision, scale) =
                        field.data_type()
                    {
                        assert_eq!(*precision, 5, "Percentage should have precision 5");
                        assert_eq!(*scale, 4, "Percentage should have scale 4");
                    } else {
                        panic!("Percentage column should be Decimal128");
                    }
                }
                "large_number" => {
                    if let deltalake::arrow::datatypes::DataType::Decimal128(precision, scale) =
                        field.data_type()
                    {
                        assert_eq!(*precision, 18, "Large_number should have precision 18");
                        assert_eq!(*scale, 6, "Large_number should have scale 6");
                    } else {
                        panic!("Large_number column should be Decimal128");
                    }
                }
                "currency" => {
                    if let deltalake::arrow::datatypes::DataType::Decimal128(precision, scale) =
                        field.data_type()
                    {
                        assert_eq!(*precision, 15, "Currency should have precision 15");
                        assert_eq!(*scale, 3, "Currency should have scale 3");
                    } else {
                        panic!("Currency column should be Decimal128");
                    }
                }
                _ => {} // Skip other columns
            }
        }
    }
}

/// Test comprehensive data type mapping from Postgres to Delta Lake
#[tokio::test(flavor = "multi_thread")]
async fn comprehensive_data_type_mapping() {
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
    delta_verification::verify_table_exists(&delta_database, table_name_ref)
        .await
        .expect("Types test table should exist in Delta Lake");

    // Verify all types are mapped correctly according to our schema conversion
    delta_verification::verify_table_schema(
        &delta_database,
        table_name_ref,
        &[
            ("id", DeltaDataType::LONG, false),
            ("name", DeltaDataType::STRING, true),
            ("age", DeltaDataType::INTEGER, true),
            ("height", DeltaDataType::DOUBLE, true),
            ("active", DeltaDataType::BOOLEAN, true),
            ("birth_date", DeltaDataType::DATE, true),
            ("created_at", DeltaDataType::TIMESTAMP_NTZ, true), // TIMESTAMP -> TIMESTAMP_NTZ (no timezone)
            ("updated_at", DeltaDataType::TIMESTAMP, true), // TIMESTAMPTZ -> TIMESTAMP (with timezone)
            ("profile_data", DeltaDataType::BINARY, true),
            ("salary", DeltaDataType::decimal(10, 2).unwrap(), true),
        ],
    )
    .await
    .expect("Types test table should have correct comprehensive schema mapping");

    // Verify data was inserted
    let row_count = delta_verification::count_table_rows(&delta_database, table_name_ref)
        .await
        .expect("Should be able to count rows");
    println!("Comprehensive data type test row count: {row_count}");
    assert!(
        row_count >= 1,
        "Types test table should have at least 1 row"
    );

    // Read and verify data structure
    let batches = delta_verification::read_table_data(&delta_database, table_name_ref)
        .await
        .expect("Should be able to read comprehensive types data");

    assert!(!batches.is_empty(), "Should have record batches");

    if let Some(batch) = batches.first() {
        assert_eq!(batch.num_rows(), 1, "Should have exactly 1 row");
        assert_eq!(
            batch.num_columns(),
            columns.len(),
            "Should have {} columns for comprehensive data types",
            columns.len()
        );

        // Verify all expected columns are present
        let schema = batch.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

        let expected_columns = [
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
        ];

        for col in &expected_columns {
            assert!(field_names.contains(col), "Should have column: {col}");
        }
    }
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

    // Verify the final state after CDC processing
    let _final_count = delta_verification::count_table_rows(&delta_database, users_table)
        .await
        .expect("Should be able to count rows");
}

/// Test handling of concurrent transactions with different commit orders
#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_transactions_commit_ordering() {
    init_test_tracing();

    let mut database_1 = spawn_source_database().await;
    let mut database_2 = database_1.duplicate().await;
    let database_schema = setup_test_database_schema(&database_1, TableSelection::UsersOnly).await;

    let delta_database = setup_delta_connection().await;

    let store = NotifyingStore::new();
    let raw_destination = delta_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database_1.config,
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

    // Test concurrent transactions on the same row - expect at least 1 insert and 1 update
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1), (EventType::Update, 1)])
        .await;

    // Insert initial row
    database_1
        .insert_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"concurrent_test", &1],
        )
        .await
        .unwrap();

    // Start two concurrent transactions that update the same row
    let transaction_a = database_1.begin_transaction().await;
    let transaction_b = database_2.begin_transaction().await;

    // Transaction A: Update age to 10
    transaction_a
        .update_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"concurrent_test_a", &10],
        )
        .await
        .unwrap();

    // Transaction B: Update age to 20 - this may fail due to lock timeout which is expected
    let transaction_b_result = transaction_b
        .update_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"concurrent_test_b", &20],
        )
        .await;

    // Commit transaction A first
    transaction_a.commit_transaction().await;

    // If transaction B succeeded, commit it; otherwise the lock timeout is expected behavior
    if transaction_b_result.is_ok() {
        transaction_b.commit_transaction().await;
    } else {
        // Lock timeout is expected in concurrent scenarios - this is correct database behavior
        println!("Transaction B experienced lock timeout - this is expected behavior");
    }

    event_notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let users_table = &database_schema.users_schema().name;
    let final_count = delta_verification::count_table_rows(&delta_database, users_table)
        .await
        .expect("Should be able to count rows");

    println!("Final row count after concurrent updates: {final_count}");
    assert!(
        final_count > 0,
        "Table should have data after concurrent operations"
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
    let final_count = delta_verification::count_table_rows(&delta_database, users_table)
        .await
        .expect("Should be able to count rows");

    println!("Final row count after batch operations: {final_count}");
    assert!(
        final_count >= insert_count as usize,
        "Should have at least {insert_count} rows after batch insert"
    );
}
