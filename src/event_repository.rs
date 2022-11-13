use async_trait::async_trait;
use cqrs_es::persist::{
    PersistedEventRepository, PersistenceError, ReplayStream, SerializedEvent, SerializedSnapshot,
};
use cqrs_es::Aggregate;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{OptionalExtension, Row, Transaction, TransactionBehavior};
use serde_json::Value;

use crate::error::SqliteAggregateError;
use crate::sql_query::SqlQueryFactory;

const DEFAULT_EVENT_TABLE: &str = "events";
const DEFAULT_SNAPSHOT_TABLE: &str = "snapshots";

const DEFAULT_STREAMING_CHANNEL_SIZE: usize = 200;

/// An event repository relying on a Sqlite database for persistence.
pub struct SqliteEventRepository {
    pool: Pool<SqliteConnectionManager>,
    query_factory: SqlQueryFactory,
    stream_channel_size: usize,
}

#[async_trait]
impl PersistedEventRepository for SqliteEventRepository {
    async fn get_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        self.select_events::<A>(aggregate_id, self.query_factory.select_events())
            .await
    }

    async fn get_last_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
        last_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        let query = self.query_factory.get_last_events(last_sequence);
        self.select_events::<A>(aggregate_id, &query).await
    }

    async fn get_snapshot<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Option<SerializedSnapshot>, PersistenceError> {
        let connection = self.pool.get().map_err(SqliteAggregateError::from)?;
        let mut statement = connection
            .prepare_cached(self.query_factory.select_snapshot())
            .map_err(SqliteAggregateError::from)?;
        match statement
            .query_row((A::aggregate_type(), &aggregate_id), |row| {
                self.deser_snapshot(row)
            })
            .optional()
            .map_err(SqliteAggregateError::from)?
        {
            Some(snapshot) => Ok(Some(snapshot)),
            None => Ok(None),
        }
    }

    async fn persist<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<(String, Value, usize)>,
    ) -> Result<(), PersistenceError> {
        match snapshot_update {
            None => {
                self.insert_events::<A>(events)?;
            }
            Some((aggregate_id, aggregate, current_snapshot)) => {
                println!("Aggregate ID ({aggregate_id})  Current snapshot: {current_snapshot}");
                if current_snapshot == 1 {
                    self.insert::<A>(aggregate, aggregate_id, current_snapshot, events)?;
                } else {
                    self.update::<A>(aggregate, aggregate_id, current_snapshot, events)?;
                }
            }
        };
        Ok(())
    }

    async fn stream_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<ReplayStream, PersistenceError> {
        Ok(stream_events(
            self.query_factory.select_events().to_string(),
            A::aggregate_type(),
            aggregate_id.to_string(),
            self.pool.clone(),
            self.stream_channel_size,
        ))
    }

    // TODO: aggregate id is unused here, `stream_events` function needs to be broken up
    async fn stream_all_events<A: Aggregate>(&self) -> Result<ReplayStream, PersistenceError> {
        Ok(stream_events(
            self.query_factory.all_events().to_string(),
            A::aggregate_type(),
            "".to_string(),
            self.pool.clone(),
            self.stream_channel_size,
        ))
    }
}

fn stream_events(
    _query: String,
    _aggregate_type: String,
    _aggregate_id: String,
    _pool: Pool<SqliteConnectionManager>,
    channel_size: usize,
) -> ReplayStream {
    let (mut _feed, stream) = ReplayStream::new(channel_size);
    // tokio::spawn(async move {
    //     let connection = pool.get().unwrap();
    //     let mut statement = connection.prepare_cached(&query).unwrap();
    //     let mut rows = statement.query((&aggregate_type, &aggregate_id)).unwrap();
    //     while let Some(row) = rows.next().unwrap() {
    //         let event_result: Result<SerializedEvent, PersistenceError> =
    //             SqliteEventRepository::deser_event(row).map_err(Into::into);
    //         if feed.push(event_result).await.is_err() {
    //             // TODO: in the unlikely event of a broken channel this error should be reported.
    //             return;
    //         };
    //     }
    // });
    stream
}

impl SqliteEventRepository {
    async fn select_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
        query: &str,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        let connection = self.pool.get().map_err(SqliteAggregateError::from)?;
        let mut statement = connection
            .prepare_cached(query)
            .map_err(SqliteAggregateError::from)?;
        let mut rows = statement
            .query((A::aggregate_type(), aggregate_id))
            .map_err(SqliteAggregateError::from)?;
        let mut result: Vec<SerializedEvent> = Default::default();
        while let Some(row) = rows.next().map_err(SqliteAggregateError::from)? {
            result.push(SqliteEventRepository::deser_event(row)?);
        }
        Ok(result)
    }
}

impl SqliteEventRepository {
    /// Creates a new `SqliteEventRepository` from the provided database connection.
    /// This uses the default tables 'events' and 'snapshots'.
    ///
    /// ```
    /// use r2d2::Pool;
    /// use r2d2_sqlite::SqliteConnectionManager;
    /// use sqlite_es::SqliteEventRepository;
    ///
    /// fn configure_repo(pool: Pool<SqliteConnectionManager>) -> SqliteEventRepository {
    ///     SqliteEventRepository::new(pool)
    /// }
    /// ```
    pub fn new(pool: Pool<SqliteConnectionManager>) -> Self {
        Self::use_tables(pool, DEFAULT_EVENT_TABLE, DEFAULT_SNAPSHOT_TABLE)
    }

    /// Configures a `SqliteEventRepository` to use a streaming queue of the provided size.
    ///
    /// _Example: configure the repository to stream with a 1000 event buffer._
    /// ```
    /// use r2d2::Pool;
    /// use r2d2_sqlite::SqliteConnectionManager;
    /// use sqlite_es::SqliteEventRepository;
    ///
    /// fn configure_repo(pool: Pool<SqliteConnectionManager>) -> SqliteEventRepository {
    ///     let store = SqliteEventRepository::new(pool);
    ///     store.with_streaming_channel_size(1000)
    /// }
    /// ```
    pub fn with_streaming_channel_size(self, stream_channel_size: usize) -> Self {
        Self {
            pool: self.pool,
            query_factory: self.query_factory,
            stream_channel_size,
        }
    }

    /// Configures a `SqliteEventRepository` to use the provided table names.
    ///
    /// _Example: configure the repository to use "my_event_table" and "my_snapshot_table"
    /// for the event and snapshot table names._
    /// ```
    /// use r2d2::Pool;
    /// use r2d2_sqlite::SqliteConnectionManager;
    /// use sqlite_es::SqliteEventRepository;
    ///
    /// fn configure_repo(pool: Pool<SqliteConnectionManager>) -> SqliteEventRepository {
    ///     let store = SqliteEventRepository::new(pool);
    ///     store.with_tables("my_event_table", "my_snapshot_table")
    /// }
    /// ```
    pub fn with_tables(self, events_table: &str, snapshots_table: &str) -> Self {
        Self::use_tables(self.pool, events_table, snapshots_table)
    }

    fn use_tables(
        pool: Pool<SqliteConnectionManager>,
        events_table: &str,
        snapshots_table: &str,
    ) -> Self {
        Self {
            pool,
            query_factory: SqlQueryFactory::new(events_table, snapshots_table),
            stream_channel_size: DEFAULT_STREAMING_CHANNEL_SIZE,
        }
    }

    pub(crate) fn insert_events<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
    ) -> Result<(), SqliteAggregateError> {
        let mut connection = self.pool.get().map_err(SqliteAggregateError::from)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(SqliteAggregateError::from)?;
        self.persist_events::<A>(self.query_factory.insert_event(), &tx, events)?;
        tx.commit().map_err(SqliteAggregateError::from)?;
        Ok(())
    }

    pub(crate) fn insert<A: Aggregate>(
        &self,
        aggregate_payload: Value,
        aggregate_id: String,
        current_snapshot: usize,
        events: &[SerializedEvent],
    ) -> Result<(), SqliteAggregateError> {
        let mut connection = self.pool.get().map_err(SqliteAggregateError::from)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(SqliteAggregateError::from)?;

        let current_sequence =
            self.persist_events::<A>(self.query_factory.insert_event(), &tx, events)?;

        let mut statement = tx
            .prepare_cached(self.query_factory.insert_snapshot())
            .map_err(SqliteAggregateError::from)?;
        statement
            .execute((
                A::aggregate_type(),
                aggregate_id.as_str(),
                current_sequence as i32,
                current_snapshot as i32,
                &aggregate_payload,
            ))
            .map_err(SqliteAggregateError::from)?;
        drop(statement);

        tx.commit().map_err(SqliteAggregateError::from)?;
        Ok(())
    }

    pub(crate) fn update<A: Aggregate>(
        &self,
        aggregate_payload: Value,
        aggregate_id: String,
        current_snapshot: usize,
        events: &[SerializedEvent],
    ) -> Result<(), SqliteAggregateError> {
        let mut connection = self.pool.get().map_err(SqliteAggregateError::from)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(SqliteAggregateError::from)?;

        let current_sequence =
            self.persist_events::<A>(self.query_factory.insert_event(), &tx, events)?;
        println!("Current sequence: {current_sequence}");

        let mut statement = tx
            .prepare_cached(self.query_factory.update_snapshot())
            .map_err(SqliteAggregateError::from)?;
        let rows_affected = statement
            .execute((
                current_sequence as i32,
                &aggregate_payload,
                current_snapshot as i32,
                A::aggregate_type(),
                aggregate_id.as_str(),
                (current_snapshot - 1) as i32,
            ))
            .map_err(SqliteAggregateError::from)?;
        drop(statement);

        tx.commit().map_err(SqliteAggregateError::from)?;
        println!("Rows affected: {rows_affected}");
        match rows_affected {
            1 => Ok(()),
            _ => Err(SqliteAggregateError::OptimisticLock),
        }
    }

    fn deser_event(row: &Row) -> Result<SerializedEvent, SqliteAggregateError> {
        let aggregate_type: String = row
            .get("aggregate_type")
            .map_err(SqliteAggregateError::from)?;
        let aggregate_id: String = row
            .get("aggregate_id")
            .map_err(SqliteAggregateError::from)?;
        let sequence = {
            let s: i64 = row.get("sequence").map_err(SqliteAggregateError::from)?;
            s as usize
        };
        let event_type: String = row.get("event_type").map_err(SqliteAggregateError::from)?;
        let event_version: String = row
            .get("event_version")
            .map_err(SqliteAggregateError::from)?;
        let payload: Value = row.get("payload").map_err(SqliteAggregateError::from)?;
        let metadata: Value = row.get("metadata").map_err(SqliteAggregateError::from)?;
        Ok(SerializedEvent::new(
            aggregate_id,
            sequence,
            aggregate_type,
            event_type,
            event_version,
            payload,
            metadata,
        ))
    }

    fn deser_snapshot(&self, row: &Row) -> Result<SerializedSnapshot, rusqlite::Error> {
        let aggregate_id = row.get("aggregate_id")?;
        let s: i64 = row.get("last_sequence")?;
        let current_sequence = s as usize;
        let s: i64 = row.get("current_snapshot")?;
        let current_snapshot = s as usize;
        let aggregate: Value = row.get("payload")?;
        Ok(SerializedSnapshot {
            aggregate_id,
            aggregate,
            current_sequence,
            current_snapshot,
        })
    }

    fn persist_events<A: Aggregate>(
        &self,
        insert_event_query: &str,
        tx: &Transaction<'_>,
        events: &[SerializedEvent],
    ) -> Result<usize, SqliteAggregateError> {
        let mut current_sequence: usize = 0;
        for event in events {
            current_sequence = event.sequence;
            let payload = serde_json::to_value(&event.payload)?;
            let metadata = serde_json::to_value(&event.metadata)?;
            let mut statement = tx
                .prepare_cached(insert_event_query)
                .map_err(SqliteAggregateError::from)?;
            statement
                .execute((
                    A::aggregate_type(),
                    event.aggregate_id.as_str(),
                    event.sequence as i32,
                    &event.event_type,
                    &event.event_version,
                    &payload,
                    &metadata,
                ))
                .map_err(SqliteAggregateError::from)?;
        }
        Ok(current_sequence)
    }
}

#[cfg(test)]
mod test {
    use cqrs_es::persist::PersistedEventRepository;
    use std::fs;

    use crate::error::SqliteAggregateError;
    use crate::testing::tests::{
        snapshot_context, test_event_envelope, Created, SomethingElse, TestAggregate, TestEvent,
        Tested, TEST_CONNECTION_STRING,
    };
    use crate::{default_sqlite_pool, SqliteEventRepository};

    #[tokio::test]
    async fn event_repositories() {
        let pool = default_sqlite_pool(TEST_CONNECTION_STRING);
        let contents = fs::read_to_string("db/init.sql").unwrap();
        let conn = pool.get().unwrap();
        conn.execute_batch(contents.as_str()).unwrap();
        drop(conn);

        let id = uuid::Uuid::new_v4().to_string();
        let event_repo: SqliteEventRepository =
            SqliteEventRepository::new(pool.clone()).with_streaming_channel_size(1);
        let events = event_repo.get_events::<TestAggregate>(&id).await.unwrap();
        assert!(events.is_empty());

        event_repo
            .insert_events::<TestAggregate>(&[
                test_event_envelope(&id, 1, TestEvent::Created(Created { id: id.clone() })),
                test_event_envelope(
                    &id,
                    2,
                    TestEvent::Tested(Tested {
                        test_name: "a test was run".to_string(),
                    }),
                ),
            ])
            .unwrap();
        let events = event_repo.get_events::<TestAggregate>(&id).await.unwrap();
        assert_eq!(2, events.len());
        events.iter().for_each(|e| assert_eq!(&id, &e.aggregate_id));

        // Optimistic lock error
        let result = event_repo
            .insert_events::<TestAggregate>(&[
                test_event_envelope(
                    &id,
                    3,
                    TestEvent::SomethingElse(SomethingElse {
                        description: "this should not persist".to_string(),
                    }),
                ),
                test_event_envelope(
                    &id,
                    2,
                    TestEvent::SomethingElse(SomethingElse {
                        description: "bad sequence number".to_string(),
                    }),
                ),
            ])
            .unwrap_err();
        match result {
            SqliteAggregateError::OptimisticLock => {}
            _ => panic!("invalid error result found during insert: {}", result),
        };

        let events = event_repo.get_events::<TestAggregate>(&id).await.unwrap();
        assert_eq!(2, events.len());

        verify_replay_stream(&id, event_repo).await;
    }

    async fn verify_replay_stream(id: &str, event_repo: SqliteEventRepository) {
        let mut stream = event_repo.stream_events::<TestAggregate>(id).await.unwrap();
        let mut found_in_stream = 0;
        while (stream.next::<TestAggregate>().await).is_some() {
            found_in_stream += 1;
        }
        assert_eq!(found_in_stream, 2);

        let mut stream = event_repo
            .stream_all_events::<TestAggregate>()
            .await
            .unwrap();
        let mut found_in_stream = 0;
        while (stream.next::<TestAggregate>().await).is_some() {
            found_in_stream += 1;
        }
        assert!(found_in_stream >= 2);
    }

    #[tokio::test]
    async fn snapshot_repositories() {
        let pool = default_sqlite_pool(TEST_CONNECTION_STRING);
        let contents = fs::read_to_string("db/init.sql").unwrap();
        let conn = pool.get().unwrap();
        conn.execute_batch(contents.as_str()).unwrap();
        drop(conn);

        let id = uuid::Uuid::new_v4().to_string();
        let event_repo: SqliteEventRepository = SqliteEventRepository::new(pool.clone());
        let snapshot = event_repo.get_snapshot::<TestAggregate>(&id).await.unwrap();
        assert_eq!(None, snapshot);

        let test_description = "some test snapshot here".to_string();
        let test_tests = vec!["testA".to_string(), "testB".to_string()];
        event_repo
            .insert::<TestAggregate>(
                serde_json::to_value(TestAggregate {
                    id: id.clone(),
                    description: test_description.clone(),
                    tests: test_tests.clone(),
                })
                .unwrap(),
                id.clone(),
                1,
                &[],
            )
            .unwrap();

        let snapshot = event_repo.get_snapshot::<TestAggregate>(&id).await.unwrap();
        assert_eq!(
            Some(snapshot_context(
                id.clone(),
                0,
                1,
                serde_json::to_value(TestAggregate {
                    id: id.clone(),
                    description: test_description.clone(),
                    tests: test_tests.clone(),
                })
                .unwrap()
            )),
            snapshot
        );

        // sequence iterated, does update
        event_repo
            .update::<TestAggregate>(
                serde_json::to_value(TestAggregate {
                    id: id.clone(),
                    description: "a test description that should be saved".to_string(),
                    tests: test_tests.clone(),
                })
                .unwrap(),
                id.clone(),
                2,
                &[],
            )
            .unwrap();

        let snapshot = event_repo.get_snapshot::<TestAggregate>(&id).await.unwrap();
        assert_eq!(
            Some(snapshot_context(
                id.clone(),
                0,
                2,
                serde_json::to_value(TestAggregate {
                    id: id.clone(),
                    description: "a test description that should be saved".to_string(),
                    tests: test_tests.clone(),
                })
                .unwrap()
            )),
            snapshot
        );

        // sequence out of order or not iterated, does not update
        let result = event_repo
            .update::<TestAggregate>(
                serde_json::to_value(TestAggregate {
                    id: id.clone(),
                    description: "a test description that should not be saved".to_string(),
                    tests: test_tests.clone(),
                })
                .unwrap(),
                id.clone(),
                2,
                &[],
            )
            .unwrap_err();
        match result {
            SqliteAggregateError::OptimisticLock => {}
            _ => panic!("invalid error result found during insert: {}", result),
        };

        let snapshot = event_repo.get_snapshot::<TestAggregate>(&id).await.unwrap();
        assert_eq!(
            Some(snapshot_context(
                id.clone(),
                0,
                2,
                serde_json::to_value(TestAggregate {
                    id: id.clone(),
                    description: "a test description that should be saved".to_string(),
                    tests: test_tests.clone(),
                })
                .unwrap()
            )),
            snapshot
        );
    }
}
