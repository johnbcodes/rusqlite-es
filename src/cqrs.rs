use cqrs_es::persist::PersistedEventStore;
use cqrs_es::{Aggregate, CqrsFramework, Query};

use crate::{SqliteCqrs, SqliteEventRepository};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

/// A convenience method for building a simple connection pool for an SQLite database.
/// A connection pool is needed for both the event and view repositories.
///
/// ```
/// use r2d2::Pool;
/// use r2d2_sqlite::SqliteConnectionManager;
/// use sqlite_es::default_sqlite_pool;
///
/// let connection_string = "test.db";
/// let pool: Pool<SqliteConnectionManager> = default_sqlite_pool(connection_string);
/// ```
pub fn default_sqlite_pool(connection_string: &str) -> Pool<SqliteConnectionManager> {
    let manager = SqliteConnectionManager::file(connection_string)
        .with_init(|conn| conn.pragma_update(None, "journal_mode", "wal"))
        .with_init(|conn| conn.pragma_update(None, "synchronous", "normal"));
    Pool::builder()
        .max_size(1)
        .build(manager)
        .expect("unable to build pool")
}

/// A convenience function for creating a CqrsFramework from a database connection pool
/// and queries.
pub fn sqlite_cqrs<A>(
    pool: Pool<SqliteConnectionManager>,
    query_processor: Vec<Box<dyn Query<A>>>,
    services: A::Services,
) -> SqliteCqrs<A>
where
    A: Aggregate,
{
    let repo = SqliteEventRepository::new(pool);
    let store = PersistedEventStore::new_event_store(repo);
    CqrsFramework::new(store, query_processor, services)
}

/// A convenience function for creating a CqrsFramework using a snapshot store.
pub fn sqlite_snapshot_cqrs<A>(
    pool: Pool<SqliteConnectionManager>,
    query_processor: Vec<Box<dyn Query<A>>>,
    snapshot_size: usize,
    services: A::Services,
) -> SqliteCqrs<A>
where
    A: Aggregate,
{
    let repo = SqliteEventRepository::new(pool);
    let store = PersistedEventStore::new_snapshot_store(repo, snapshot_size);
    CqrsFramework::new(store, query_processor, services)
}

/// A convenience function for creating a CqrsFramework using an aggregate store.
pub fn sqlite_aggregate_cqrs<A>(
    pool: Pool<SqliteConnectionManager>,
    query_processor: Vec<Box<dyn Query<A>>>,
    services: A::Services,
) -> SqliteCqrs<A>
where
    A: Aggregate,
{
    let repo = SqliteEventRepository::new(pool);
    let store = PersistedEventStore::new_aggregate_store(repo);
    CqrsFramework::new(store, query_processor, services)
}

#[cfg(test)]
mod test {
    use crate::testing::tests::{
        TestAggregate, TestQueryRepository, TestServices, TestView, TEST_CONNECTION_STRING,
    };
    use crate::{default_sqlite_pool, sqlite_cqrs, SqliteViewRepository};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_valid_cqrs_framework() {
        let pool = default_sqlite_pool(TEST_CONNECTION_STRING);
        let repo = SqliteViewRepository::<TestView, TestAggregate>::new("test_view", pool.clone());
        let query = TestQueryRepository::new(Arc::new(repo));
        let _ps = sqlite_cqrs(pool, vec![Box::new(query)], TestServices);
    }
}
