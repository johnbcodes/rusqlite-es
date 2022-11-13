use std::marker::PhantomData;

use async_trait::async_trait;
use cqrs_es::persist::{PersistenceError, ViewContext, ViewRepository};
use cqrs_es::{Aggregate, View};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::OptionalExtension;

use crate::error::SqliteAggregateError;

/// An SQLite backed query repository for use in backing a `GenericQuery`.
pub struct SqliteViewRepository<V, A> {
    insert_sql: String,
    update_sql: String,
    select_sql: String,
    pool: Pool<SqliteConnectionManager>,
    _phantom: PhantomData<(V, A)>,
}

impl<V, A> SqliteViewRepository<V, A>
where
    V: View<A>,
    A: Aggregate,
{
    /// Creates a new `SqliteViewRepository` that will store serialized views in an SQLite table
    /// named identically to the `view_name` value provided. This table should be created by the
    /// user before using this query repository (see `/db/init.sql` sql initialization file).
    ///
    /// ```
    /// # use cqrs_es::doc::MyAggregate;
    /// # use cqrs_es::persist::doc::MyView;
    /// use r2d2::Pool;
    /// use r2d2_sqlite::SqliteConnectionManager;
    /// use sqlite_es::SqliteViewRepository;
    ///
    /// fn configure_view_repo(pool: Pool<SqliteConnectionManager>) -> SqliteViewRepository<MyView,MyAggregate> {
    ///     SqliteViewRepository::new("my_view_table", pool)
    /// }
    /// ```
    pub fn new(view_name: &str, pool: Pool<SqliteConnectionManager>) -> Self {
        let insert_sql = format!(
            "INSERT INTO {} (payload, version, view_id) VALUES ( ?, ?, ? )",
            view_name
        );
        let update_sql = format!(
            "UPDATE {} SET payload= ? , version= ? WHERE view_id= ?",
            view_name
        );
        let select_sql = format!("SELECT version,payload FROM {} WHERE view_id= ?", view_name);
        Self {
            insert_sql,
            update_sql,
            select_sql,
            pool,
            _phantom: Default::default(),
        }
    }
}

#[async_trait]
impl<V, A> ViewRepository<V, A> for SqliteViewRepository<V, A>
where
    V: View<A>,
    A: Aggregate,
{
    async fn load(&self, view_id: &str) -> Result<Option<V>, PersistenceError> {
        let connection = self.pool.get().map_err(SqliteAggregateError::from)?;
        let mut statement = connection
            .prepare_cached(self.select_sql.as_str())
            .map_err(SqliteAggregateError::from)?;
        let row = statement
            .query_row([view_id], |row| {
                let payload = row.get("payload")?;
                Ok(payload)
            })
            .optional()
            .map_err(SqliteAggregateError::from)?;
        match row {
            None => Ok(None),
            Some(value) => {
                let view = serde_json::from_value(value)?;
                Ok(Some(view))
            }
        }
    }

    async fn load_with_context(
        &self,
        view_id: &str,
    ) -> Result<Option<(V, ViewContext)>, PersistenceError> {
        let connection = self.pool.get().map_err(SqliteAggregateError::from)?;
        let mut statement = connection
            .prepare_cached(self.select_sql.as_str())
            .map_err(SqliteAggregateError::from)?;
        let row = statement
            .query_row([view_id], |row| {
                let version = row.get("version")?;
                let value = row.get("payload")?;
                Ok((version, value))
            })
            .optional()
            .map_err(SqliteAggregateError::from)?;
        match row {
            None => Ok(None),
            Some((version, value)) => {
                let view = serde_json::from_value(value)?;
                let view_context = ViewContext::new(view_id.to_string(), version);
                Ok(Some((view, view_context)))
            }
        }
    }

    async fn update_view(&self, view: V, context: ViewContext) -> Result<(), PersistenceError> {
        let sql = match context.version {
            0 => &self.insert_sql,
            _ => &self.update_sql,
        };
        let connection = self.pool.get().map_err(SqliteAggregateError::from)?;
        let mut statement = connection
            .prepare_cached(sql)
            .map_err(SqliteAggregateError::from)?;

        let version = context.version + 1;
        let payload = serde_json::to_value(&view).map_err(SqliteAggregateError::from)?;
        statement
            .execute((payload, &version, context.view_instance_id))
            .map_err(SqliteAggregateError::from)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::testing::tests::{
        Created, TestAggregate, TestEvent, TestView, TEST_CONNECTION_STRING,
    };
    use crate::{default_sqlite_pool, SqliteViewRepository};
    use cqrs_es::persist::{ViewContext, ViewRepository};
    use std::fs;

    #[tokio::test]
    async fn test_valid_view_repository() {
        let pool = default_sqlite_pool(TEST_CONNECTION_STRING);
        let contents = fs::read_to_string("db/init.sql").unwrap();
        let conn = pool.get().unwrap();
        conn.execute_batch(contents.as_str()).unwrap();
        drop(conn);

        let repo = SqliteViewRepository::<TestView, TestAggregate>::new("test_view", pool.clone());
        let test_view_id = uuid::Uuid::new_v4().to_string();

        let view = TestView {
            events: vec![TestEvent::Created(Created {
                id: "just a test event for this view".to_string(),
            })],
        };
        repo.update_view(view.clone(), ViewContext::new(test_view_id.to_string(), 0))
            .await
            .unwrap();
        let (found, context) = repo
            .load_with_context(&test_view_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(found, view);
        let found = repo.load(&test_view_id).await.unwrap().unwrap();
        assert_eq!(found, view);

        let updated_view = TestView {
            events: vec![TestEvent::Created(Created {
                id: "a totally different view".to_string(),
            })],
        };
        repo.update_view(updated_view.clone(), context)
            .await
            .unwrap();
        let found_option = repo.load(&test_view_id).await.unwrap();
        let found = found_option.unwrap();

        assert_eq!(found, updated_view);
    }
}
