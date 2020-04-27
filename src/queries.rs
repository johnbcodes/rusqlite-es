use std::fmt::Debug;
use std::marker::PhantomData;

use postgres::Connection;
use serde::de::DeserializeOwned;
use serde::Serialize;

use cqrs_es::{DomainEvent, Aggregate, AggregateError, MessageEnvelope, Query};

/// This provides a simple query repository that can be used both to return deserialized
/// views and to act as a query processor.
pub struct GenericQueryRepository<V, A, E>
    where V: Query<A, E>,
          E: DomainEvent<A>,
          A: Aggregate
{
    query_name: String,
    error_handler: Option<Box<ErrorHandler>>,
    _phantom: PhantomData<(V, A, E)>,
}

type ErrorHandler = dyn Fn(AggregateError);

impl<V, A, E> GenericQueryRepository<V, A, E>
    where V: Query<A, E>,
          E: DomainEvent<A>,
          A: Aggregate
{
    /// Creates a new `GenericQueryRepository` that will store its' views in the table named
    /// identically to the `query_name` value provided. This table should be created by the user
    /// previously (see `/db/init.sql`).
    #[must_use]
    pub fn new(query_name: String) -> Self {
        GenericQueryRepository { query_name, error_handler: None, _phantom: PhantomData }
    }
    /// Since inbound views cannot
    pub fn with_error_handler(&mut self, error_handler: Box<ErrorHandler>) {
        self.error_handler = Some(error_handler);
    }

    /// Returns the originally configured view name.
    #[must_use]
    pub fn view_name(&self) -> String {
        self.query_name.to_string()
    }


    fn load_mut(&self, conn: &Connection, aggregate_id: String) -> Result<(V, QueryContext<V>), AggregateError> {
        let query = format!("SELECT version,payload FROM {} WHERE aggregate_id= $1", &self.query_name);
        let result = match conn.query(query.as_str(), &[&aggregate_id]) {
            Ok(result) => { result }
            Err(e) => {
                return Err(AggregateError::new(e.to_string().as_str()));
            }
        };
        match result.iter().next() {
            Some(row) => {
                let view_name = self.query_name.clone();
                let version = row.get("version");
                let payload = row.get("payload");
                let view = serde_json::from_value(payload)?;
                let view_context = QueryContext {
                    query_name: view_name,
                    query_instance_id: aggregate_id,
                    version,
                    _phantom: PhantomData,
                };
                Ok((view, view_context))
            }
            None => {
                let view_context = QueryContext {
                    query_name: self.query_name.clone(),
                    query_instance_id: aggregate_id,
                    version: 0,
                    _phantom: PhantomData,
                };
                Ok((Default::default(), view_context))
            }
        }
    }

    /// Used to apply committed events to a view.
    pub fn apply_events(&self, conn: &Connection, aggregate_id: &str, events: &[MessageEnvelope<A, E>])
    {
        match self.load_mut(conn, aggregate_id.to_string()) {
            Ok((mut view, view_context)) => {
                for event in events {
                    view.update(event);
                }
                view_context.commit(conn, view);
            }
            Err(e) => {
                match &self.error_handler {
                    None => {}
                    Some(handler) => {
                        (handler)(e);
                    }
                }
            }
        };
    }

    /// Loads and deserializes a view based on the view id.
    pub fn load(&self, conn: &Connection, query_instance_id: String) -> Option<V> {
        let query = format!("SELECT version,payload FROM {} WHERE query_instance_id= $1", &self.query_name);
        let result = match conn.query(query.as_str(), &[&query_instance_id]) {
            Ok(result) => { result }
            Err(err) => {
                panic!("unable to load view '{}' with id: '{}', encountered: {}", &query_instance_id, &self.query_name, err);
            }
        };
        match result.iter().next() {
            Some(row) => {
                let payload = row.get("payload");
                match serde_json::from_value(payload) {
                    Ok(view) => Some(view),
                    Err(e) => {
                        match &self.error_handler {
                            None => {}
                            Some(handler) => {
                                (handler)(e.into());
                            }
                        }
                        None
                    }
                }
            }
            None => None,
        }
    }
}

struct QueryContext<V>
    where V: Debug + Default + Serialize + DeserializeOwned + Default
{
    query_name: String,
    query_instance_id: String,
    version: i64,
    _phantom: PhantomData<V>,
}

impl<V> QueryContext<V>
    where V: Debug + Default + Serialize + DeserializeOwned + Default
{
    fn commit(&self, conn: &Connection, view: V) {
        let sql = match self.version {
            0 => format!("INSERT INTO {} (payload, version, query_instance_id) VALUES ( $1, $2, $3 )", &self.query_name),
            _ => format!("UPDATE {} SET payload= $1 , version= $2 WHERE query_instance_id= $3", &self.query_name),
        };
        let version = self.version + 1;
        // let query_instance_id = &self.query_instance_id;
        let payload = match serde_json::to_value(&view) {
            Ok(payload) => { payload }
            Err(err) => {
                panic!("unable to covert view '{}' with id: '{}', to value: {}\n  view: {:?}", &self.query_instance_id, &self.query_name, err, &view);
            }
        };
        match conn.execute(sql.as_str(), &[&payload, &version, &self.query_instance_id]) {
            Ok(_) => {}
            Err(err) => {
                panic!("unable to update view '{}' with id: '{}', encountered: {}", &self.query_instance_id, &self.query_name, err);
            }
        };
    }
}
