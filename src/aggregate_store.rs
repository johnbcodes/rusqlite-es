use std::collections::HashMap;
use std::marker::PhantomData;

use async_trait::async_trait;
use cqrs_es::{Aggregate, AggregateContext, AggregateError, EventEnvelope, EventStore};

use crate::{EventRepository, SnapshotRepository};

/// Storage engine using an Postgres backing and relying on a serialization of the aggregate rather
/// than individual events. This is similar to the "snapshot strategy" seen in many CQRS
/// frameworks.
pub struct PostgresSnapshotStore<A: Aggregate> {
    repo: SnapshotRepository<A>,
    event_repo: EventRepository<A>,
    _phantom: PhantomData<A>,
}

impl<A: Aggregate> PostgresSnapshotStore<A> {
    /// Creates a new `PostgresSnapshotStore` from the provided database connection.
    pub fn new(repo: SnapshotRepository<A>, event_repo: EventRepository<A>) -> Self {
        PostgresSnapshotStore {
            repo,
            event_repo,
            _phantom: PhantomData,
        }
    }
    fn peek_at_last_sequence(events: &Vec<EventEnvelope<A>>) -> usize {
        match events.get(events.len() - 1) {
            None => 0,
            Some(event) => event.sequence,
        }
    }
}

static INSERT_EVENT: &str =
    "INSERT INTO events (aggregate_type, aggregate_id, sequence, payload, metadata)
                               VALUES ($1, $2, $3, $4, $5)";
static SELECT_EVENTS: &str = "SELECT aggregate_type, aggregate_id, sequence, payload, metadata
                                FROM events
                                WHERE aggregate_type = $1 AND aggregate_id = $2 ORDER BY sequence";

#[async_trait]
impl<A: Aggregate> EventStore<A, PostgresSnapshotStoreAggregateContext<A>>
for PostgresSnapshotStore<A>
{
    async fn load(&self, aggregate_id: &str) -> Vec<EventEnvelope<A>> {
        // TODO: combine with store
        match self.event_repo.get_events(aggregate_id).await {
            Ok(val) => val,
            Err(_err) => {
                // TODO: improved error handling
                Default::default()
            },
        }
    }
    async fn load_aggregate(&self, aggregate_id: &str) -> PostgresSnapshotStoreAggregateContext<A> {
        match self.repo.get_snapshot(aggregate_id).await {
            Ok(snapshot) => match snapshot {
                Some(snapshot) => {
                    let _tmp = serde_json::to_string(&snapshot.aggregate).unwrap();
                    snapshot
                },
                None => {
                    PostgresSnapshotStoreAggregateContext {
                        aggregate_id: aggregate_id.to_string(),
                        aggregate: Default::default(),
                        current_sequence: 0,
                        current_snapshot: 0,
                    }
                }
            }
            Err(e) => { panic!("{}", e); }
        }
    }

    async fn commit(
        &self,
        events: Vec<A::Event>,
        mut context: PostgresSnapshotStoreAggregateContext<A>,
        metadata: HashMap<String, String>,
    ) -> Result<Vec<EventEnvelope<A>>, AggregateError> {
        for event in events.clone() {
            context.aggregate.apply(event);
        }
        let aggregate_id = context.aggregate_id.clone();
        let wrapped_events = self.wrap_events(&aggregate_id, context.current_sequence, events, metadata);
        self.event_repo.insert_events(wrapped_events.clone()).await?;
        let last_sequence = PostgresSnapshotStore::peek_at_last_sequence(&wrapped_events);

        if context.current_sequence == 0 {
            self.repo.insert(context.aggregate, aggregate_id, last_sequence, 1).await?;
        } else {
            self.repo.update(context.aggregate, aggregate_id, last_sequence, context.current_snapshot + 1).await?;
        }


        Ok(wrapped_events)
    }
}

/// Holds context for a pure event store implementation for MemStore
#[derive(Debug, PartialEq)]
pub struct PostgresSnapshotStoreAggregateContext<A>
    where
        A: Aggregate,
{
    /// The aggregate ID of the aggregate instance that has been loaded.
    pub aggregate_id: String,
    /// The current state of the aggregate instance.
    pub(crate) aggregate: A,
    /// The last committed event sequence number for this aggregate instance.
    pub current_sequence: usize,
    /// The last committed snapshot version for this aggregate instance.
    pub current_snapshot: usize,
}

impl<A> AggregateContext<A> for PostgresSnapshotStoreAggregateContext<A>
    where
        A: Aggregate,
{
    fn aggregate(&self) -> &A {
        &self.aggregate
    }
}

impl<A> PostgresSnapshotStoreAggregateContext<A>
    where
        A: Aggregate,
{
    pub fn new(aggregate_id: String, current_sequence: usize, current_snapshot: usize, aggregate: A) -> Self {
        Self {
            aggregate_id,
            aggregate,
            current_sequence,
            current_snapshot
        }
    }
    pub(crate) fn aggregate_copy(&self) -> A {
        let ser = serde_json::to_value(&self.aggregate).unwrap();
        serde_json::from_value(ser).unwrap()
    }
}
