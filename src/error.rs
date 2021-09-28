use std::fmt::{Debug, Display, Formatter};

use cqrs_es::persist::PersistenceError;
use cqrs_es::AggregateError;
use sqlx::Error;

#[derive(Debug, PartialEq)]
pub enum PostgresAggregateError {
    OptimisticLockError,
    ConnectionError(String),
    UnknownError(String),
}

impl Display for PostgresAggregateError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PostgresAggregateError::OptimisticLockError => write!(f, "optimistic lock error"),
            PostgresAggregateError::UnknownError(msg) => write!(f, "{}", msg),
            PostgresAggregateError::ConnectionError(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for PostgresAggregateError {}

impl From<sqlx::Error> for PostgresAggregateError {
    fn from(err: sqlx::Error) -> Self {
        // TODO: improve error handling
        match &err {
            Error::Database(database_error) => {
                if let Some(code) = database_error.code() {
                    if code.as_ref() == "23505" {
                        return PostgresAggregateError::OptimisticLockError;
                    }
                }
                PostgresAggregateError::UnknownError(format!("{:?}", err))
            }
            Error::Io(e) => PostgresAggregateError::ConnectionError(e.to_string()),
            Error::Tls(e) => PostgresAggregateError::ConnectionError(e.to_string()),
            Error::Protocol(e) => panic!("sql protocol error encountered: {}", e),
            _ => PostgresAggregateError::UnknownError(format!("{:?}", err)),
        }
    }
}

impl From<PostgresAggregateError> for AggregateError {
    fn from(err: PostgresAggregateError) -> Self {
        match err {
            PostgresAggregateError::OptimisticLockError => AggregateError::AggregateConflict,
            PostgresAggregateError::UnknownError(msg) => AggregateError::TechnicalError(msg),
            PostgresAggregateError::ConnectionError(msg) => AggregateError::TechnicalError(msg),
        }
    }
}

impl From<serde_json::Error> for PostgresAggregateError {
    fn from(err: serde_json::Error) -> Self {
        PostgresAggregateError::UnknownError(err.to_string())
    }
}

impl From<PostgresAggregateError> for PersistenceError {
    fn from(err: PostgresAggregateError) -> Self {
        match err {
            PostgresAggregateError::OptimisticLockError => PersistenceError::OptimisticLockError,
            PostgresAggregateError::UnknownError(msg) => PersistenceError::UnknownError(msg),
            PostgresAggregateError::ConnectionError(msg) => PersistenceError::ConnectionError(msg),
        }
    }
}
