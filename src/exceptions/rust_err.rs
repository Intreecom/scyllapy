use thiserror::Error;

use super::py_err::{
    ScyllaPyBaseError, ScyllaPyBindingError, ScyllaPyDBError, ScyllaPyMappingError,
    ScyllaPySessionError,
};

pub type ScyllaPyResult<T> = Result<T, ScyllaPyError>;

/// Error type for internal use.
///
/// Used only inside Rust application.
#[derive(Error, Debug)]
pub enum ScyllaPyError {
    #[error("Session error: {0}")]
    SessionError(String),
    #[error("Binding error. Cause: {0}")]
    BindingError(String),

    // Derived exception.
    #[error("Database returned error: {0}")]
    QueryError(#[from] scylla_cql::errors::QueryError),
    #[error("Database Error: {0}")]
    DBError(#[from] scylla_cql::errors::DbError),
    #[error("Python exceptiom: {0}")]
    PyError(#[from] pyo3::PyErr),
    #[error("OpenSSL error: {0}")]
    SSLError(#[from] openssl::error::ErrorStack),
    #[error("Cannot construct new session: {0}")]
    ScyllaSessionError(#[from] scylla_cql::errors::NewSessionError),

    // Binding errors
    #[error("Binding error. Cannot build values for query: {0}")]
    ScyllaValueError(#[from] scylla::frame::value::SerializeValuesError),
    #[error("Binding error. Cannot parse time, because of: {0}")]
    DateParseError(#[from] chrono::ParseError),
    #[error("Binding error. Cannot parse ip address, because of: {0}")]
    IpParseError(#[from] std::net::AddrParseError),
    #[error("Binding error. Cannot parse uuid, because of: {0}")]
    UuidParseError(#[from] uuid::Error),

    // Mapping errors
    #[error("Cannot map rows: {0}")]
    RowsDowncastError(String),
}

impl From<ScyllaPyError> for pyo3::PyErr {
    fn from(error: ScyllaPyError) -> Self {
        match error {
            ScyllaPyError::PyError(err) => err,
            ScyllaPyError::QueryError(e) => ScyllaPyDBError::new_err((e.to_string(),)),
            ScyllaPyError::DBError(e) => ScyllaPyDBError::new_err((e.to_string(),)),
            ScyllaPyError::SSLError(err) => ScyllaPyBaseError::new_err((err.to_string(),)),
            ScyllaPyError::ScyllaSessionError(err) => ScyllaPyDBError::new_err((err.to_string(),)),
            ScyllaPyError::SessionError(err) => ScyllaPySessionError::new_err((err,)),
            ScyllaPyError::BindingError(err) => ScyllaPyBindingError::new_err((err,)),
            ScyllaPyError::ScyllaValueError(err) => {
                ScyllaPyBindingError::new_err((err.to_string(),))
            }
            ScyllaPyError::DateParseError(err) => ScyllaPyBindingError::new_err((err.to_string(),)),
            ScyllaPyError::IpParseError(err) => ScyllaPyBindingError::new_err((err.to_string(),)),
            ScyllaPyError::UuidParseError(err) => ScyllaPyBindingError::new_err((err.to_string(),)),
            ScyllaPyError::RowsDowncastError(err) => ScyllaPyMappingError::new_err((err,)),
        }
    }
}
