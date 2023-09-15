use thiserror::Error;

use super::py_err::{ScyllaPyBaseError, ScyllaPyDBError};

pub type ScyllaPyResult<T> = Result<T, ScyllaPyError>;

/// Error type for internal use.
///
/// Used only inside Rust application.
#[derive(Error, Debug)]
pub enum ScyllaPyError {
    #[error("Database returned error: {0}")]
    QueryError(#[from] scylla_cql::errors::QueryError),
    #[error("Database Error: {0}")]
    DBError(#[from] scylla_cql::errors::DbError),
    #[error("Uncaught exception: {0}")]
    UncaughtException(#[from] anyhow::Error),
    #[error("Python exceptiom: {0}")]
    PyError(#[from] pyo3::PyErr),
    #[error("OpenSSL error: {0}")]
    SSLError(#[from] openssl::error::ErrorStack),
    #[error("Cannot construct new session: {0}")]
    ScyllaSessionError(#[from] scylla_cql::errors::NewSessionError),
}

impl From<ScyllaPyError> for pyo3::PyErr {
    fn from(error: ScyllaPyError) -> Self {
        match error {
            ScyllaPyError::QueryError(e) => ScyllaPyDBError::new_err((e.to_string(),)),
            ScyllaPyError::DBError(e) => ScyllaPyDBError::new_err((e.to_string(),)),
            ScyllaPyError::UncaughtException(e) => ScyllaPyBaseError::new_err((e.to_string(),)),
            ScyllaPyError::SSLError(err) => ScyllaPyBaseError::new_err((err.to_string(),)),
            ScyllaPyError::ScyllaSessionError(err) => ScyllaPyDBError::new_err((err.to_string(),)),
            ScyllaPyError::PyError(err) => err,
        }
    }
}
