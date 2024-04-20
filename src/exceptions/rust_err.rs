use thiserror::Error;

use super::py_err::{
    ScyllaPyBaseError, ScyllaPyBindingError, ScyllaPyDBError, ScyllaPyMappingError,
    ScyllaPyQueryBuiderError, ScyllaPySessionError,
};

pub type ScyllaPyResult<T> = Result<T, ScyllaPyError>;

/// Error type for internal use.
///
/// Used only inside Rust application.
#[derive(Error, Debug)]
pub enum ScyllaPyError {
    #[error("Session error: {0}.")]
    SessionError(String),
    #[error("Binding error. Cause: {0}.")]
    BindingError(String),

    // Derived exception.
    #[error("{0}")]
    QueryError(#[from] scylla::transport::errors::QueryError),
    #[error("{0}")]
    DBError(#[from] scylla::transport::errors::DbError),
    #[error("Python exception: {0}.")]
    PyError(#[from] pyo3::PyErr),
    #[error("OpenSSL error: {0}.")]
    SSLError(#[from] openssl::error::ErrorStack),
    #[error("Cannot construct new session: {0}.")]
    ScyllaSessionError(#[from] scylla::transport::errors::NewSessionError),

    // Binding errors
    #[error("Binding error. Cannot build values for query: {0},")]
    ScyllaValueError(#[from] scylla::frame::value::SerializeValuesError),
    #[error("Binding error. Cannot parse time, because of: {0}.")]
    DateParseError(#[from] chrono::ParseError),
    #[error("Binding error. Cannot parse ip address, because of: {0}.")]
    IpParseError(#[from] std::net::AddrParseError),
    #[error("Binding error. Cannot parse uuid, because of: {0}.")]
    UuidParseError(#[from] uuid::Error),

    // Mapping errors
    #[error("Cannot map rows: {0}.")]
    RowsDowncastError(String),
    #[error("Cannot parse value of column {0} as {1}.")]
    ValueDowncastError(String, &'static str),
    #[error("Cannot downcast UDT {0} of column {1}. Reason: {2}.")]
    UDTDowncastError(String, String, String),
    #[error("Query didn't suppose to return anything.")]
    NoReturnsError,
    #[error("Query doesn't have columns.")]
    NoColumns,

    // QueryBuilder errors
    #[error("Query builder error: {0}.")]
    QueryBuilderError(&'static str),
}

impl From<ScyllaPyError> for pyo3::PyErr {
    fn from(error: ScyllaPyError) -> Self {
        let err_desc = error.to_string();
        match error {
            ScyllaPyError::PyError(err) => err,
            ScyllaPyError::SSLError(_) => ScyllaPyBaseError::new_err((err_desc,)),
            ScyllaPyError::QueryError(_) | ScyllaPyError::DBError(_) => {
                ScyllaPyDBError::new_err((err_desc,))
            }
            ScyllaPyError::SessionError(_) | ScyllaPyError::ScyllaSessionError(_) => {
                ScyllaPySessionError::new_err((err_desc,))
            }
            ScyllaPyError::BindingError(_)
            | ScyllaPyError::ScyllaValueError(_)
            | ScyllaPyError::DateParseError(_)
            | ScyllaPyError::UuidParseError(_)
            | ScyllaPyError::IpParseError(_) => ScyllaPyBindingError::new_err((err_desc,)),
            ScyllaPyError::RowsDowncastError(_)
            | ScyllaPyError::ValueDowncastError(_, _)
            | ScyllaPyError::UDTDowncastError(_, _, _)
            | ScyllaPyError::NoReturnsError
            | ScyllaPyError::NoColumns => ScyllaPyMappingError::new_err((err_desc,)),
            ScyllaPyError::QueryBuilderError(_) => ScyllaPyQueryBuiderError::new_err((err_desc,)),
        }
    }
}
