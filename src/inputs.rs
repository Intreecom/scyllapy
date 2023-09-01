use pyo3::FromPyObject;

use crate::{
    batches::{ScyllaPyBatch, ScyllaPyInlineBatch},
    prepared_queries::ScyllaPyPreparedQuery,
    queries::ScyllaPyQuery,
};
use scylla::{batch::BatchStatement, query::Query};

#[derive(Clone, FromPyObject)]
pub enum ExecuteInput {
    #[pyo3(transparent, annotation = "str")]
    Text(String),
    #[pyo3(transparent, annotation = "Query")]
    Query(ScyllaPyQuery),
    #[pyo3(transparent, annotation = "PreparedQuery")]
    PreparedQuery(ScyllaPyPreparedQuery),
}

#[derive(Clone, FromPyObject)]
pub enum BatchQueryInput {
    #[pyo3(transparent, annotation = "str")]
    Text(String),
    #[pyo3(transparent, annotation = "Query")]
    Query(ScyllaPyQuery),
    #[pyo3(transparent, annotation = "PreparedQuery")]
    PreparedQuery(ScyllaPyPreparedQuery),
}

impl From<BatchQueryInput> for BatchStatement {
    fn from(value: BatchQueryInput) -> Self {
        match value {
            BatchQueryInput::Text(text) => Self::Query(text.into()),
            BatchQueryInput::Query(query) => Self::Query(query.into()),
            BatchQueryInput::PreparedQuery(prepared) => Self::PreparedStatement(prepared.into()),
        }
    }
}

#[derive(Clone, FromPyObject)]
pub enum PrepareInput {
    #[pyo3(transparent, annotation = "str")]
    Text(String),
    #[pyo3(transparent, annotation = "Query")]
    Query(ScyllaPyQuery),
}

impl From<PrepareInput> for Query {
    fn from(value: PrepareInput) -> Self {
        match value {
            PrepareInput::Text(text) => Self::new(text),
            PrepareInput::Query(query) => Self::from(query),
        }
    }
}

#[derive(Clone, FromPyObject)]
pub enum BatchInput {
    #[pyo3(transparent, annotation = "Batch")]
    Batch(ScyllaPyBatch),
    #[pyo3(transparent, annotation = "InlineBatch")]
    InlineBatch(ScyllaPyInlineBatch),
}
