use pyo3::FromPyObject;

use crate::{prepared_query::PreparedQuery, query::Query};
use scylla::{batch::BatchStatement, query::Query as ScyllaQuery};

#[derive(Clone, FromPyObject)]
pub enum ExecuteInput {
    #[pyo3(transparent, annotation = "str")]
    Text(String),
    #[pyo3(transparent, annotation = "Query")]
    Query(Query),
    #[pyo3(transparent, annotation = "PreparedQuery")]
    PreparedQuery(PreparedQuery),
}

#[derive(Clone, FromPyObject)]
pub enum BatchQueryInput {
    #[pyo3(transparent, annotation = "str")]
    Text(String),
    #[pyo3(transparent, annotation = "Query")]
    Query(Query),
    #[pyo3(transparent, annotation = "PreparedQuery")]
    PreparedQuery(PreparedQuery),
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
    Query(Query),
}

impl From<PrepareInput> for ScyllaQuery {
    fn from(value: PrepareInput) -> Self {
        match value {
            PrepareInput::Text(text) => Self::new(text),
            PrepareInput::Query(query) => Self::from(query),
        }
    }
}
