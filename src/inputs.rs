use pyo3::FromPyObject;

use crate::{prepared_query::PreparedQuery, query::Query};
use scylla::query::Query as ScyllaQuery;

#[derive(Clone, FromPyObject, Debug)]
pub enum ExecuteInput {
    #[pyo3(transparent, annotation = "str")]
    Text(String),
    #[pyo3(transparent, annotation = "Query")]
    Query(Query),
    #[pyo3(transparent, annotation = "PreparedQuery")]
    PreparedQuery(PreparedQuery),
}

#[derive(Clone, FromPyObject, Debug)]
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
