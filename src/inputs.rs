use pyo3::FromPyObject;

use crate::query::Query;

use scylla::query::Query as ScyllaQuery;

#[derive(Clone, FromPyObject, Debug)]
pub enum QueryInput {
    #[pyo3(transparent, annotation = "str")]
    Text(String),
    #[pyo3(transparent, annotation = "Query")]
    Query(Query),
}

impl From<QueryInput> for ScyllaQuery {
    fn from(value: QueryInput) -> Self {
        match value {
            QueryInput::Query(query) => Self::from(query),
            QueryInput::Text(text) => Self::new(text),
        }
    }
}
