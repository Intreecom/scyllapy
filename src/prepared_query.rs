use pyo3::pyclass;
use scylla::prepared_statement::PreparedStatement;

#[pyclass]
#[derive(Clone, Debug)]
pub struct PreparedQuery {
    inner: PreparedStatement,
}

impl From<PreparedStatement> for PreparedQuery {
    fn from(value: PreparedStatement) -> Self {
        Self { inner: value }
    }
}

impl From<PreparedQuery> for PreparedStatement {
    fn from(value: PreparedQuery) -> Self {
        value.inner
    }
}
