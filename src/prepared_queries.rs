use pyo3::pyclass;
use scylla::prepared_statement::PreparedStatement;

#[pyclass(name = "PreparedQuery", from_py_object)]
#[derive(Clone, Debug)]
pub struct ScyllaPyPreparedQuery {
    pub inner: PreparedStatement,
}

impl From<PreparedStatement> for ScyllaPyPreparedQuery {
    fn from(value: PreparedStatement) -> Self {
        Self { inner: value }
    }
}

impl From<ScyllaPyPreparedQuery> for PreparedStatement {
    fn from(value: ScyllaPyPreparedQuery) -> Self {
        value.inner
    }
}
