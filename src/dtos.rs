use pyo3::{pyclass, IntoPy, Py, PyAny};
use scylla::{
    FromRow,
    _macro_internal::{CqlValue, Row},
    cql_to_rust::{FromCqlVal, FromCqlValError},
};

#[derive(Clone, Copy)]
pub struct PyUuid {
    internal: uuid::Uuid,
}

impl IntoPy<Py<PyAny>> for PyUuid {
    fn into_py(self, py: pyo3::Python<'_>) -> Py<PyAny> {
        py.import("uuid")
            .unwrap()
            .getattr("UUID")
            .unwrap()
            .call1((self.internal.simple().to_string(),))
            .unwrap()
            .into()
    }
}

impl FromCqlVal<CqlValue> for PyUuid {
    fn from_cql(cql_val: CqlValue) -> Result<Self, scylla::cql_to_rust::FromCqlValError> {
        let possible_uuid = cql_val.as_uuid();
        if possible_uuid.is_none() {
            return Err(FromCqlValError::BadCqlType);
        }
        Ok(PyUuid {
            internal: possible_uuid.unwrap(),
        })
    }
}

#[pyclass]
pub struct InboxDTO {
    #[pyo3(get)]
    user_id: PyUuid,
    #[pyo3(get)]
    chat_id: PyUuid,
}

impl FromRow for InboxDTO {
    fn from_row(row: Row) -> Result<Self, scylla::cql_to_rust::FromRowError> {
        let parsed = row.into_typed::<(PyUuid, PyUuid)>()?;
        Ok(InboxDTO {
            user_id: parsed.0,
            chat_id: parsed.1,
        })
    }
}
