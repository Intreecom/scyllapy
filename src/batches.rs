use pyo3::{pyclass, pymethods};
use scylla::batch::{Batch, BatchType};

use crate::{
    consistencies::{ScyllaPyConsistency, ScyllaPySerialConsistency},
    inputs::BatchQueryInput,
};

#[pyclass(name = "BatchType")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScyllaPyBatchType {
    COUNTER,
    LOGGED,
    UNLOGGED,
}

#[pyclass(name = "Batch")]
#[derive(Clone)]
pub struct ScyllaPyBatch {
    inner: Batch,
    #[pyo3(get)]
    pub consistency: Option<ScyllaPyConsistency>,
    #[pyo3(get)]
    pub serial_consistency: Option<ScyllaPySerialConsistency>,
    #[pyo3(get)]
    pub request_timeout: Option<u64>,
    #[pyo3(get)]
    pub timestamp: Option<i64>,
    #[pyo3(get)]
    pub is_idempotent: Option<bool>,
    #[pyo3(get)]
    pub tracing: Option<bool>,
}

impl From<ScyllaPyBatch> for Batch {
    fn from(value: ScyllaPyBatch) -> Self {
        value.inner
    }
}

#[pymethods]
impl ScyllaPyBatch {
    #[new]
    #[pyo3(signature = (
        batch_type = ScyllaPyBatchType::UNLOGGED,
        consistency = None,
        serial_consistency = None,
        request_timeout = None,
        timestamp = None,
        is_idempotent = None,
        tracing = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn py_new(
        batch_type: ScyllaPyBatchType,
        consistency: Option<ScyllaPyConsistency>,
        serial_consistency: Option<ScyllaPySerialConsistency>,
        request_timeout: Option<u64>,
        timestamp: Option<i64>,
        is_idempotent: Option<bool>,
        tracing: Option<bool>,
    ) -> Self {
        Self {
            inner: Batch::new(batch_type.into()),
            consistency,
            serial_consistency,
            request_timeout,
            timestamp,
            is_idempotent,
            tracing,
        }
    }

    pub fn add_query(&mut self, query: BatchQueryInput) {
        self.inner.append_statement(query);
    }
}

impl From<ScyllaPyBatchType> for BatchType {
    fn from(value: ScyllaPyBatchType) -> Self {
        match value {
            ScyllaPyBatchType::COUNTER => Self::Counter,
            ScyllaPyBatchType::LOGGED => Self::Logged,
            ScyllaPyBatchType::UNLOGGED => Self::Unlogged,
        }
    }
}
