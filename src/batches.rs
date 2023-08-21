use pyo3::{pyclass, pymethods};
use scylla::batch::{Batch as ScyllaBatch, BatchType as ScyllaBatchType};

use crate::{
    consistencies::{Consistency, SerialConsistency},
    inputs::BatchQueryInput,
};

#[pyclass]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchType {
    COUNTER,
    LOGGED,
    UNLOGGED,
}

#[pyclass]
#[derive(Clone)]
pub struct Batch {
    inner: ScyllaBatch,
    #[pyo3(get)]
    pub consistency: Option<Consistency>,
    #[pyo3(get)]
    pub serial_consistency: Option<SerialConsistency>,
    #[pyo3(get)]
    pub request_timeout: Option<u64>,
    #[pyo3(get)]
    pub timestamp: Option<i64>,
    #[pyo3(get)]
    pub is_idempotent: Option<bool>,
    #[pyo3(get)]
    pub tracing: Option<bool>,
}

impl From<Batch> for ScyllaBatch {
    fn from(value: Batch) -> Self {
        value.inner
    }
}

#[pymethods]
impl Batch {
    #[new]
    #[pyo3(signature = (
        batch_type = BatchType::UNLOGGED,
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
        batch_type: BatchType,
        consistency: Option<Consistency>,
        serial_consistency: Option<SerialConsistency>,
        request_timeout: Option<u64>,
        timestamp: Option<i64>,
        is_idempotent: Option<bool>,
        tracing: Option<bool>,
    ) -> Self {
        Self {
            inner: ScyllaBatch::new(batch_type.into()),
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

impl From<BatchType> for ScyllaBatchType {
    fn from(value: BatchType) -> Self {
        match value {
            BatchType::COUNTER => Self::Counter,
            BatchType::LOGGED => Self::Logged,
            BatchType::UNLOGGED => Self::Unlogged,
        }
    }
}
