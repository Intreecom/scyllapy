use pyo3::{pyclass, pymethods, types::PyDict};
use scylla::batch::{Batch, BatchType};

use crate::{inputs::BatchQueryInput, queries::ScyllaPyRequestParams};

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
    request_params: ScyllaPyRequestParams,
}

impl From<ScyllaPyBatch> for Batch {
    fn from(value: ScyllaPyBatch) -> Self {
        let mut inner = value.inner;
        value.request_params.apply_to_batch(&mut inner);
        inner
    }
}

#[pymethods]
impl ScyllaPyBatch {
    /// Create new batch.
    ///
    /// # Errors
    ///
    /// Can return an error in case if
    /// wrong type for parameters were passed.
    #[new]
    #[pyo3(signature = (
        batch_type = ScyllaPyBatchType::UNLOGGED,
        **params
    ))]
    pub fn py_new(batch_type: ScyllaPyBatchType, params: Option<&PyDict>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Batch::new(batch_type.into()),
            request_params: ScyllaPyRequestParams::from_dict(params)?,
        })
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
