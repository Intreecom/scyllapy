use pyo3::{pyclass, pymethods, types::PyDict, PyAny};
use scylla::batch::{Batch, BatchStatement, BatchType};

use crate::{
    exceptions::rust_err::ScyllaPyResult, inputs::BatchQueryInput, queries::ScyllaPyRequestParams,
    utils::parse_python_query_params,
};
use scylla::frame::value::SerializedValues;

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

#[pyclass(name = "InlineBatch")]
#[derive(Clone)]
pub struct ScyllaPyInlineBatch {
    inner: Batch,
    request_params: ScyllaPyRequestParams,
    values: Vec<SerializedValues>,
}

impl From<ScyllaPyBatch> for Batch {
    fn from(value: ScyllaPyBatch) -> Self {
        let mut inner = value.inner;
        value.request_params.apply_to_batch(&mut inner);
        inner
    }
}

impl From<ScyllaPyInlineBatch> for (Batch, Vec<SerializedValues>) {
    fn from(mut value: ScyllaPyInlineBatch) -> Self {
        value.request_params.apply_to_batch(&mut value.inner);
        (value.inner, value.values)
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
    pub fn py_new(batch_type: ScyllaPyBatchType, params: Option<&PyDict>) -> ScyllaPyResult<Self> {
        Ok(Self {
            inner: Batch::new(batch_type.into()),
            request_params: ScyllaPyRequestParams::from_dict(params)?,
        })
    }

    pub fn add_query(&mut self, query: BatchQueryInput) {
        self.inner.append_statement(query);
    }
}

impl ScyllaPyInlineBatch {
    pub fn add_query_inner(
        &mut self,
        query: impl Into<BatchStatement>,
        values: impl Into<SerializedValues>,
    ) {
        self.inner.append_statement(query);
        self.values.push(values.into());
    }
}

#[pymethods]
impl ScyllaPyInlineBatch {
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
    pub fn py_new(batch_type: ScyllaPyBatchType, params: Option<&PyDict>) -> ScyllaPyResult<Self> {
        Ok(Self {
            inner: Batch::new(batch_type.into()),
            request_params: ScyllaPyRequestParams::from_dict(params)?,
            values: vec![],
        })
    }

    /// Add query to batch.
    ///
    /// This function appends query to batch.
    /// along with values, so you don't need to
    /// pass values in execute.
    ///
    /// # Errors
    ///
    /// Will result in an error, if
    /// values are incorrect.
    #[pyo3(signature = (query, values = None))]
    pub fn add_query(
        &mut self,
        query: BatchQueryInput,
        values: Option<&PyAny>,
    ) -> ScyllaPyResult<()> {
        self.inner.append_statement(query);
        if let Some(passed_params) = values {
            self.values
                .push(parse_python_query_params(Some(passed_params), false)?);
        } else {
            self.values.push(SerializedValues::new());
        }
        Ok(())
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
