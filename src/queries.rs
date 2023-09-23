use std::time::Duration;

use crate::{
    consistencies::{ScyllaPyConsistency, ScyllaPySerialConsistency},
    exceptions::rust_err::ScyllaPyResult,
    execution_profiles::ScyllaPyExecutionProfile,
};
use pyo3::{pyclass, pymethods, types::PyDict, FromPyObject, Python};
use scylla::{batch::Batch, execution_profile::ExecutionProfileHandle, statement::query::Query};

#[derive(Clone, Debug, Default, FromPyObject)]
pub struct ScyllaPyRequestParams {
    pub consistency: Option<ScyllaPyConsistency>,
    pub serial_consistency: Option<ScyllaPySerialConsistency>,
    pub request_timeout: Option<u64>,
    pub timestamp: Option<i64>,
    pub is_idempotent: Option<bool>,
    pub tracing: Option<bool>,
    pub profile: Option<ScyllaPyExecutionProfile>,
}

impl ScyllaPyRequestParams {
    /// Apply parameters to scylla's query.
    pub fn apply_to_query(&self, query: &mut Query) {
        if let Some(consistency) = self.consistency {
            query.set_consistency(consistency.into());
        }
        if let Some(is_idempotent) = self.is_idempotent {
            query.set_is_idempotent(is_idempotent);
        }
        if let Some(tracing) = self.tracing {
            query.set_tracing(tracing);
        }
        query.set_execution_profile_handle(self.profile.as_ref().map(ExecutionProfileHandle::from));
        query.set_timestamp(self.timestamp);
        query.set_request_timeout(self.request_timeout.map(Duration::from_secs));
        query.set_serial_consistency(self.serial_consistency.map(Into::into));
    }

    pub fn apply_to_batch(&self, batch: &mut Batch) {
        if let Some(consistency) = self.consistency {
            batch.set_consistency(consistency.into());
        }
        if let Some(is_idempotent) = self.is_idempotent {
            batch.set_is_idempotent(is_idempotent);
        }
        if let Some(tracing) = self.tracing {
            batch.set_tracing(tracing);
        }
        batch.set_timestamp(self.timestamp);
        batch.set_serial_consistency(self.serial_consistency.map(Into::into));
    }

    /// Parse dict to query parameters.
    ///
    /// This function takes dict and
    /// tries to construct `ScyllaPyRequestParams`.
    ///
    /// # Errors
    ///
    /// May result in an error if
    /// incorrect type passed.
    pub fn from_dict(params: Option<&PyDict>) -> ScyllaPyResult<Self> {
        let Some(params) = params else {
            return Ok(Self::default());
        };
        Ok(Self {
            consistency: params
                .get_item("consistency")
                .map(pyo3::FromPyObject::extract)
                .transpose()?,
            serial_consistency: params
                .get_item("serial_consistency")
                .map(pyo3::FromPyObject::extract)
                .transpose()?,
            request_timeout: params
                .get_item("request_timeout")
                .map(pyo3::FromPyObject::extract)
                .transpose()?,
            timestamp: params
                .get_item("timestamp")
                .map(pyo3::FromPyObject::extract)
                .transpose()?,
            is_idempotent: params
                .get_item("is_idempotent")
                .map(pyo3::FromPyObject::extract)
                .transpose()?,
            tracing: params
                .get_item("tracing")
                .map(pyo3::FromPyObject::extract)
                .transpose()?,
            profile: params
                .get_item("profile")
                .map(pyo3::FromPyObject::extract)
                .transpose()?,
        })
    }
}

#[pyclass(name = "Query")]
#[derive(Clone, Debug)]
pub struct ScyllaPyQuery {
    #[pyo3(get)]
    pub query: String,
    pub params: ScyllaPyRequestParams,
}

impl From<&ScyllaPyQuery> for ScyllaPyQuery {
    fn from(value: &ScyllaPyQuery) -> Self {
        ScyllaPyQuery {
            query: value.query.clone(),
            params: ScyllaPyRequestParams::default(),
        }
    }
}

#[pymethods]
impl ScyllaPyQuery {
    #[new]
    #[pyo3(signature = (query,**kwargs))]
    #[allow(clippy::too_many_arguments)]
    /// Creates new query.
    ///
    /// # Errors
    /// May raise an error if incorrect type passed in kwargs.
    pub fn py_new(_py: Python<'_>, query: String, kwargs: Option<&PyDict>) -> ScyllaPyResult<Self> {
        Ok(Self {
            query,
            params: ScyllaPyRequestParams::from_dict(kwargs)?,
        })
    }

    #[must_use]
    pub fn __str__(&self) -> String {
        format!("{self:?}")
    }

    #[must_use]
    pub fn with_consistency(&self, consistency: Option<ScyllaPyConsistency>) -> Self {
        let mut query = Self::from(self);
        query.params.consistency = consistency;
        query
    }

    #[must_use]
    pub fn with_serial_consistency(
        &self,
        serial_consistency: Option<ScyllaPySerialConsistency>,
    ) -> Self {
        let mut query = Self::from(self);
        query.params.serial_consistency = serial_consistency;
        query
    }

    #[must_use]
    pub fn with_request_timeout(&self, request_timeout: Option<u64>) -> Self {
        let mut query = Self::from(self);
        query.params.request_timeout = request_timeout;
        query
    }

    #[must_use]
    pub fn with_timestamp(&self, timestamp: Option<i64>) -> Self {
        let mut query = Self::from(self);
        query.params.timestamp = timestamp;
        query
    }

    #[must_use]
    pub fn with_is_idempotent(&self, is_idempotent: Option<bool>) -> Self {
        let mut query = Self::from(self);
        query.params.is_idempotent = is_idempotent;
        query
    }

    #[must_use]
    pub fn with_tracing(&self, tracing: Option<bool>) -> Self {
        let mut query = Self::from(self);
        query.params.tracing = tracing;
        query
    }

    #[must_use]
    pub fn with_profile(&self, profile: Option<ScyllaPyExecutionProfile>) -> Self {
        let mut query = Self::from(self);
        query.params.profile = profile;
        query
    }
}

impl From<ScyllaPyQuery> for Query {
    fn from(value: ScyllaPyQuery) -> Self {
        let mut query = Self::new(value.query);
        value.params.apply_to_query(&mut query);
        query
    }
}
