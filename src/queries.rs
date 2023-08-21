use std::time::Duration;

use crate::consistencies::{ScyllaPyConsistency, ScyllaPySerialConsistency};
use pyo3::{pyclass, pymethods, Python};
use scylla::statement::query::Query;

#[pyclass(name = "Query")]
#[derive(Clone, Debug)]
pub struct ScyllaPyQuery {
    #[pyo3(get)]
    pub query: String,
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

impl From<&ScyllaPyQuery> for ScyllaPyQuery {
    fn from(value: &ScyllaPyQuery) -> Self {
        ScyllaPyQuery {
            query: value.query.clone(),
            consistency: value.consistency,
            serial_consistency: value.serial_consistency,
            request_timeout: value.request_timeout,
            timestamp: value.timestamp,
            is_idempotent: value.is_idempotent,
            tracing: value.tracing,
        }
    }
}

#[pymethods]
impl ScyllaPyQuery {
    #[new]
    #[pyo3(signature = (
        query,
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
        _py: Python<'_>,
        query: String,
        consistency: Option<ScyllaPyConsistency>,
        serial_consistency: Option<ScyllaPySerialConsistency>,
        request_timeout: Option<u64>,
        timestamp: Option<i64>,
        is_idempotent: Option<bool>,
        tracing: Option<bool>,
    ) -> Self {
        Self {
            query,
            consistency,
            serial_consistency,
            request_timeout,
            timestamp,
            is_idempotent,
            tracing,
        }
    }

    #[must_use]
    pub fn __str__(&self) -> String {
        format!("{self:?}")
    }

    #[must_use]
    pub fn with_consistency(&self, consistency: Option<ScyllaPyConsistency>) -> Self {
        let mut query = Self::from(self);
        query.consistency = consistency;
        query
    }

    #[must_use]
    pub fn with_serial_consistency(
        &self,
        serial_consistency: Option<ScyllaPySerialConsistency>,
    ) -> Self {
        let mut query = Self::from(self);
        query.serial_consistency = serial_consistency;
        query
    }

    #[must_use]
    pub fn with_request_timeout(&self, request_timeout: Option<u64>) -> Self {
        let mut query = Self::from(self);
        query.request_timeout = request_timeout;
        query
    }

    #[must_use]
    pub fn with_timestamp(&self, timestamp: Option<i64>) -> Self {
        let mut query = Self::from(self);
        query.timestamp = timestamp;
        query
    }

    #[must_use]
    pub fn with_is_idempotent(&self, is_idempotent: Option<bool>) -> Self {
        let mut query = Self::from(self);
        query.is_idempotent = is_idempotent;
        query
    }

    #[must_use]
    pub fn with_tracing(&self, tracing: Option<bool>) -> Self {
        let mut query = Self::from(self);
        query.tracing = tracing;
        query
    }
}

impl From<ScyllaPyQuery> for Query {
    fn from(value: ScyllaPyQuery) -> Self {
        let mut query = Self::new(value.query);
        if let Some(consistency) = value.consistency {
            query.set_consistency(consistency.into());
        }
        if let Some(is_idempotent) = value.is_idempotent {
            query.set_is_idempotent(is_idempotent);
        }
        if let Some(tracing) = value.tracing {
            query.set_tracing(tracing);
        }
        query.set_timestamp(value.timestamp);
        query.set_request_timeout(value.request_timeout.map(Duration::from_secs));
        query.set_serial_consistency(value.serial_consistency.map(Into::into));
        query
    }
}
