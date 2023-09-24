use std::time::Duration;

use pyo3::{pyclass, pymethods};
use scylla::{execution_profile::ExecutionProfileHandle, statement::SerialConsistency};

use crate::{
    consistencies::{ScyllaPyConsistency, ScyllaPySerialConsistency},
    load_balancing::ScyllaPyLoadBalancingPolicy,
};

#[pyclass(name = "ExecutionProfile")]
#[derive(Clone, Debug)]
pub struct ScyllaPyExecutionProfile {
    inner: scylla::ExecutionProfile,
}

#[pymethods]
impl ScyllaPyExecutionProfile {
    #[new]
    #[pyo3(signature = (*,
        consistency=None,
        serial_consistency=None,
        request_timeout=None,
        load_balancing_policy = None
    ))]
    fn py_new(
        consistency: Option<ScyllaPyConsistency>,
        serial_consistency: Option<ScyllaPySerialConsistency>,
        request_timeout: Option<u64>,
        load_balancing_policy: Option<ScyllaPyLoadBalancingPolicy>,
    ) -> Self {
        let mut profile_builder = scylla::ExecutionProfile::builder();
        if let Some(consistency) = consistency {
            profile_builder = profile_builder.consistency(consistency.into());
        }
        if let Some(load_balancing_policy) = load_balancing_policy {
            profile_builder = profile_builder.load_balancing_policy(load_balancing_policy.into());
        }
        profile_builder = profile_builder
            .serial_consistency(serial_consistency.map(SerialConsistency::from))
            .request_timeout(request_timeout.map(Duration::from_secs));
        Self {
            inner: profile_builder.build(),
        }
    }
}

impl From<&ScyllaPyExecutionProfile> for ExecutionProfileHandle {
    fn from(value: &ScyllaPyExecutionProfile) -> Self {
        value.inner.clone().into_handle()
    }
}

impl From<ScyllaPyExecutionProfile> for ExecutionProfileHandle {
    fn from(value: ScyllaPyExecutionProfile) -> Self {
        value.inner.into_handle()
    }
}
