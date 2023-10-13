use std::sync::Arc;

use pyo3::{
    pyclass, pymethods,
    types::{PyModule, PyType},
    PyAny, PyResult, Python,
};
use scylla::load_balancing::{DefaultPolicy, LatencyAwarenessBuilder, LoadBalancingPolicy};
use std::time::Duration;

use crate::{exceptions::rust_err::ScyllaPyResult, utils::scyllapy_future};

#[pyclass(name = "LoadBalancingPolicy")]
#[derive(Clone, Debug)]
pub struct ScyllaPyLoadBalancingPolicy {
    inner: Arc<dyn LoadBalancingPolicy>,
}

#[pymethods]
impl ScyllaPyLoadBalancingPolicy {
    #[classmethod]
    #[pyo3(signature = (
        *,
        token_aware = None,
        prefer_rack = None,
        prefer_datacenter = None,
        permit_dc_failover = None,
        shuffling_replicas = None,
        latency_awareness = None,
    )
    )]
    fn build(
        cls: &PyType,
        token_aware: Option<bool>,
        prefer_rack: Option<String>,
        prefer_datacenter: Option<String>,
        permit_dc_failover: Option<bool>,
        shuffling_replicas: Option<bool>,
        latency_awareness: Option<ScyllaPyLatencyAwareness>,
    ) -> ScyllaPyResult<&PyAny> {
        scyllapy_future(cls.py(), async move {
            let mut policy_builer = DefaultPolicy::builder();
            if let Some(permit) = permit_dc_failover {
                policy_builer = policy_builer.permit_dc_failover(permit);
            }
            if let Some(token) = token_aware {
                policy_builer = policy_builer.token_aware(token);
            }
            if let Some(dc) = prefer_datacenter {
                if let Some(rack) = prefer_rack {
                    policy_builer = policy_builer.prefer_datacenter_and_rack(dc, rack);
                } else {
                    policy_builer = policy_builer.prefer_datacenter(dc);
                }
            }
            if let Some(shufle) = shuffling_replicas {
                policy_builer = policy_builer.enable_shuffling_replicas(shufle);
            }
            if let Some(latency_awareness) = latency_awareness {
                policy_builer = policy_builer.latency_awareness(latency_awareness.into());
            }
            Ok(Self {
                inner: policy_builer.build(),
            })
        })
    }
}

#[pyclass(name = "LatencyAwareness")]
#[derive(Clone, Debug)]
pub struct ScyllaPyLatencyAwareness {
    inner: LatencyAwarenessBuilder,
}

#[pymethods]
impl ScyllaPyLatencyAwareness {
    #[new]
    #[pyo3(signature = (
        *,
        minimum_measurements = None,
        retry_period = None,
        exclusion_threshold = None,
        update_rate = None,
        scale = None,
    ))]
    fn new(
        minimum_measurements: Option<usize>,
        retry_period: Option<u64>,
        exclusion_threshold: Option<f64>,
        update_rate: Option<u64>,
        scale: Option<u64>,
    ) -> Self {
        let mut builder = LatencyAwarenessBuilder::new();
        if let Some(minimum_measurements) = minimum_measurements {
            builder = builder.minimum_measurements(minimum_measurements);
        }
        if let Some(retry_period) = retry_period {
            builder = builder.retry_period(Duration::from_millis(retry_period));
        }
        if let Some(exclusion_threshold) = exclusion_threshold {
            builder = builder.exclusion_threshold(exclusion_threshold);
        }
        if let Some(update_rate) = update_rate {
            builder = builder.update_rate(Duration::from_millis(update_rate));
        }
        if let Some(scale) = scale {
            builder = builder.scale(Duration::from_millis(scale));
        }
        Self { inner: builder }
    }
}

impl From<ScyllaPyLatencyAwareness> for LatencyAwarenessBuilder {
    fn from(value: ScyllaPyLatencyAwareness) -> Self {
        value.inner
    }
}

impl From<ScyllaPyLoadBalancingPolicy> for Arc<dyn LoadBalancingPolicy> {
    fn from(value: ScyllaPyLoadBalancingPolicy) -> Self {
        value.inner
    }
}

/// Setup load balancing module.
///
/// This function adds `LoadBalancingPolicy` and `LatencyAwareness` classes to the module.
///
/// # Errors
///
/// If cannot add class to the module.
pub fn setup_module(_py: Python<'_>, module: &PyModule) -> PyResult<()> {
    module.add_class::<ScyllaPyLoadBalancingPolicy>()?;
    module.add_class::<ScyllaPyLatencyAwareness>()?;
    Ok(())
}
