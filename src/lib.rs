pub mod batches;
pub mod consistencies;
pub mod extra_types;
pub mod inputs;
pub mod prepared_queries;
pub mod queries;
pub mod query_results;
pub mod scylla_cls;
pub mod utils;

use pyo3::{pymodule, types::PyModule, PyResult, Python};

#[pymodule]
#[pyo3(name = "_internal")]
fn _internal(py: Python<'_>, pymod: &PyModule) -> PyResult<()> {
    pyo3_log::init();
    pymod.add_class::<scylla_cls::Scylla>()?;
    pymod.add_class::<consistencies::ScyllaPyConsistency>()?;
    pymod.add_class::<consistencies::ScyllaPySerialConsistency>()?;
    pymod.add_class::<queries::ScyllaPyQuery>()?;
    pymod.add_class::<prepared_queries::ScyllaPyPreparedQuery>()?;
    pymod.add_class::<batches::ScyllaPyBatch>()?;
    pymod.add_class::<batches::ScyllaPyBatchType>()?;
    pymod.add_class::<query_results::ScyllaPyQueryResult>()?;
    pymod.add_submodule(extra_types::add_module(py, "extra_types")?)?;
    Ok(())
}
