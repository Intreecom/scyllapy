pub mod batches;
pub mod consistencies;
pub mod extra_types;
pub mod inputs;
pub mod prepared_queries;
pub mod queries;
pub mod query_builder;
pub mod query_results;
pub mod scylla_cls;
pub mod utils;

use pyo3::{pymodule, types::PyModule, PyResult, Python};

use crate::utils::add_submodule;

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
    pymod.add_class::<batches::ScyllaPyInlineBatch>()?;
    pymod.add_class::<query_results::ScyllaPyQueryResult>()?;
    add_submodule(py, pymod, "extra_types", extra_types::module_constructor)?;
    add_submodule(py, pymod, "query_builder", query_builder::add_module)?;
    Ok(())
}
