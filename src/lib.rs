pub mod batches;
pub mod consistencies;
pub mod inputs;
pub mod prepared_query;
pub mod query;
pub mod query_results;
pub mod scylla_cls;
pub mod utils;

use pyo3::{pymodule, types::PyModule, PyResult, Python};

#[pymodule]
#[pyo3(name = "_internal")]
fn _internal(_py: Python<'_>, pymod: &PyModule) -> PyResult<()> {
    pyo3_log::init();
    pymod.add_class::<scylla_cls::Scylla>()?;
    pymod.add_class::<consistencies::Consistency>()?;
    pymod.add_class::<consistencies::SerialConsistency>()?;
    pymod.add_class::<query::Query>()?;
    pymod.add_class::<prepared_query::PreparedQuery>()?;
    pymod.add_class::<batches::Batch>()?;
    pymod.add_class::<batches::BatchType>()?;
    pymod.add_class::<query_results::ScyllaPyQueryResult>()?;
    Ok(())
}
