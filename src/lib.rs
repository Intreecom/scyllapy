pub mod consistencies;
pub mod inputs;
pub mod query;
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
    Ok(())
}
