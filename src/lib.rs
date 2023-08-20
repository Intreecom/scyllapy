pub mod scylla_main;
pub mod utils;

use pyo3::{pymodule, types::PyModule, PyResult, Python};
use scylla_main::Scylla;

#[pymodule]
#[pyo3(name = "_internal")]
fn _internal(_py: Python<'_>, pymod: &PyModule) -> PyResult<()> {
    pyo3_log::init();
    pymod.add_class::<Scylla>()?;
    Ok(())
}
