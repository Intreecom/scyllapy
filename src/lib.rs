pub mod consistency;
pub mod scylla_cls;
pub mod utils;

use consistency::Consistency;
use pyo3::{pymodule, types::PyModule, PyResult, Python};
use scylla_cls::Scylla;

#[pymodule]
#[pyo3(name = "_internal")]
fn _internal(_py: Python<'_>, pymod: &PyModule) -> PyResult<()> {
    pyo3_log::init();
    pymod.add_class::<Scylla>()?;
    pymod.add_class::<Consistency>()?;
    Ok(())
}
