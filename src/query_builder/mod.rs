use pyo3::{types::PyModule, PyResult, Python};

use self::{insert::Insert, select::Select};

pub mod insert;
pub mod select;
mod utils;

pub fn add_module<'a>(py: Python<'a>, name: &'static str) -> PyResult<&'a PyModule> {
    let module = PyModule::new(py, name)?;
    module.add_class::<Select>()?;
    module.add_class::<Insert>()?;
    Ok(module)
}
