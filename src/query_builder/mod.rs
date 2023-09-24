use pyo3::{types::PyModule, PyResult, Python};

use self::{delete::Delete, insert::Insert, select::Select, update::Update};

pub mod delete;
pub mod insert;
pub mod select;
pub mod update;
mod utils;

/// Create `QueryBuilder` module.
///
/// This function creates a module with a
/// given name and adds classes to it.
///
/// # Errors
///
/// * Cannot create module by any reason.
/// * Cannot add class by some reason.
pub fn setup_module(_py: Python<'_>, module: &PyModule) -> PyResult<()> {
    module.add_class::<Select>()?;
    module.add_class::<Insert>()?;
    module.add_class::<Delete>()?;
    module.add_class::<Update>()?;
    Ok(())
}
