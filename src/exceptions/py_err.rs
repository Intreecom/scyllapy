use pyo3::{create_exception, types::PyModule, PyResult, Python};

create_exception!("scyllapy", ScyllaPyBaseError, pyo3::exceptions::PyException);
create_exception!("scyllapy", ScyllaPyDBError, ScyllaPyBaseError);

/// Create module with exceptions.
///
/// This method adds custom exceptions
/// to scyllapy python module.
///
/// # Errors
///
/// May throw an error, if module cannot be constructed.
pub fn module_constructor(py: Python<'_>, module: &PyModule) -> PyResult<()> {
    module.add("ScyllaPyBaseError", py.get_type::<ScyllaPyBaseError>())?;
    module.add("ScyllaPyDBError", py.get_type::<ScyllaPyDBError>())?;
    Ok(())
}
