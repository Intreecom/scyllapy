use pyo3::{create_exception, types::PyModule, PyResult, Python};

create_exception!(
    scyllapy.exceptions,
    ScyllaPyBaseError,
    pyo3::exceptions::PyException
);
create_exception!(scyllapy.exceptions, ScyllaPyBindingError, ScyllaPyBaseError);
create_exception!(scyllapy.exceptions, ScyllaPyDBError, ScyllaPyBaseError);
create_exception!(scyllapy.exceptions, ScyllaPySessionError, ScyllaPyDBError);
create_exception!(scyllapy.exceptions, ScyllaPyMappingError, ScyllaPyBaseError);
create_exception!(
    scyllapy.exceptions,
    ScyllaPyQueryBuiderError,
    ScyllaPyBaseError
);

/// Create module with exceptions.
///
/// This method adds custom exceptions
/// to scyllapy python module.
///
/// # Errors
///
/// May throw an error, if module cannot be constructed.
pub fn setup_module(py: Python<'_>, module: &PyModule) -> PyResult<()> {
    module.add("ScyllaPyBaseError", py.get_type::<ScyllaPyBaseError>())?;
    module.add("ScyllaPyDBError", py.get_type::<ScyllaPyDBError>())?;
    module.add(
        "ScyllaPySessionError",
        py.get_type::<ScyllaPySessionError>(),
    )?;
    module.add(
        "ScyllaPyBindingError",
        py.get_type::<ScyllaPyBindingError>(),
    )?;
    module.add(
        "ScyllaPyMappingError",
        py.get_type::<ScyllaPyMappingError>(),
    )?;
    module.add(
        "ScyllaPyQueryBuiderError",
        py.get_type::<ScyllaPyQueryBuiderError>(),
    )?;
    Ok(())
}
