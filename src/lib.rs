#![deny(
    // Base lints.
    clippy::all,
    // Some pedantic lints.
    clippy::pedantic,
    // New lints which are cool.
    clippy::nursery,
)]
#![
    allow(
        // Beacsue some types naming conventions are
        // just different to the ones that rust have.
        non_camel_case_types,
        // I don't care about this.
        clippy::module_name_repetitions,
        // Yo, the hell you should put
        // it in docs, if signature is clear as sky.
        clippy::missing_errors_doc,
        // Because pythonic way is
        // to have many args with defaults.
        clippy::too_many_arguments
)]

pub mod batches;
pub mod consistencies;
pub mod exceptions;
pub mod execution_profiles;
pub mod extra_types;
pub mod inputs;
pub mod load_balancing;
pub mod prepared_queries;
pub mod queries;
pub mod query_builder;
pub mod query_results;
pub mod scylla_cls;
pub mod utils;

use pyo3::{
    pymodule,
    types::{PyModule, PyModuleMethods},
    Bound, PyResult, Python,
};

use crate::utils::add_submodule;

#[pymodule]
#[pyo3(name = "_internal")]
fn _internal(py: Python<'_>, pymod: &Bound<'_, PyModule>) -> PyResult<()> {
    pyo3_log::init();
    pymod.add_class::<scylla_cls::Scylla>()?;
    pymod.add_class::<scylla_cls::ScyllaPySSLVerifyMode>()?;
    pymod.add_class::<consistencies::ScyllaPyConsistency>()?;
    pymod.add_class::<consistencies::ScyllaPySerialConsistency>()?;
    pymod.add_class::<queries::ScyllaPyQuery>()?;
    pymod.add_class::<prepared_queries::ScyllaPyPreparedQuery>()?;
    pymod.add_class::<batches::ScyllaPyBatch>()?;
    pymod.add_class::<batches::ScyllaPyBatchType>()?;
    pymod.add_class::<batches::ScyllaPyInlineBatch>()?;
    pymod.add_class::<query_results::ScyllaPyQueryResult>()?;
    pymod.add_class::<execution_profiles::ScyllaPyExecutionProfile>()?;
    add_submodule(py, pymod, "extra_types", extra_types::setup_module)?;
    add_submodule(py, pymod, "query_builder", query_builder::setup_module)?;
    add_submodule(py, pymod, "exceptions", exceptions::py_err::setup_module)?;
    add_submodule(py, pymod, "load_balancing", load_balancing::setup_module)?;
    Ok(())
}
