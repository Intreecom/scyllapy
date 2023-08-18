use std::future::Future;

use pyo3::{IntoPy, PyAny, PyObject, Python};

pub fn anyhow_py_future<F, T>(py: Python, fut: F) -> anyhow::Result<&PyAny>
where
    F: Future<Output = anyhow::Result<T>> + Send + 'static,
    T: IntoPy<PyObject>,
{
    let res = pyo3_asyncio::tokio::future_into_py(py, async { fut.await.map_err(Into::into) })
        .map(Into::into)?;
    Ok(res)
}
