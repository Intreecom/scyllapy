mod dtos;
mod utils;

use std::{any, collections::HashMap, sync::Arc, time::Duration};

use dtos::InboxDTO;
use openssl::{
    ssl::{SslContextBuilder, SslMethod, SslVerifyMode},
    x509::X509,
};
use pyo3::{
    exceptions::PyValueError,
    pyclass, pymethods, pymodule,
    types::{PyList, PyModule, PyIterator},
    IntoPy, Py, PyAny, PyErr, PyObject, PyRef, PyResult, Python,
};
use scylla::{_macro_internal::ValueList, query::Query, QueryResult};

use crate::utils::anyhow_py_future;

#[pyclass]
struct ScyllaDAOs {
    contact_points: Vec<String>,
    username: String,
    password: String,
    keyspace: String,
    cert_data: Option<String>,
    scylla_session: Arc<tokio::sync::RwLock<Option<scylla::Session>>>,
}

impl ScyllaDAOs {
    pub fn _exec<'a, Q, V, CB, RT>(
        &'a self,
        py: Python<'a>,
        query: Q,
        values: V,
        callback: CB,
    ) -> anyhow::Result<&'a PyAny>
    where
        Q: Into<Query> + Send + 'static,
        V: ValueList + Send + 'static,
        CB: Fn(QueryResult) -> anyhow::Result<RT> + Send + 'static,
        RT: IntoPy<Py<PyAny>>,
    {
        let session_arc = self.scylla_session.clone();
        anyhow_py_future(py, async move {
            let session_guard = session_arc.read().await;
            if session_guard.is_none() {
                return Err(PyValueError::new_err("Session is not initialized.").into());
            }
            let session = session_guard.as_ref().unwrap();
            let res = session.query(query.into(), values).await?;
            log::debug!("Query executed!");
            callback(res).into()
        })
        .map_err(Into::into)
    }
}

#[pymethods]
impl ScyllaDAOs {
    #[new]
    #[pyo3(signature = (contact_points, username, password, keyspace, cert_data = None))]
    pub fn py_new(
        contact_points: Vec<String>,
        username: String,
        password: String,
        keyspace: String,
        cert_data: Option<String>,
    ) -> PyResult<Self> {
        Ok(ScyllaDAOs {
            contact_points,
            username,
            password,
            cert_data,
            keyspace,
            scylla_session: Arc::new(tokio::sync::RwLock::new(None)),
        })
    }

    pub fn asleep(self_: PyRef<'_, Self>, secs: u64) -> PyResult<&PyAny> {
        pyo3_asyncio::tokio::future_into_py(self_.py(), async move {
            tokio::time::sleep(Duration::from_secs(secs)).await;
            Ok(())
        })
    }

    pub fn startup<'a>(&'a self, py: Python<'a>) -> anyhow::Result<&'a PyAny> {
        log::debug!("Initializing scylla pool.");
        let contact_points = self.contact_points.clone();
        let username = self.username.clone();
        let password = self.password.clone();
        let mut ssl_context = None;
        if let Some(cert_data) = self.cert_data.clone() {
            log::debug!("Preparing SSL Context");
            let mut ssl_context_builder = SslContextBuilder::new(SslMethod::tls())?;
            let pem = X509::from_pem(cert_data.as_bytes())?;
            ssl_context_builder.set_certificate(&pem)?;
            ssl_context_builder.set_verify(SslVerifyMode::NONE);
            ssl_context = Some(ssl_context_builder.build());
        }
        let keyspace = self.keyspace.clone();
        let scylla_session = self.scylla_session.clone();
        anyhow_py_future(py, async move {
            if scylla_session.read().await.is_some() {
                return Err(PyErr::new::<PyValueError, _>("meme").into());
            }
            let mut session_builder = scylla::SessionBuilder::new();
            log::debug!("Adding known contact points.");
            for known_node in contact_points {
                session_builder = session_builder.known_node(known_node);
            }
            let session = session_builder
                .user(username, password)
                .ssl_context(ssl_context)
                .use_keyspace(keyspace, true)
                .build()
                .await?;
            let mut session_guard = scylla_session.write().await;
            *session_guard = Some(session);
            Ok(())
        })
        .map_err(Into::into)
    }

    #[pyo3(signature = (query, params = None))]
    pub fn exec_query<'a>(
        &'a self,
        py: Python<'a>,
        query: String,
        params: Option<&'a PyList>,
    ) -> anyhow::Result<&'a PyAny> {
        self._exec(py, query, &[], move |res| {
            for row in res.rows()? {
                log::info!("{:?}", row);
            }
            Ok(())
        })
    }

    fn sleep_for<'p>(&self, py: Python<'p>, secs: &'p PyAny) -> PyResult<&'p PyAny> {
        let secs = secs.extract()?;
        pyo3_asyncio::tokio::future_into_py(py, async move {
            tokio::time::sleep(Duration::from_secs(secs)).await;
            Python::with_gil(|py| Ok(py.None()))
        })
    }

    #[pyo3(signature = (query, params = None, as_class = None))]
    pub fn exec_cls<'a>(
        &'a self,
        py: Python<'a>,
        query: String,
        params: Option<&'a PyIterator>,
        as_class: Option<PyObject>,
    ) -> anyhow::Result<&'a PyAny> {
        let session_arc = self.scylla_session.clone();
        anyhow_py_future(py, async move {
            let session_guard = session_arc.read().await;
            if session_guard.is_none() {
                return Err(PyValueError::new_err("Session is not initialized.").into());
            }
            let session = session_guard.as_ref().unwrap();
            let res = session.query(query, &[]).await?;
            log::debug!("Query executed!");
            // as_class.unwrap().call1(py, (1,));
            Ok(())
        })
        .map_err(Into::into)
    }
}

#[pymodule]
#[pyo3(name = "_internal")]
fn _internal(_py: Python<'_>, pymod: &PyModule) -> PyResult<()> {
    pyo3_log::init();
    pymod.add_class::<ScyllaDAOs>()?;
    Ok(())
}
