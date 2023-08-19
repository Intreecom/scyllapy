mod dtos;
mod utils;

use std::{collections::HashMap, sync::Arc};

use openssl::{
    ssl::{SslContextBuilder, SslMethod, SslVerifyMode},
    x509::X509,
};
use pyo3::{
    exceptions::PyValueError, pyclass, pymethods, pymodule, types::PyModule, IntoPy, Py, PyAny,
    PyErr, PyObject, PyResult, Python, ToPyObject,
};
use scylla::{_macro_internal::ValueList, query::Query, QueryResult};
use utils::{cql_to_py, py_to_cql_value};

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

    #[pyo3(signature = (query, params = None, as_class = None))]
    pub fn execute<'a>(
        &'a self,
        py: Python<'a>,
        query: String,
        params: Option<&'a PyAny>,
        as_class: Option<PyObject>,
    ) -> anyhow::Result<&'a PyAny> {
        let mut query_params = Vec::new();
        if let Some(passed_params) = params {
            for item in passed_params.iter()? {
                query_params.push(py_to_cql_value(item?)?);
            }
        }
        let session_arc = self.scylla_session.clone();
        anyhow_py_future(py, async move {
            let session_guard = session_arc.read().await;
            if session_guard.is_none() {
                return Err(PyValueError::new_err("Session is not initialized.").into());
            }
            let session = session_guard.as_ref().unwrap();
            let cql_query = Query::new(query);
            let res = session.query(cql_query, query_params).await?;
            log::debug!("Query executed!");
            let specs = res.col_specs.clone();
            if let Ok(rows) = res.rows() {
                Python::with_gil(|py| {
                    let mut dumped_rows = Vec::new();
                    for row in rows {
                        let mut map = HashMap::new();
                        let mut index = 0;
                        for col in row.columns {
                            map.insert(
                                specs[index].name.as_str(),
                                cql_to_py(py, &specs[index].typ, col)?,
                            );
                            index += 1;
                        }
                        dumped_rows.push(map);
                    }
                    Ok(Some(dumped_rows.to_object(py)))
                })
            } else {
                Ok(None)
            }
            // as_class.unwrap().call1(py, (1,));
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
