use std::{collections::HashMap, sync::Arc};

use openssl::{
    ssl::{SslContextBuilder, SslMethod, SslVerifyMode},
    x509::X509,
};
use pyo3::{
    exceptions::PyValueError, pyclass, pymethods, types::PyDict, PyAny, PyErr, PyObject, PyResult,
    Python, ToPyObject,
};
use scylla::query::Query;

use crate::utils::{anyhow_py_future, cql_to_py, py_to_cql_value};

#[pyclass]
pub struct Scylla {
    contact_points: Vec<String>,
    username: Option<String>,
    password: Option<String>,
    keyspace: Option<String>,
    ssl_cert: Option<String>,
    scylla_session: Arc<tokio::sync::RwLock<Option<scylla::Session>>>,
}

#[pymethods]
impl Scylla {
    #[new]
    #[pyo3(signature = (
        contact_points, 
        username = None, 
        password = None, 
        keyspace = None, 
        ssl_cert = None,
    ))]
    pub fn py_new(
        contact_points: Vec<String>,
        username: Option<String>,
        password: Option<String>,
        keyspace: Option<String>,
        ssl_cert: Option<String>,
    ) -> PyResult<Self> {
        Ok(Scylla {
            contact_points,
            username,
            password,
            ssl_cert,
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
        if let Some(cert_data) = self.ssl_cert.clone() {
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
            let mut session_builder = scylla::SessionBuilder::new().ssl_context(ssl_context);
            log::debug!("Adding known contact points.");
            for known_node in contact_points {
                session_builder = session_builder.known_node(known_node);
            }
            match (username, password) {
                (Some(user), Some(pass)) => session_builder = session_builder.user(user, pass),
                (None, None) => {}
                _ => {
                    return Err(anyhow::anyhow!(
                        "Cannot use username without a password and vice versa."
                    ));
                }
            }
            if let Some(keyspace) = keyspace {
                session_builder = session_builder.use_keyspace(keyspace, true);
            }

            let mut session_guard = scylla_session.write().await;
            *session_guard = Some(session_builder.build().await?);
            Ok(())
        })
        .map_err(Into::into)
    }

    pub fn shutdown<'a>(&'a self, py: Python<'a>) -> anyhow::Result<&'a PyAny> {
        let session = self.scylla_session.clone();
        anyhow_py_future(py, async move {
            let mut guard = session.write().await;
            log::debug!("Shutting down session.");
            if guard.is_none() {
                return Err(anyhow::anyhow!("The session is not initialized."));
            }
            guard.take();
            Ok(())
        })
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
                        for (index, col) in row.columns.into_iter().enumerate() {
                            map.insert(
                                specs[index].name.as_str(),
                                cql_to_py(py, &specs[index].typ, col)?,
                            );
                        }
                        dumped_rows.push(map);
                    }
                    if let Some(parser) = as_class {
                        let mut result = Vec::new();
                        for row in dumped_rows {
                            result.push(parser.call(
                                py,
                                (),
                                Some(row.to_object(py).downcast::<PyDict>(py).map_err(|_| {
                                    anyhow::anyhow!("Cannot prepare data for calling function")
                                })?),
                            )?);
                        }
                        return Ok(Some(result.to_object(py)));
                    }
                    Ok(Some(dumped_rows.to_object(py)))
                })
            } else {
                Ok(None)
            }
        })
        .map_err(Into::into)
    }
}
