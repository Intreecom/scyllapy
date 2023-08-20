use std::{collections::HashMap, sync::Arc, time::Duration};

use openssl::{
    ssl::{SslContextBuilder, SslMethod, SslVerifyMode},
    x509::X509,
};
use pyo3::{
    pyclass, pymethods,
    types::{PyDict, PyList},
    PyAny, PyObject, Python, ToPyObject,
};
use scylla::{frame::types::Consistency as ScyllaConsistency, query::Query};

use crate::{
    consistency::Consistency,
    utils::{anyhow_py_future, cql_to_py, py_to_cql_value},
};

#[pyclass(frozen, weakref)]
pub struct Scylla {
    contact_points: Vec<String>,
    username: Option<String>,
    password: Option<String>,
    keyspace: Option<String>,
    ssl_cert: Option<String>,
    connection_timeout: Option<u64>,
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
        connection_timeout = None,
    ))]
    pub fn py_new(
        contact_points: Vec<String>,
        username: Option<String>,
        password: Option<String>,
        keyspace: Option<String>,
        ssl_cert: Option<String>,
        connection_timeout: Option<u64>,
    ) -> Self {
        Scylla {
            contact_points,
            username,
            password,
            ssl_cert,
            keyspace,
            connection_timeout,
            scylla_session: Arc::new(tokio::sync::RwLock::new(None)),
        }
    }

    /// Start the session.
    ///
    /// Here we create a new scylla session
    /// and save it in our structure.
    ///
    /// # Errors
    /// May return an error in several cases:
    /// * The session is already initialized;
    /// * Username passed without password and vice versa;
    /// * Cannot connect to the database.
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
        let conn_timeout = self.connection_timeout;
        anyhow_py_future(py, async move {
            if scylla_session.read().await.is_some() {
                return Err(anyhow::anyhow!("Session already initialized."));
            }
            let mut session_builder = scylla::SessionBuilder::new()
                .ssl_context(ssl_context)
                .known_nodes(contact_points);
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
            if let Some(connection_timeout) = conn_timeout {
                session_builder =
                    session_builder.connection_timeout(Duration::from_secs(connection_timeout));
            }
            let mut session_guard = scylla_session.write().await;
            *session_guard = Some(session_builder.build().await?);
            Ok(())
        })
        .map_err(Into::into)
    }

    /// Close current session, free resources.
    ///
    /// # Errors
    ///
    /// Returns error if session wasn't initialized before
    /// calling this method.
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

    /// Execute a query.
    ///
    /// This function takes a query and other parameters
    /// for performing actual request to the database.
    ///
    /// It creates a python future and executes
    /// the query, using it's `scylla_session`.
    ///
    /// # Errors
    ///
    /// Can result in an error in any case, when something goes wrong.
    #[pyo3(signature = (query, params = None, consistency = None, as_class = None))]
    pub fn execute<'a>(
        &'a self,
        py: Python<'a>,
        query: String,
        params: Option<&'a PyAny>,
        consistency: Option<Consistency>,
        as_class: Option<PyObject>,
    ) -> anyhow::Result<&'a PyAny> {
        // We need to prepare parameter we're going to use
        // in query.
        let mut query_params = Vec::new();
        // If parameters were passed, we parse python values,
        // to corresponding CQL values.
        if let Some(passed_params) = params {
            for item in passed_params.iter()? {
                query_params.push(py_to_cql_value(item?)?);
            }
        }
        // We need this clone, to safely share the session between threads.
        let session_arc = self.scylla_session.clone();
        let consistency = consistency.map(ScyllaConsistency::from);
        anyhow_py_future(py, async move {
            let session_guard = session_arc.read().await;
            let session = session_guard
                .as_ref()
                .ok_or(anyhow::anyhow!("Session is not initialized."))?;
            // We construct query, using passed query string.
            let mut cql_query = Query::new(query);
            if let Some(consistency) = consistency {
                cql_query.set_consistency(consistency);
            }
            let res = session.query(cql_query, query_params).await?;
            log::debug!("Query executed!");
            // Column specs is a class that holds information
            // about all columns returned by the query.
            let specs = res.col_specs.clone();
            if let Ok(rows) = res.rows() {
                // We need to enable GIL here,
                // because we're going to create references
                // in Python's heap memory, so everything
                // returned by the query may be accessed from python.
                Python::with_gil(|py| {
                    let mut dumped_rows = Vec::new();
                    for row in rows {
                        let mut map = HashMap::new();
                        for (index, col) in row.columns.into_iter().enumerate() {
                            map.insert(
                                specs[index].name.as_str(),
                                // Here we convert returned row to python-native type.
                                cql_to_py(py, &specs[index].typ, col)?,
                            );
                        }
                        dumped_rows.push(map);
                    }
                    let py_rows = dumped_rows.to_object(py);
                    // If user wants to use custom DTO for rows,
                    // we use call it for every row.
                    if let Some(parser) = as_class {
                        let mut result = Vec::new();
                        let py_rows_list = py_rows.downcast::<PyList>(py).map_err(|_| {
                            anyhow::anyhow!("Cannot parse returned results as list.")
                        })?;
                        for row in py_rows_list {
                            result.push(parser.call(
                                py,
                                (),
                                // Here we pass returned fields as kwargs.
                                Some(row.downcast::<PyDict>().map_err(|_| {
                                    anyhow::anyhow!("Cannot prepare data for calling function")
                                })?),
                            )?);
                        }
                        return Ok(Some(result.to_object(py)));
                    }
                    Ok(Some(py_rows))
                })
            } else {
                Ok(None)
            }
        })
        .map_err(Into::into)
    }
}
