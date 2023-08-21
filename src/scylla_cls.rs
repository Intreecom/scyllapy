use std::{sync::Arc, time::Duration};

use openssl::{
    ssl::{SslContextBuilder, SslMethod, SslVerifyMode},
    x509::X509,
};
use pyo3::{pyclass, pymethods, PyAny, Python};
use scylla::{_macro_internal::SerializedValues, batch::Batch, query::Query};

use crate::{
    batches::ScyllaPyBatch,
    inputs::{ExecuteInput, PrepareInput},
    prepared_queries::ScyllaPyPreparedQuery,
    query_results::ScyllaPyQueryResult,
    utils::{anyhow_py_future, py_to_value},
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
    #[pyo3(signature = (query, params = None))]
    pub fn execute<'a>(
        &'a self,
        py: Python<'a>,
        query: ExecuteInput,
        params: Option<Vec<&'a PyAny>>,
    ) -> anyhow::Result<&'a PyAny> {
        // We need to prepare parameter we're going to use
        // in query.
        let mut query_params = SerializedValues::new();
        // If parameters were passed, we parse python values,
        // to corresponding CQL values.
        if let Some(passed_params) = params {
            for param in passed_params {
                query_params.add_value(&py_to_value(param)?)?;
            }
        }
        // We need this clone, to safely share the session between threads.
        let session_arc = self.scylla_session.clone();
        anyhow_py_future(py, async move {
            let session_guard = session_arc.read().await;
            let session = session_guard
                .as_ref()
                .ok_or(anyhow::anyhow!("Session is not initialized."))?;
            let res = match query {
                ExecuteInput::Text(text) => session.query(text, query_params).await?,
                ExecuteInput::Query(query) => session.query(query, query_params).await?,
                ExecuteInput::PreparedQuery(prepared) => {
                    session.execute(&prepared.into(), query_params).await?
                }
            };
            log::debug!("Query executed!");
            Ok(ScyllaPyQueryResult::new(res))
        })
        .map_err(Into::into)
    }

    /// Execute a batch statement.
    ///
    /// This function takes a batch and list of lists of params.
    #[pyo3(signature = (batch, params = None))]
    pub fn batch<'a>(
        &'a self,
        py: Python<'a>,
        batch: ScyllaPyBatch,
        params: Option<Vec<&'a PyAny>>,
    ) -> anyhow::Result<&'a PyAny> {
        // We need to prepare parameter we're going to use
        // in query.
        let mut batch_params = Vec::new();
        // If parameters were passed, we parse python values,
        // to corresponding CQL values.
        if let Some(passed_params) = params {
            for query_params in passed_params {
                let mut query_serialized = SerializedValues::new();
                for param in query_params.iter()? {
                    query_serialized.add_value(&py_to_value(param?)?)?;
                }
                batch_params.push(query_serialized);
            }
        }
        let batch = Batch::from(batch);
        // We need this clone, to safely share the session between threads.
        let session_arc = self.scylla_session.clone();
        anyhow_py_future(py, async move {
            let session_guard = session_arc.read().await;
            let session = session_guard
                .as_ref()
                .ok_or(anyhow::anyhow!("Session is not initialized."))?;
            let res = session.batch(&batch, batch_params).await?;
            log::debug!("Query executed!");
            Ok(ScyllaPyQueryResult::new(res))
        })
        .map_err(Into::into)
    }

    /// Prepare a query.
    ///
    /// This function takes a query to prepare
    /// and sends it to server.
    ///
    /// After preparation it returns a prepared
    /// query, that you can use later.
    pub fn prepare<'a>(
        &'a self,
        python: Python<'a>,
        query: PrepareInput,
    ) -> anyhow::Result<&'a PyAny> {
        let session_arc = self.scylla_session.clone();
        anyhow_py_future(python, async move {
            let cql_query = Query::from(query);
            let session_guard = session_arc.read().await;
            let session = session_guard
                .as_ref()
                .ok_or(anyhow::anyhow!("Session is not initialized."))?;
            let prepared = session.prepare(cql_query).await?;
            Ok(ScyllaPyPreparedQuery::from(prepared))
        })
    }
}
