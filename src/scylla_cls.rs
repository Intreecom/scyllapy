use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use crate::{
    inputs::{BatchInput, ExecuteInput, PrepareInput},
    prepared_queries::ScyllaPyPreparedQuery,
    query_results::ScyllaPyQueryResult,
    utils::{anyhow_py_future, parse_python_query_params},
};
use openssl::{
    ssl::{SslContextBuilder, SslMethod, SslVerifyMode},
    x509::X509,
};
use pyo3::{pyclass, pymethods, PyAny, Python};
use scylla::{frame::value::ValueList, query::Query};

#[pyclass(frozen, weakref)]
#[derive(Clone)]
pub struct Scylla {
    contact_points: Vec<String>,
    username: Option<String>,
    password: Option<String>,
    keyspace: Option<String>,
    ssl_cert: Option<String>,
    connection_timeout: Option<u64>,
    write_coalescing: Option<bool>,
    disallow_shard_aware_port: Option<bool>,
    pool_size_per_host: Option<NonZeroUsize>,
    pool_size_per_shard: Option<NonZeroUsize>,
    keepalive_interval: Option<u64>,
    keepalive_timeout: Option<u64>,
    tcp_keepalive_interval: Option<u64>,
    tcp_nodelay: Option<bool>,
    scylla_session: Arc<tokio::sync::RwLock<Option<scylla::Session>>>,
}

impl Scylla {
    /// Execute a query.
    ///
    /// This function is not exposed to python
    /// and used to execute queries from rust code.
    ///
    /// The main reason of using separate method is
    /// an ability to use generic parameters in this function.
    ///
    /// # Errors
    ///
    /// May raise an error if driver
    /// fails to execute query.
    pub fn native_execute<'a>(
        &'a self,
        py: Python<'a>,
        query: impl Into<Query> + Send + 'static,
        values: impl ValueList + Send + 'static,
    ) -> anyhow::Result<&'a PyAny> {
        let session_arc = self.scylla_session.clone();
        anyhow_py_future(py, async move {
            let session_guard = session_arc.read().await;
            let session = session_guard
                .as_ref()
                .ok_or(anyhow::anyhow!("Session is not initialized."))?;
            let res = session.query(query, values).await?;
            log::debug!("Query executed!");
            Ok(ScyllaPyQueryResult::new(res))
        })
        .map_err(Into::into)
    }
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
        write_coalescing = None,
        pool_size_per_host = None,
        pool_size_per_shard = None,
        keepalive_interval = None,
        keepalive_timeout= None,
        tcp_keepalive_interval = None,
        tcp_nodelay = None,
        disallow_shard_aware_port = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn py_new(
        contact_points: Vec<String>,
        username: Option<String>,
        password: Option<String>,
        keyspace: Option<String>,
        ssl_cert: Option<String>,
        connection_timeout: Option<u64>,
        write_coalescing: Option<bool>,
        pool_size_per_host: Option<NonZeroUsize>,
        pool_size_per_shard: Option<NonZeroUsize>,
        keepalive_interval: Option<u64>,
        keepalive_timeout: Option<u64>,
        tcp_keepalive_interval: Option<u64>,
        tcp_nodelay: Option<bool>,
        disallow_shard_aware_port: Option<bool>,
    ) -> Self {
        Scylla {
            contact_points,
            username,
            password,
            ssl_cert,
            keyspace,
            connection_timeout,
            write_coalescing,
            disallow_shard_aware_port,
            pool_size_per_host,
            pool_size_per_shard,
            keepalive_interval,
            keepalive_timeout,
            tcp_keepalive_interval,
            tcp_nodelay,
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
        let write_coalescing = self.write_coalescing;
        let disallow_shard_aware_port = self.disallow_shard_aware_port;
        let pool_size_per_host = self.pool_size_per_host;
        let pool_size_per_shard = self.pool_size_per_shard;
        let keepalive_interval = self.keepalive_interval;
        let keepalive_timeout = self.keepalive_timeout;
        let tcp_keepalive_interval = self.tcp_keepalive_interval;
        let tcp_nodelay = self.tcp_nodelay;
        anyhow_py_future(py, async move {
            if scylla_session.read().await.is_some() {
                return Err(anyhow::anyhow!("Session already initialized."));
            }
            let mut session_builder = scylla::SessionBuilder::new()
                .ssl_context(ssl_context)
                .known_nodes(contact_points);
            if let Some(write_coalescing) = write_coalescing {
                session_builder = session_builder.write_coalescing(write_coalescing);
            }
            if let Some(disallow) = disallow_shard_aware_port {
                session_builder = session_builder.disallow_shard_aware_port(disallow);
            }
            if let Some(pool_per_host) = pool_size_per_host {
                session_builder = session_builder
                    .pool_size(scylla::transport::session::PoolSize::PerHost(pool_per_host));
            } else if let Some(pool_size_per_shard) = pool_size_per_shard {
                session_builder = session_builder.pool_size(
                    scylla::transport::session::PoolSize::PerShard(pool_size_per_shard),
                );
            }
            if let Some(inter) = keepalive_interval {
                session_builder = session_builder.keepalive_interval(Duration::from_secs(inter));
            }
            if let Some(timeout) = keepalive_timeout {
                session_builder = session_builder.keepalive_timeout(Duration::from_secs(timeout));
            }
            if let Some(inter) = tcp_keepalive_interval {
                session_builder =
                    session_builder.tcp_keepalive_interval(Duration::from_secs(inter));
            }
            if let Some(tcp_nodelay) = tcp_nodelay {
                session_builder = session_builder.tcp_nodelay(tcp_nodelay);
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
        params: Option<&'a PyAny>,
    ) -> anyhow::Result<&'a PyAny> {
        // We need to prepare parameter we're going to use
        // in query.
        let query_params = parse_python_query_params(params, true)?;
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
        batch: BatchInput,
        params: Option<Vec<&'a PyAny>>,
    ) -> anyhow::Result<&'a PyAny> {
        // We need to prepare parameter we're going to use
        // in query.
        // If parameters were passed, we parse python values,
        // to corresponding CQL values.

        let (batch, batch_params) = match batch {
            BatchInput::Batch(batch) => {
                let mut batch_params = Vec::new();
                if let Some(passed_params) = params {
                    for query_params in passed_params {
                        batch_params.push(parse_python_query_params(Some(query_params), false)?);
                    }
                }
                (batch.into(), batch_params)
            }
            BatchInput::InlineBatch(inline) => inline.into(),
        };
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

    /// Set keyspace to all connections.
    ///
    /// # Errors
    /// May return an error, if
    /// sessions was not initialized.
    pub fn use_keyspace<'a>(
        &'a self,
        python: Python<'a>,
        keyspace: String,
    ) -> anyhow::Result<&'a PyAny> {
        let session_arc = self.scylla_session.clone();
        anyhow_py_future(python, async move {
            let guard = session_arc.write().await;
            let session = guard
                .as_ref()
                .ok_or(anyhow::anyhow!("Session is not initialized."))?;
            session.use_keyspace(keyspace, true).await?;
            Ok(())
        })
    }

    /// Get current keyspace.
    ///
    /// # Errors
    /// May return an error, if
    /// sessions was not initialized.
    pub fn get_keyspace<'a>(&'a self, python: Python<'a>) -> anyhow::Result<&'a PyAny> {
        let session_arc = self.scylla_session.clone();
        anyhow_py_future(python, async move {
            let guard = session_arc.write().await;
            let session = guard
                .as_ref()
                .ok_or(anyhow::anyhow!("Session is not initialized."))?;
            let keyspace = session.get_keyspace().map(|ks| (*ks).clone());
            Ok(keyspace)
        })
    }
}
