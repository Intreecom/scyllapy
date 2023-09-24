use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use crate::{
    exceptions::rust_err::{ScyllaPyError, ScyllaPyResult},
    execution_profiles::ScyllaPyExecutionProfile,
    inputs::{BatchInput, ExecuteInput, PrepareInput},
    prepared_queries::ScyllaPyPreparedQuery,
    query_results::{ScyllaPyIterableQueryResult, ScyllaPyQueryResult, ScyllaPyQueryReturns},
    utils::{parse_python_query_params, scyllapy_future},
};
use openssl::{
    ssl::{SslContextBuilder, SslMethod, SslVerifyMode},
    x509::X509,
};
use pyo3::{pyclass, pymethods, PyAny, Python};
use scylla::{frame::value::ValueList, prepared_statement::PreparedStatement, query::Query};

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
    default_execution_profile: Option<ScyllaPyExecutionProfile>,
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
        query: Option<impl Into<Query> + Send + 'static>,
        prepared: Option<PreparedStatement>,
        values: impl ValueList + Send + 'static,
        paged: bool,
    ) -> ScyllaPyResult<&'a PyAny> {
        let session_arc = self.scylla_session.clone();
        scyllapy_future(py, async move {
            let session_guard = session_arc.read().await;
            let session = session_guard.as_ref().ok_or(ScyllaPyError::SessionError(
                "Session is not initialized.".into(),
            ))?;
            // let res = session.query(query, values).await?;
            if paged {
                match (query, prepared) {
                    (Some(query), None) => Ok(ScyllaPyQueryReturns::IterableQueryResult(
                        ScyllaPyIterableQueryResult::new(session.query_iter(query, values).await?),
                    )),
                    (None, Some(prepared)) => Ok(ScyllaPyQueryReturns::IterableQueryResult(
                        ScyllaPyIterableQueryResult::new(
                            session.execute_iter(prepared, values).await?,
                        ),
                    )),
                    _ => Err(ScyllaPyError::SessionError(
                        "You should pass either query or prepared query.".into(),
                    )),
                }
            } else {
                match (query, prepared) {
                    (Some(query), None) => Ok(ScyllaPyQueryReturns::QueryResult(
                        ScyllaPyQueryResult::new(session.query(query, values).await?),
                    )),
                    (None, Some(prepared)) => Ok(ScyllaPyQueryReturns::QueryResult(
                        ScyllaPyQueryResult::new(session.execute(&prepared, values).await?),
                    )),
                    _ => Err(ScyllaPyError::SessionError(
                        "You should pass either query or prepared query.".into(),
                    )),
                }
            }
        })
        .map_err(Into::into)
    }
}

#[pymethods]
impl Scylla {
    #[new]
    #[pyo3(signature = (
        contact_points,
        *,
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
        default_execution_profile = None,
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
        default_execution_profile: Option<ScyllaPyExecutionProfile>,
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
            default_execution_profile,
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
    pub fn startup<'a>(&'a self, py: Python<'a>) -> ScyllaPyResult<&'a PyAny> {
        let contact_points = self.contact_points.clone();
        let username = self.username.clone();
        let password = self.password.clone();
        let mut ssl_context = None;
        if let Some(cert_data) = self.ssl_cert.clone() {
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
        let default_execution_profile = self.default_execution_profile.clone();
        scyllapy_future(py, async move {
            if scylla_session.read().await.is_some() {
                return Err(ScyllaPyError::SessionError(
                    "Session already initialized.".into(),
                ));
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
            if let Some(execution_prof) = default_execution_profile {
                session_builder =
                    session_builder.default_execution_profile_handle(execution_prof.into());
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
                    return Err(ScyllaPyError::SessionError(
                        "Cannot use username without a password and vice versa.".into(),
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
    }

    /// Close current session, free resources.
    ///
    /// # Errors
    ///
    /// Returns error if session wasn't initialized before
    /// calling this method.
    pub fn shutdown<'a>(&'a self, py: Python<'a>) -> ScyllaPyResult<&'a PyAny> {
        let session = self.scylla_session.clone();
        scyllapy_future(py, async move {
            let mut guard = session.write().await;
            if guard.is_none() {
                return Err(ScyllaPyError::SessionError(
                    "The session is not initialized.".into(),
                ));
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
    #[pyo3(signature = (query, params = None, *, paged = false))]
    pub fn execute<'a>(
        &'a self,
        py: Python<'a>,
        query: ExecuteInput,
        params: Option<&'a PyAny>,
        paged: bool,
    ) -> ScyllaPyResult<&'a PyAny> {
        // We need to prepare parameter we're going to use
        // in query.
        let query_params = parse_python_query_params(params, true)?;
        // We need this clone, to safely share the session between threads.
        let (query, prepared) = match query {
            ExecuteInput::Text(txt) => (Some(Query::new(txt)), None),
            ExecuteInput::Query(query) => (Some(Query::from(query)), None),
            ExecuteInput::PreparedQuery(prep) => (None, Some(PreparedStatement::from(prep))),
        };
        self.native_execute(py, query, prepared, query_params, paged)
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
    ) -> ScyllaPyResult<&'a PyAny> {
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
        scyllapy_future(py, async move {
            let session_guard = session_arc.read().await;
            let session = session_guard.as_ref().ok_or(ScyllaPyError::SessionError(
                "Session is not initialized.".into(),
            ))?;
            let res = session.batch(&batch, batch_params).await?;
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
    ) -> ScyllaPyResult<&'a PyAny> {
        let session_arc = self.scylla_session.clone();
        scyllapy_future(python, async move {
            let cql_query = Query::from(query);
            let session_guard = session_arc.read().await;
            let session = session_guard.as_ref().ok_or(ScyllaPyError::SessionError(
                "Session is not initialized.".into(),
            ))?;
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
    ) -> ScyllaPyResult<&'a PyAny> {
        let session_arc = self.scylla_session.clone();
        scyllapy_future(python, async move {
            let guard = session_arc.write().await;
            let session = guard.as_ref().ok_or(ScyllaPyError::SessionError(
                "Session is not initialized.".into(),
            ))?;
            session.use_keyspace(keyspace, true).await?;
            Ok(())
        })
    }

    /// Get current keyspace.
    ///
    /// # Errors
    /// May return an error, if
    /// sessions was not initialized.
    pub fn get_keyspace<'a>(&'a self, python: Python<'a>) -> ScyllaPyResult<&'a PyAny> {
        let session_arc = self.scylla_session.clone();
        scyllapy_future(python, async move {
            let guard = session_arc.write().await;
            let session = guard.as_ref().ok_or(ScyllaPyError::SessionError(
                "Session is not initialized.".into(),
            ))?;
            let keyspace = session.get_keyspace().map(|ks| (*ks).clone());
            Ok(keyspace)
        })
    }
}
