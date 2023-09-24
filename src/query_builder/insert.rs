use pyo3::{pyclass, pymethods, types::PyDict, PyAny, PyRefMut, Python};
use scylla::query::Query;

use crate::{
    batches::ScyllaPyInlineBatch,
    exceptions::rust_err::{ScyllaPyError, ScyllaPyResult},
    queries::ScyllaPyRequestParams,
    scylla_cls::Scylla,
    utils::{py_to_value, ScyllaPyCQLDTO},
};
use scylla::frame::value::SerializedValues;

use super::utils::{pretty_build, Timeout};

#[pyclass]
#[derive(Clone, Debug, Default)]
pub struct Insert {
    table_: String,
    if_not_exists_: bool,
    names_: Vec<String>,
    values_: Vec<ScyllaPyCQLDTO>,

    timeout_: Option<Timeout>,
    ttl_: Option<i32>,
    timestamp_: Option<u64>,

    request_params_: ScyllaPyRequestParams,
}

impl Insert {
    /// Build a statement.
    ///
    /// # Errors
    /// If no values was set.
    pub fn build_query(&self) -> ScyllaPyResult<String> {
        if self.names_.is_empty() {
            return Err(ScyllaPyError::QueryBuilderError(
                "`set` method should be called at least one time",
            ));
        }
        let names = self.names_.join(",");
        let values = self
            .names_
            .iter()
            .map(|_| "?")
            .collect::<Vec<&str>>()
            .join(",");
        let names_values = format!("({names}) VALUES ({values})");
        let ifnexist = if self.if_not_exists_ {
            "IF NOT EXISTS"
        } else {
            ""
        };
        let params = vec![
            self.timestamp_
                .map(|timestamp| format!("TIMESTAMP {timestamp}")),
            self.ttl_.map(|ttl| format!("TTL {ttl}")),
            self.timeout_.as_ref().map(|timeout| match timeout {
                Timeout::Int(int) => format!("TIMEOUT {int}"),
                Timeout::Str(string) => format!("TIMEOUT {string}"),
            }),
        ];
        let prepared_params = params
            .iter()
            .map(|item| item.as_ref().map_or("", String::as_str))
            .filter(|item| !item.is_empty())
            .collect::<Vec<_>>();
        let usings = if prepared_params.is_empty() {
            String::new()
        } else {
            format!("USING {}", prepared_params.join(" AND "))
        };

        Ok(pretty_build([
            "INSERT INTO",
            self.table_.as_str(),
            names_values.as_str(),
            ifnexist,
            usings.as_str(),
        ]))
    }
}

#[pymethods]
impl Insert {
    #[new]
    #[must_use]
    pub fn py_new(table: String) -> Self {
        Self {
            table_: table,
            ..Default::default()
        }
    }

    #[must_use]
    pub fn if_not_exists(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.if_not_exists_ = true;
        slf
    }

    /// Set value to column.
    ///
    /// # Errors
    ///
    /// If value cannot be translated
    /// into `Rust` type.
    pub fn set<'a>(
        mut slf: PyRefMut<'a, Self>,
        name: String,
        value: &'a PyAny,
    ) -> ScyllaPyResult<PyRefMut<'a, Self>> {
        slf.names_.push(name);
        // Small optimization to speedup inserts.
        if value.is_none() {
            slf.values_.push(ScyllaPyCQLDTO::Unset);
        } else {
            slf.values_.push(py_to_value(value)?);
        }
        Ok(slf)
    }

    #[must_use]
    pub fn timeout(mut slf: PyRefMut<'_, Self>, timeout: Timeout) -> PyRefMut<'_, Self> {
        slf.timeout_ = Some(timeout);
        slf
    }

    #[must_use]
    pub fn timestamp(mut slf: PyRefMut<'_, Self>, timestamp: u64) -> PyRefMut<'_, Self> {
        slf.timestamp_ = Some(timestamp);
        slf
    }

    #[must_use]
    pub fn ttl(mut slf: PyRefMut<'_, Self>, ttl: i32) -> PyRefMut<'_, Self> {
        slf.ttl_ = Some(ttl);
        slf
    }

    /// Add parameters to the request.
    ///
    /// These parameters are used by scylla.
    ///
    /// # Errors
    ///
    /// May return an error, if request parameters
    /// cannot be built.
    #[pyo3(signature = (**params))]
    pub fn request_params<'a>(
        mut slf: PyRefMut<'a, Self>,
        params: Option<&'a PyDict>,
    ) -> ScyllaPyResult<PyRefMut<'a, Self>> {
        slf.request_params_ = ScyllaPyRequestParams::from_dict(params)?;
        Ok(slf)
    }

    /// Execute a query.
    ///
    /// This function is used to execute built query.
    ///
    /// # Errors
    ///
    /// If query cannot be built.
    /// Also proxies errors from `native_execute`.
    pub fn execute<'a>(&'a self, py: Python<'a>, scylla: &'a Scylla) -> ScyllaPyResult<&'a PyAny> {
        let mut query = Query::new(self.build_query()?);
        self.request_params_.apply_to_query(&mut query);
        scylla.native_execute(py, Some(query), None, self.values_.clone(), false)
    }

    /// Add to batch
    ///
    /// Adds current query to batch.
    ///
    /// # Error
    ///
    /// May result into error if query cannot be build.
    /// Or values cannot be passed to batch.
    pub fn add_to_batch(&self, batch: &mut ScyllaPyInlineBatch) -> ScyllaPyResult<()> {
        let mut query = Query::new(self.build_query()?);
        self.request_params_.apply_to_query(&mut query);

        let mut serialized = SerializedValues::new();
        for val in self.values_.clone() {
            serialized.add_value(&val)?;
        }
        batch.add_query_inner(query, serialized);
        Ok(())
    }

    #[must_use]
    pub fn __repr__(&self) -> String {
        format!("{self:?}")
    }

    /// Returns string part of a query.
    ///
    /// # Errors
    /// If cannot construct query.
    pub fn __str__(&self) -> ScyllaPyResult<String> {
        self.build_query()
    }

    #[must_use]
    pub fn __copy__(&self) -> Self {
        self.clone()
    }

    #[must_use]
    pub fn __deepcopy__(&self, _memo: &PyDict) -> Self {
        self.clone()
    }
}
