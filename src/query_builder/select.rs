use pyo3::{
    pyclass, pymethods,
    types::{PyDict, PyTuple},
    PyAny, PyRefMut, Python,
};
use scylla::query::Query;

use crate::{
    batches::ScyllaPyInlineBatch,
    exceptions::rust_err::ScyllaPyResult,
    queries::ScyllaPyRequestParams,
    scylla_cls::Scylla,
    utils::{py_to_value, ScyllaPyCQLDTO},
};

use super::utils::{pretty_build, Timeout};
use scylla::frame::value::SerializedValues;

#[pyclass]
#[derive(Clone, Debug, Default)]
pub struct Select {
    table_: String,
    distinct_: bool,
    allow_filtering_: bool,
    bypass_cache_: bool,
    timeout_: Option<Timeout>,
    limit_: Option<i32>,
    per_partition_limit_: Option<i32>,
    order_by_: Option<Vec<(String, bool)>>,
    group_by_: Option<String>,
    columns_: Option<Vec<String>>,
    where_clauses_: Vec<String>,
    values_: Vec<ScyllaPyCQLDTO>,

    request_params_: ScyllaPyRequestParams,
}

impl Select {
    #[must_use]
    pub fn build_query(&self) -> String {
        let columns = self
            .columns_
            .as_ref()
            .map_or(String::from("*"), |cols| cols.join(","));
        let group_by = self
            .group_by_
            .as_ref()
            .map_or(String::new(), |grp| format!("GROUP BY {grp}"));
        let where_cls = if self.where_clauses_.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", self.where_clauses_.join(" AND "))
        };
        let orders = self.order_by_.as_ref().map_or(String::new(), |ords| {
            let ordered_cols = ords
                .iter()
                .map(|(col_name, desc)| {
                    if *desc {
                        format!("{col_name} DESC")
                    } else {
                        format!("{col_name} ASC")
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("ORDER BY {ordered_cols}")
        });
        let per_part_limit = self.per_partition_limit_.map_or(String::new(), |pplimit| {
            format!("PER PARTITION LIMIT {pplimit}")
        });
        let limit = self
            .limit_
            .map_or(String::new(), |limit| format!("LIMIT {limit}"));
        let allow_filtering = if self.allow_filtering_ {
            "ALLOW FILTERING"
        } else {
            ""
        };
        let bypass_cache = if self.bypass_cache_ {
            "BYPASS CACHE"
        } else {
            ""
        };
        let distinct = if self.distinct_ { "DISTINCT" } else { "" };
        let timeout = self
            .timeout_
            .as_ref()
            .map_or(String::new(), |timeout| match timeout {
                Timeout::Int(int) => format!("USING TIMEOUT {int}"),
                Timeout::Str(string) => format!("USING TIMEOUT {string}"),
            });
        pretty_build([
            "SELECT",
            distinct,
            columns.as_str(),
            "FROM",
            self.table_.as_str(),
            where_cls.as_str(),
            group_by.as_str(),
            orders.as_str(),
            per_part_limit.as_str(),
            limit.as_str(),
            allow_filtering,
            bypass_cache,
            timeout.as_str(),
        ])
    }
}

#[pymethods]
impl Select {
    #[new]
    #[must_use]
    pub fn py_new(table: String) -> Self {
        Self {
            table_: table,
            ..Default::default()
        }
    }

    /// Specify columns to fetch.
    ///
    /// # Errors
    /// Returns error, if
    /// passed arguments are not strings.
    #[pyo3(signature = (*columns))]
    pub fn only<'a>(
        mut slf: PyRefMut<'a, Self>,
        columns: &'a PyTuple,
    ) -> ScyllaPyResult<PyRefMut<'a, Self>> {
        let cols = columns.extract::<Vec<String>>()?;
        slf.columns_ = Some(cols);
        Ok(slf)
    }

    /// Add where clause.
    ///
    /// This function takes the clause
    /// and adds it to the list of all where clauses.
    ///
    /// Also, it takes a value, so you can
    /// bind parameters, while building query.
    ///
    /// # Errors
    /// May return an `Err` if any value cannot be
    /// translated into Rust.
    #[pyo3(signature = (clause, values = None))]
    pub fn r#where<'a>(
        mut slf: PyRefMut<'a, Self>,
        clause: String,
        values: Option<Vec<&'a PyAny>>,
    ) -> ScyllaPyResult<PyRefMut<'a, Self>> {
        slf.where_clauses_.push(clause);
        if let Some(vals) = values {
            for value in vals {
                slf.values_.push(py_to_value(value)?);
            }
        }
        Ok(slf)
    }

    #[must_use]
    pub fn group_by(mut slf: PyRefMut<'_, Self>, group: String) -> PyRefMut<'_, Self> {
        slf.group_by_ = Some(group);
        slf
    }

    #[must_use]
    #[pyo3(signature = (order, desc = false))]
    pub fn order_by(mut slf: PyRefMut<'_, Self>, order: String, desc: bool) -> PyRefMut<'_, Self> {
        if let Some(order_by) = &mut slf.order_by_ {
            order_by.push((order, desc));
        } else {
            slf.order_by_ = Some(vec![(order, desc)]);
        }
        slf
    }

    #[must_use]
    pub fn per_partition_limit(
        mut slf: PyRefMut<'_, Self>,
        per_partition_limit: i32,
    ) -> PyRefMut<'_, Self> {
        slf.per_partition_limit_ = Some(per_partition_limit);
        slf
    }

    #[must_use]
    pub fn limit(mut slf: PyRefMut<'_, Self>, limit: i32) -> PyRefMut<'_, Self> {
        slf.limit_ = Some(limit);
        slf
    }

    #[must_use]
    pub fn allow_filtering(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.allow_filtering_ = true;
        slf
    }

    #[must_use]
    pub fn bypass_cache(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.bypass_cache_ = true;
        slf
    }

    #[must_use]
    pub fn distinct(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.distinct_ = true;
        slf
    }

    #[must_use]
    pub fn timeout(mut slf: PyRefMut<'_, Self>, timeout: Timeout) -> PyRefMut<'_, Self> {
        slf.timeout_ = Some(timeout);
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
    /// Proxies errors from `native_execute`.
    #[pyo3(signature = (scylla, *, paged = false))]
    pub fn execute<'a>(
        &'a self,
        py: Python<'a>,
        scylla: &'a Scylla,
        paged: bool,
    ) -> ScyllaPyResult<&'a PyAny> {
        let mut query = Query::new(self.build_query());
        self.request_params_.apply_to_query(&mut query);
        scylla.native_execute(py, Some(query), None, self.values_.clone(), paged)
    }

    /// Add to batch
    ///
    /// Adds current query to batch.
    ///
    /// # Error
    ///
    /// Returns error if values cannot be passed to batch.
    pub fn add_to_batch(&self, batch: &mut ScyllaPyInlineBatch) -> ScyllaPyResult<()> {
        let mut query = Query::new(self.build_query());
        self.request_params_.apply_to_query(&mut query);

        let mut serialized = SerializedValues::new();
        for val in self.values_.clone() {
            serialized.add_value(&val)?;
        }
        batch.add_query_inner(query, serialized);
        Ok(())
    }

    #[must_use]
    pub fn __str__(&self) -> String {
        self.build_query()
    }

    #[must_use]
    pub fn __repr__(&self) -> String {
        format!("{self:?}")
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
