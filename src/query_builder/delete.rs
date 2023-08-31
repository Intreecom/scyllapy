use pyo3::{pyclass, pymethods, types::PyDict, PyAny, PyRefMut, Python};
use scylla::query::Query;

use crate::{
    queries::ScyllaPyRequestParams,
    scylla_cls::Scylla,
    utils::{py_to_value, ScyllaPyCQLDTO},
};

use super::utils::Timeout;

#[derive(Clone, Debug)]
pub enum IfCluase {
    Exists,
    Condition {
        clauses: Vec<String>,
        values: Vec<ScyllaPyCQLDTO>,
    },
}

impl IfCluase {
    #[must_use]
    pub fn extend_values(&self, values: &[ScyllaPyCQLDTO]) -> Vec<ScyllaPyCQLDTO> {
        match self {
            IfCluase::Exists => values.to_owned(),
            IfCluase::Condition { clauses: _, values } => {
                values.iter().chain(values.iter()).cloned().collect()
            }
        }
    }
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct Delete {
    table_: String,
    columns: Option<Vec<String>>,
    timeout_: Option<Timeout>,
    timestamp_: Option<u64>,
    if_clause_: Option<IfCluase>,
    where_clauses_: Vec<String>,
    values_: Vec<ScyllaPyCQLDTO>,
    request_params: ScyllaPyRequestParams,
}

impl Delete {
    fn build_query(&self) -> anyhow::Result<String> {
        if self.where_clauses_.is_empty() {
            return Err(anyhow::anyhow!(
                "At least one where clause should be specified."
            ));
        }
        let columns = self
            .columns
            .as_ref()
            .map_or(String::new(), |cols| cols.join(", "));
        let params = vec![
            self.timestamp_
                .map(|timestamp| format!("TIMESTAMP {timestamp}")),
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
        let where_clause = format!("WHERE {}", self.where_clauses_.join(" AND "));
        let if_conditions = self
            .if_clause_
            .as_ref()
            .map_or(String::default(), |cond| match cond {
                IfCluase::Exists => String::from("IF EXISTS"),
                IfCluase::Condition { clauses, values: _ } => {
                    format!("IF {}", clauses.join(" AND "))
                }
            });
        let query = vec![
            "DELETE",
            columns.as_str(),
            "FROM",
            self.table_.as_str(),
            usings.as_str(),
            where_clause.as_str(),
            if_conditions.as_str(),
        ]
        .into_iter()
        .filter(|s| !s.is_empty())
        .collect::<Vec<&str>>()
        .join(" ");
        Ok(query)
    }
}

#[pymethods]
impl Delete {
    #[new]
    #[must_use]
    pub fn py_new(table: String) -> Self {
        Self {
            table_: table,
            columns: None,
            timeout_: None,
            timestamp_: None,
            if_clause_: None,
            where_clauses_: vec![],
            values_: vec![],
            request_params: ScyllaPyRequestParams::default(),
        }
    }

    #[must_use]
    #[pyo3(signature = (*cols))]
    pub fn cols(mut slf: PyRefMut<'_, Self>, cols: Vec<String>) -> PyRefMut<'_, Self> {
        slf.columns = Some(cols);
        slf
    }

    /// Add where clause.
    ///
    /// This function adds where with values.
    ///
    /// # Errors
    ///
    /// Can return an error, if values
    /// cannot be parsed.
    #[pyo3(signature = (clause, values = None))]
    pub fn r#where<'a>(
        mut slf: PyRefMut<'a, Self>,
        clause: String,
        values: Option<Vec<&'a PyAny>>,
    ) -> anyhow::Result<PyRefMut<'a, Self>> {
        slf.where_clauses_.push(clause);
        if let Some(vals) = values {
            for value in vals {
                slf.values_.push(py_to_value(value)?);
            }
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
    pub fn if_exists(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.if_clause_ = Some(IfCluase::Exists);
        slf
    }

    /// Add if clause.
    ///
    /// # Errors
    ///
    /// May return an error, if values
    /// cannot be converted to rust types.
    #[pyo3(signature = (clause, values = None))]
    pub fn if_<'a>(
        mut slf: PyRefMut<'a, Self>,
        clause: String,
        values: Option<Vec<&'a PyAny>>,
    ) -> anyhow::Result<PyRefMut<'a, Self>> {
        let parsed_values = if let Some(vals) = values {
            vals.iter()
                .map(|item| py_to_value(item))
                .collect::<Result<Vec<_>, _>>()?
        } else {
            vec![]
        };
        match slf.if_clause_.as_mut() {
            Some(IfCluase::Condition { clauses, values }) => {
                clauses.push(clause);
                values.extend(parsed_values);
            }
            None | Some(IfCluase::Exists) => {
                slf.if_clause_ = Some(IfCluase::Condition {
                    clauses: vec![clause],
                    values: parsed_values,
                });
            }
        }
        Ok(slf)
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
    ) -> anyhow::Result<PyRefMut<'a, Self>> {
        if let Some(params) = params {
            let parsed_params = ScyllaPyRequestParams::from_dict(params)?;
            slf.request_params = parsed_params;
        } else {
            slf.request_params = ScyllaPyRequestParams::default();
        }
        Ok(slf)
    }

    /// Execute a query.
    ///
    /// # Errors
    ///
    /// May return an error, if something goes wrong
    /// during query building
    /// or during query execution.
    pub fn execute<'a>(&'a self, py: Python<'a>, scylla: &'a Scylla) -> anyhow::Result<&'a PyAny> {
        let mut query = Query::new(self.build_query()?);
        self.request_params.apply(&mut query);

        let values = if let Some(if_clause) = &self.if_clause_ {
            if_clause.extend_values(&self.values_)
        } else {
            self.values_.clone()
        };
        scylla.native_execute(py, query, values)
    }

    #[must_use]
    pub fn __repr__(&self) -> String {
        format!("{self:?}")
    }

    /// Convert query to string.
    ///
    /// # Errors
    ///
    /// May return an error if something
    /// goes wrong during query building.
    pub fn __str__(&self) -> anyhow::Result<String> {
        self.build_query()
    }
}
