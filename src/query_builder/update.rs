use pyo3::{pyclass, pymethods, types::PyDict, PyAny, PyRefMut, Python};
use scylla::query::Query;

use crate::{
    queries::ScyllaPyRequestParams,
    scylla_cls::Scylla,
    utils::{py_to_value, ScyllaPyCQLDTO},
};

use super::utils::{pretty_build, IfCluase, Timeout};

#[derive(Clone, Debug)]
enum UpdateAssignment {
    Simple(String),
    Inc(String, String),
    Dec(String, String),
}

impl ToString for UpdateAssignment {
    fn to_string(&self) -> String {
        match self {
            UpdateAssignment::Simple(name) => format!("{name} = ?"),
            UpdateAssignment::Inc(left, right) => format!("{left} = {right} + ?"),
            UpdateAssignment::Dec(left, right) => format!("{left} = {right} - ?"),
        }
    }
}

#[pyclass]
#[derive(Clone, Debug, Default)]
pub struct Update {
    table_: String,
    assignments_: Vec<UpdateAssignment>,
    values_: Vec<ScyllaPyCQLDTO>,

    where_clauses_: Vec<String>,
    where_values_: Vec<ScyllaPyCQLDTO>,

    timeout_: Option<Timeout>,
    ttl_: Option<i32>,
    timestamp_: Option<u64>,
    if_clause_: Option<IfCluase>,

    request_params_: ScyllaPyRequestParams,
}

impl Update {
    fn build_query(&self) -> anyhow::Result<String> {
        if self.assignments_.is_empty() {
            return Err(anyhow::anyhow!(
                "Update should contain at least one assignment"
            ));
        }
        if self.where_clauses_.is_empty() {
            return Err(anyhow::anyhow!(
                "Update should contain at least one where clause"
            ));
        }
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

        let assigments = self
            .assignments_
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");

        let where_clauses = self.where_clauses_.join(" AND ");
        let if_conditions = self
            .if_clause_
            .as_ref()
            .map_or(String::default(), |cond| match cond {
                IfCluase::Exists => String::from("IF EXISTS"),
                IfCluase::Condition { clauses, values: _ } => {
                    format!("IF {}", clauses.join(" AND "))
                }
            });

        Ok(pretty_build([
            "UPDATE",
            self.table_.as_str(),
            usings.as_str(),
            format!("SET {assigments}").as_str(),
            format!("WHERE {where_clauses}").as_str(),
            if_conditions.as_str(),
        ]))
    }
}

#[pymethods]
impl Update {
    #[new]
    #[must_use]
    pub fn py_new(table: String) -> Self {
        Self {
            table_: table,
            ..Default::default()
        }
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
    ) -> anyhow::Result<PyRefMut<'a, Self>> {
        slf.assignments_.push(UpdateAssignment::Simple(name));
        slf.values_.push(py_to_value(value)?);
        Ok(slf)
    }

    /// Increment column value.
    ///
    /// # Error
    ///
    /// If cannot convert python type
    /// to appropriate rust type.
    pub fn inc<'a>(
        mut slf: PyRefMut<'a, Self>,
        name: String,
        value: &'a PyAny,
    ) -> anyhow::Result<PyRefMut<'a, Self>> {
        slf.assignments_
            .push(UpdateAssignment::Inc(name.clone(), name));
        slf.values_.push(py_to_value(value)?);
        Ok(slf)
    }

    /// Decrement value.
    ///
    /// # Errors
    ///
    /// If cannot convert python type
    /// to appropriate rust type.
    pub fn dec<'a>(
        mut slf: PyRefMut<'a, Self>,
        name: String,
        value: &'a PyAny,
    ) -> anyhow::Result<PyRefMut<'a, Self>> {
        slf.assignments_
            .push(UpdateAssignment::Dec(name.clone(), name));
        slf.values_.push(py_to_value(value)?);
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
    ) -> anyhow::Result<PyRefMut<'a, Self>> {
        slf.where_clauses_.push(clause);
        if let Some(vals) = values {
            for value in vals {
                slf.where_values_.push(py_to_value(value)?);
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
    ) -> anyhow::Result<PyRefMut<'a, Self>> {
        if let Some(params) = params {
            let parsed_params = ScyllaPyRequestParams::from_dict(params)?;
            slf.request_params_ = parsed_params;
        } else {
            slf.request_params_ = ScyllaPyRequestParams::default();
        }
        Ok(slf)
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

    /// Execute a query.
    ///
    /// # Errors
    ///
    /// May return an error, if something goes wrong
    /// during query building
    /// or during query execution.
    pub fn execute<'a>(&'a self, py: Python<'a>, scylla: &'a Scylla) -> anyhow::Result<&'a PyAny> {
        let mut query = Query::new(self.build_query()?);
        self.request_params_.apply(&mut query);
        let mut values = self.values_.clone();
        values.extend(self.where_values_.clone());
        let values = if let Some(if_clause) = &self.if_clause_ {
            if_clause.extend_values(values)
        } else {
            values
        };
        scylla.native_execute(py, query, values)
    }

    /// Build query.
    ///
    /// # Errors
    ///
    /// If query cannot be constructed.
    pub fn __str__(&self) -> anyhow::Result<String> {
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
