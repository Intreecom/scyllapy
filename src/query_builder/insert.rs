use pyo3::{pyclass, pymethods, PyAny, PyRefMut};

use crate::utils::{py_to_value, ScyllaPyCQLDTO};

use super::utils::Timeout;

#[pyclass]
#[derive(Clone, Debug)]
pub struct Insert {
    table_: String,
    if_not_exists_: bool,
    names_: Vec<String>,
    values_: Vec<ScyllaPyCQLDTO>,
    timeout_: Option<Timeout>,
    ttl_: Option<u64>,
    timestamp_: Option<u64>,
}

impl Insert {
    /// Build a statement.
    ///
    /// # Errors
    /// If no values was set.
    pub fn build_query(&self) -> anyhow::Result<String> {
        if self.names_.is_empty() {
            return Err(anyhow::anyhow!("Please use at least one set method."));
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

        let query = vec![
            "INSERT INTO",
            self.table_.as_str(),
            names_values.as_str(),
            ifnexist,
            usings.as_str(),
        ]
        .into_iter()
        .filter(|s| !s.is_empty())
        .collect::<Vec<&str>>()
        .join(" ");
        Ok(query)
    }
}

#[pymethods]
impl Insert {
    #[new]
    #[must_use]
    pub fn py_new(table: String) -> Self {
        Insert {
            table_: table,
            if_not_exists_: false,
            names_: vec![],
            values_: vec![],
            timeout_: None,
            ttl_: None,
            timestamp_: None,
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
    ) -> anyhow::Result<PyRefMut<'a, Self>> {
        slf.names_.push(name);
        slf.values_.push(py_to_value(value)?);
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
    pub fn ttl(mut slf: PyRefMut<'_, Self>, ttl: u64) -> PyRefMut<'_, Self> {
        slf.ttl_ = Some(ttl);
        slf
    }

    #[must_use]
    pub fn __repr__(&self) -> String {
        format!("{self:?}")
    }

    /// Returns string part of a query.
    ///
    /// # Errors
    /// If cannot construct query.
    pub fn __str__(&self) -> anyhow::Result<String> {
        self.build_query()
    }
}
