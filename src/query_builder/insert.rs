use pyo3::{pyclass, pymethods, PyAny, PyRefMut};

use crate::utils::{py_to_value, ScyllaPyCQLDTO};

use super::utils::Timeout;

#[pyclass]
#[derive(Clone, Debug)]
pub struct Insert {
    _table: String,
    _if_not_exists: bool,
    _names: Vec<String>,
    _values: Vec<ScyllaPyCQLDTO>,
    _timeout: Option<Timeout>,
    _ttl: Option<u64>,
    _timestamp: Option<u64>,
}

impl Insert {
    pub fn build_query(&self) -> anyhow::Result<String> {
        if self._names.is_empty() {
            return Err(anyhow::anyhow!("Please use at least one set method."));
        }
        let names = self._names.join(",");
        let values = self
            ._names
            .iter()
            .map(|_| "?")
            .collect::<Vec<&str>>()
            .join(",");
        let names_values = format!("({names}) VALUES ({values})");
        let ifnexist = if self._if_not_exists {
            "IF NOT EXISTS"
        } else {
            ""
        };
        let params = vec![
            self._timestamp
                .map(|timestamp| format!("TIMESTAMP {timestamp}")),
            self._ttl.map(|ttl| format!("TTL {ttl}")),
            self._timeout.as_ref().map(|timeout| match timeout {
                Timeout::Int(int) => format!("TIMEOUT {int}"),
                Timeout::Str(string) => format!("TIMEOUT {string}"),
            }),
        ];
        let prepared_params = params
            .iter()
            .map(|item| item.as_ref().map(String::as_str).unwrap_or(""))
            .filter(|item| !item.is_empty())
            .collect::<Vec<_>>();
        let usings = if !prepared_params.is_empty() {
            format!("USING {}", prepared_params.join(" AND "))
        } else {
            String::new()
        };

        let query = vec![
            "INSERT INTO",
            self._table.as_str(),
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
    pub fn py_new(table: String) -> Self {
        Insert {
            _table: table,
            _if_not_exists: false,
            _names: vec![],
            _values: vec![],
            _timeout: None,
            _ttl: None,
            _timestamp: None,
        }
    }

    pub fn if_not_exists<'a>(mut slf: PyRefMut<'a, Self>) -> anyhow::Result<PyRefMut<'a, Self>> {
        slf._if_not_exists = true;
        Ok(slf)
    }

    pub fn set<'a>(
        mut slf: PyRefMut<'a, Self>,
        name: String,
        value: &PyAny,
    ) -> anyhow::Result<PyRefMut<'a, Self>> {
        slf._names.push(name);
        slf._values.push(py_to_value(value)?);
        Ok(slf)
    }

    pub fn timeout<'a>(
        mut slf: PyRefMut<'a, Self>,
        timeout: Timeout,
    ) -> anyhow::Result<PyRefMut<'a, Self>> {
        slf._timeout = Some(timeout);
        Ok(slf)
    }

    pub fn timestamp<'a>(
        mut slf: PyRefMut<'a, Self>,
        timestamp: u64,
    ) -> anyhow::Result<PyRefMut<'a, Self>> {
        slf._timestamp = Some(timestamp);
        Ok(slf)
    }

    pub fn ttl<'a>(mut slf: PyRefMut<'a, Self>, ttl: u64) -> anyhow::Result<PyRefMut<'a, Self>> {
        slf._ttl = Some(ttl);
        Ok(slf)
    }

    pub fn __repr__(&self) -> String {
        format!("{:?}", self)
    }

    pub fn __str__(&self) -> anyhow::Result<String> {
        self.build_query()
    }
}
