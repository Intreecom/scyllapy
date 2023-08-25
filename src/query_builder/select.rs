use pyo3::{pyclass, pymethods, types::PyTuple, PyAny, PyRefMut};

use crate::utils::{py_to_value, ScyllaPyCQLDTO};

use super::utils::Timeout;

#[pyclass]
#[derive(Clone, Debug)]
pub struct Select {
    _table: String,
    _distinct: bool,
    _allow_filtering: bool,
    _bypass_cache: bool,
    _timeout: Option<Timeout>,
    _limit: Option<i32>,
    _per_partition_limit: Option<i32>,
    _order_by: Option<Vec<(String, bool)>>,
    _group_by: Option<String>,
    _columns: Option<Vec<String>>,
    _where_clauses: Vec<String>,
    _values: Vec<ScyllaPyCQLDTO>,
}

impl Select {
    pub fn build_query(&self) -> String {
        let columns = self
            ._columns
            .as_ref()
            .map_or(String::from("*"), |cols| cols.join(","));
        let group_by = self
            ._group_by
            .as_ref()
            .map(|grp| format!("GROUP BY {}", grp))
            .unwrap_or(String::new());
        let where_cls = if self._where_clauses.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", self._where_clauses.join(" AND "))
        };
        let orders = self._order_by.as_ref().map_or(String::new(), |ords| {
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
            format!("ORDER BY {}", ordered_cols)
        });
        let per_part_limit = self._per_partition_limit.map_or(String::new(), |pplimit| {
            format!("PER PARTITION LIMIT {pplimit}")
        });
        let limit = self
            ._limit
            .map_or(String::new(), |limit| format!("LIMIT {limit}"));
        let allow_filtering = if self._allow_filtering {
            "ALLOW FILTERING"
        } else {
            ""
        };
        let bypass_cache = if self._bypass_cache {
            "BYPASS CACHE"
        } else {
            ""
        };
        let distinct = if self._distinct { "DISTINCT" } else { "" };
        let timeout = self
            ._timeout
            .as_ref()
            .map_or(String::new(), |timeout| match timeout {
                Timeout::Int(int) => format!("USING TIMEOUT {int}"),
                Timeout::Str(string) => format!("USING TIMEOUT {string}"),
            });
        vec![
            "SELECT",
            distinct,
            columns.as_str(),
            "FROM",
            self._table.as_str(),
            where_cls.as_str(),
            group_by.as_str(),
            orders.as_str(),
            per_part_limit.as_str(),
            limit.as_str(),
            allow_filtering,
            bypass_cache,
            timeout.as_str(),
        ]
        .into_iter()
        .filter(|s| !s.is_empty())
        .collect::<Vec<&str>>()
        .join(" ")
    }
}

#[pymethods]
impl Select {
    #[new]
    pub fn py_new(table: String) -> Select {
        Select {
            _table: table,
            _distinct: false,
            _allow_filtering: false,
            _bypass_cache: false,
            _timeout: None,
            _limit: None,
            _per_partition_limit: None,
            _order_by: None,
            _group_by: None,
            _columns: None,
            _where_clauses: vec![],
            _values: vec![],
        }
    }

    #[pyo3(signature = (*columns))]
    pub fn only<'a>(
        mut slf: PyRefMut<'a, Self>,
        columns: &'a PyTuple,
    ) -> anyhow::Result<PyRefMut<'a, Self>> {
        let cols = columns.extract::<Vec<String>>()?;
        slf._columns = Some(cols);
        Ok(slf)
    }

    pub fn filter<'a>(
        mut slf: PyRefMut<'a, Self>,
        clause: String,
        values: Option<Vec<&'a PyAny>>,
    ) -> anyhow::Result<PyRefMut<'a, Self>> {
        slf._where_clauses.push(clause);
        if let Some(vals) = values {
            for value in vals {
                slf._values.push(py_to_value(value)?);
            }
        }
        Ok(slf)
    }

    pub fn group_by<'a>(
        mut slf: PyRefMut<'a, Self>,
        group: String,
    ) -> anyhow::Result<PyRefMut<'a, Self>> {
        slf._group_by = Some(group);
        Ok(slf)
    }

    #[pyo3(signature = (order, desc = false))]
    pub fn order_by<'a>(
        mut slf: PyRefMut<'a, Self>,
        order: String,
        desc: bool,
    ) -> anyhow::Result<PyRefMut<'a, Self>> {
        if let Some(order_by) = &mut slf._order_by {
            order_by.push((order, desc));
        } else {
            slf._order_by = Some(vec![(order, desc)])
        }
        Ok(slf)
    }

    pub fn per_partition_limit<'a>(
        mut slf: PyRefMut<'a, Self>,
        per_partition_limit: i32,
    ) -> anyhow::Result<PyRefMut<'a, Self>> {
        slf._per_partition_limit = Some(per_partition_limit);
        Ok(slf)
    }

    pub fn limit<'a>(
        mut slf: PyRefMut<'a, Self>,
        limit: i32,
    ) -> anyhow::Result<PyRefMut<'a, Self>> {
        slf._limit = Some(limit);
        Ok(slf)
    }

    pub fn allow_filtering<'a>(mut slf: PyRefMut<'a, Self>) -> anyhow::Result<PyRefMut<'a, Self>> {
        slf._allow_filtering = true;
        Ok(slf)
    }

    pub fn bypass_cache<'a>(mut slf: PyRefMut<'a, Self>) -> anyhow::Result<PyRefMut<'a, Self>> {
        slf._bypass_cache = true;
        Ok(slf)
    }

    pub fn distinct<'a>(mut slf: PyRefMut<'a, Self>) -> anyhow::Result<PyRefMut<'a, Self>> {
        slf._distinct = true;
        Ok(slf)
    }

    pub fn timeout<'a>(
        mut slf: PyRefMut<'a, Self>,
        timeout: Timeout,
    ) -> anyhow::Result<PyRefMut<'a, Self>> {
        slf._timeout = Some(timeout);
        Ok(slf)
    }

    pub fn __str__(&self) -> String {
        self.build_query()
    }

    pub fn __repr__(&self) -> String {
        format!("{:?}", self)
    }
}
