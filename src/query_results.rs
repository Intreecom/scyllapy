use std::collections::HashMap;

use pyo3::{pyclass, pymethods, Py, PyAny, Python, ToPyObject};
use scylla::QueryResult;

use crate::utils::{cql_to_py, map_rows};

#[pyclass(name = "QueryResult")]
pub struct ScyllaPyQueryResult {
    inner: QueryResult,
}

impl ScyllaPyQueryResult {
    pub fn new(results: QueryResult) -> Self {
        Self { inner: results }
    }
    fn get_rows<'a>(
        &'a self,
        py: Python<'a>,
        limit: Option<usize>,
    ) -> anyhow::Result<Option<Vec<HashMap<&'a str, &'a PyAny>>>> {
        let Some(rows) = &self.inner.rows else {
            return Ok(None);
        };
        let specs = &self.inner.col_specs;
        let mut dumped_rows = Vec::new();
        for (row_index, row) in rows.iter().enumerate() {
            let mut map = HashMap::new();
            for (col_index, column) in row.columns.iter().enumerate() {
                map.insert(
                    specs[col_index].name.as_str(),
                    cql_to_py(py, &specs[col_index].typ, column.as_ref())?,
                );
            }
            dumped_rows.push(map);
            if let Some(limit) = limit {
                if row_index >= limit {
                    break;
                }
            }
        }
        Ok(Some(dumped_rows))
    }
}

#[pymethods]
impl ScyllaPyQueryResult {
    /// Get all rows.
    ///
    /// This function returns all rows created by query.
    /// If `as_class` passed, it tries to cast every row
    /// to the target class, by passing all columns as
    /// keyword arguments.
    ///
    /// # Errors
    ///
    /// May return an error if the query should not return any row.
    pub fn all(&self, py: Python<'_>, as_class: Option<Py<PyAny>>) -> anyhow::Result<Py<PyAny>> {
        let Some(rows) = self.get_rows(py, None)? else {
            return Err(anyhow::anyhow!("The query doesn't have returns ."));
        };
        let py_rows = rows.to_object(py);
        if let Some(as_class) = as_class {
            return Ok(map_rows(py, &py_rows, &as_class)?.to_object(py));
        }
        Ok(py_rows)
    }

    /// Get only the first row.
    ///
    /// This method is almost the same as `all`,
    /// but only fetches one row from the database.
    ///
    /// # Errors
    ///
    /// Error can be returned if query didn't mean to return
    /// anything.
    pub fn first(
        &self,
        py: Python<'_>,
        as_class: Option<Py<PyAny>>,
    ) -> anyhow::Result<Option<Py<PyAny>>> {
        let Some(rows) = self.get_rows(py, Some(1))? else{
            return Err(anyhow::anyhow!("The query doesn't have returns ."));
        };
        if rows.is_empty() {
            return Ok(None);
        }
        if let Some(as_class) = as_class {
            let py_rows = rows.to_object(py);
            return Ok(map_rows(py, &py_rows, &as_class)?
                .first()
                .map(|val| val.to_object(py)));
        }
        Ok(Some(rows[0].to_object(py)))
    }

    /// Function to get first column of every row.
    ///
    /// This function grabs rows from all function and
    /// tries to get the first column of any row.
    ///
    /// # Erros
    ///
    /// May result in an error if:
    /// * Query doesn't have a returns;
    /// * Results don't have any columns.
    pub fn scalars(&self, py: Python<'_>) -> anyhow::Result<Option<Py<PyAny>>> {
        let Some(rows) = self.get_rows(py, None)? else{
            return Err(anyhow::anyhow!("The query doesn't have returns ."));
        };
        if rows.is_empty() {
            return Ok(None);
        }
        let Some(col_name) = self.inner.col_specs.first() else{
            return Err(anyhow::anyhow!("Cannot find any columns"));
        };
        Ok(Some(
            rows.iter()
                .filter_map(|row| row.get(col_name.name.as_str()))
                .collect::<Vec<_>>()
                .to_object(py),
        ))
    }

    /// Function to get first column of first row.
    ///
    /// This function grabs first row and
    /// tries to get the first column of a result.
    ///
    /// # Erros
    ///
    /// May result in an error if:
    /// * Query doesn't have a returns;
    /// * Results don't have any columns.
    pub fn scalar(&self, py: Python<'_>) -> anyhow::Result<Option<Py<PyAny>>> {
        let Some(rows) = self.get_rows(py, Some(1))? else{
            return Err(anyhow::anyhow!("The query doesn't have returns ."));
        };
        if rows.is_empty() {
            return Ok(None);
        }
        let Some(col_name) = self.inner.col_specs.first() else{
            return Err(anyhow::anyhow!("Cannot find any columns"));
        };
        Ok(Some(
            rows.first()
                .and_then(|row| row.get(col_name.name.as_str()))
                .to_object(py),
        ))
    }

    #[getter]
    pub fn trace_id<'a>(&'a self, py: Python<'a>) -> Option<Py<PyAny>> {
        self.inner
            .tracing_id
            .map(|uid| uid.to_string().to_object(py))
    }
}
