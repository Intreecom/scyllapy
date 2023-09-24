use std::{collections::HashMap, sync::Arc};

use futures::StreamExt;
use pyo3::{
    exceptions::PyStopAsyncIteration, pyclass, pymethods, types::PyDict, IntoPy, Py, PyAny,
    PyObject, PyRef, PyRefMut, Python, ToPyObject,
};
use scylla::{transport::iterator::RowIterator, QueryResult};
use tokio::sync::Mutex;

use crate::{
    exceptions::rust_err::{ScyllaPyError, ScyllaPyResult},
    utils::{cql_to_py, map_rows, scyllapy_future},
};

pub enum ScyllaPyQueryReturns {
    QueryResult(ScyllaPyQueryResult),
    IterableQueryResult(ScyllaPyIterableQueryResult),
}

impl IntoPy<Py<PyAny>> for ScyllaPyQueryReturns {
    fn into_py(self, py: Python<'_>) -> Py<PyAny> {
        match self {
            ScyllaPyQueryReturns::QueryResult(result) => result.into_py(py),
            ScyllaPyQueryReturns::IterableQueryResult(result) => result.into_py(py),
        }
    }
}

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
    ) -> ScyllaPyResult<Option<Vec<HashMap<&'a str, &'a PyAny>>>> {
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
                    cql_to_py(
                        py,
                        &specs[col_index].name,
                        &specs[col_index].typ,
                        column.as_ref(),
                    )?,
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
    pub fn all(&self, py: Python<'_>, as_class: Option<Py<PyAny>>) -> ScyllaPyResult<Py<PyAny>> {
        let Some(rows) = self.get_rows(py, None)? else {
            return Err(ScyllaPyError::NoReturnsError);
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
    ) -> ScyllaPyResult<Option<Py<PyAny>>> {
        let Some(rows) = self.get_rows(py, Some(1))? else {
            return Err(ScyllaPyError::NoReturnsError);
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
    pub fn scalars(&self, py: Python<'_>) -> ScyllaPyResult<Option<Py<PyAny>>> {
        let Some(rows) = self.get_rows(py, None)? else {
            return Err(ScyllaPyError::NoReturnsError);
        };
        if rows.is_empty() {
            return Ok(Some(rows.to_object(py)));
        }
        let Some(col_name) = self.inner.col_specs.first() else {
            return Err(ScyllaPyError::NoColumns);
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
    pub fn scalar(&self, py: Python<'_>) -> ScyllaPyResult<Option<Py<PyAny>>> {
        let Some(rows) = self.get_rows(py, Some(1))? else {
            return Err(ScyllaPyError::NoReturnsError);
        };
        if rows.is_empty() {
            return Ok(None);
        }
        let Some(col_name) = self.inner.col_specs.first() else {
            return Err(ScyllaPyError::NoColumns);
        };
        Ok(Some(
            rows.first()
                .and_then(|row| row.get(col_name.name.as_str()))
                .to_object(py),
        ))
    }

    /// Get lenght of the result.
    ///
    /// # Errors
    ///
    /// May result in an error
    /// if returned result doesn't contain rows.
    pub fn __len__(&self) -> ScyllaPyResult<usize> {
        self.inner
            .rows_num()
            .map_err(|_| ScyllaPyError::NoReturnsError)
    }

    #[getter]
    pub fn trace_id<'a>(&'a self, py: Python<'a>) -> Option<Py<PyAny>> {
        self.inner
            .tracing_id
            .map(|uid| uid.to_string().to_object(py))
    }
}

#[pyclass(name = "IterableQueryResult")]
pub struct ScyllaPyIterableQueryResult {
    inner: Arc<Mutex<RowIterator>>,
    mapper: Option<Py<PyAny>>,
    scalars: bool,
}

impl ScyllaPyIterableQueryResult {
    pub fn new(results: RowIterator) -> Self {
        Self {
            inner: Arc::new(Mutex::new(results)),
            mapper: None,
            scalars: false,
        }
    }
}

#[pymethods]
impl ScyllaPyIterableQueryResult {
    #[must_use]
    pub fn as_cls(mut slf: PyRefMut<'_, Self>, as_class: Py<PyAny>) -> PyRefMut<'_, Self> {
        slf.mapper = Some(as_class);
        slf
    }

    #[must_use]
    pub fn scalars(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.scalars = true;
        slf
    }

    #[must_use]
    pub fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Actual async iteration.
    ///
    /// Here we define how to
    pub fn __anext__(&self, py: Python<'_>) -> ScyllaPyResult<Option<PyObject>> {
        let streamer = self.inner.clone();
        let map_function = self.mapper.clone();
        let scalars = self.scalars;
        // Here we create our future that actually yields row.
        let future = scyllapy_future(py, async move {
            let mut row_iterator = streamer.lock().await;
            let row = row_iterator.next().await;
            let col_spec = row_iterator.get_column_specs();
            match row {
                Some(val) => {
                    let row_val = val?;
                    // If user have chosen to iterate over scalars, we
                    // just return first column of a row.
                    if scalars {
                        let spec = col_spec.first().ok_or(ScyllaPyError::NoColumns)?;
                        let a = row_val.columns.first().ok_or(ScyllaPyError::NoColumns)?;
                        return Python::with_gil(|gil| {
                            Ok(cql_to_py(gil, &spec.name, &spec.typ, a.as_ref())?.into_py(gil))
                        });
                    }
                    // Here we acquire GIL and map row to python object.
                    Python::with_gil(move |gil| -> ScyllaPyResult<Py<PyAny>> {
                        let row_dict = PyDict::new(gil);
                        for (col_index, column) in row_val.columns.iter().enumerate() {
                            row_dict.set_item(
                                col_spec[col_index].name.as_str(),
                                cql_to_py(
                                    gil,
                                    &col_spec[col_index].name,
                                    &col_spec[col_index].typ,
                                    column.as_ref(),
                                )?,
                            )?;
                        }
                        if let Some(mapper) = map_function {
                            Ok(mapper.call(gil, (), Some(row_dict))?.into_py(gil))
                        } else {
                            Ok(row_dict.into())
                        }
                    })
                }
                None => Err(PyStopAsyncIteration::new_err("No more rows").into()),
            }
        });
        Ok(Some(future?.into()))
    }
}
