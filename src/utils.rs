use std::{collections::HashMap, future::Future};

use pyo3::{
    types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PySet, PyString, PyTuple},
    IntoPy, Py, PyAny, PyObject, Python, ToPyObject,
};
use scylla::{
    _macro_internal::{CqlValue, Value},
    frame::response::result::ColumnType,
    QueryResult,
};

/// Small function to integrate anyhow result
/// and `pyo3_asyncio`.
///
/// It's almost the same as `future_into_py`,
/// but it expects future to return anyhow result, rather
/// than `PyResult` from `pyo3`. It's useful for using `?` operators all over the place.
///
/// # Errors
///
/// If result of a future was unsuccessful, it propagates the error.
pub fn anyhow_py_future<F, T>(py: Python<'_>, fut: F) -> anyhow::Result<&PyAny>
where
    F: Future<Output = anyhow::Result<T>> + Send + 'static,
    T: IntoPy<PyObject>,
{
    let res = pyo3_asyncio::tokio::future_into_py(py, async { fut.await.map_err(Into::into) })
        .map(Into::into)?;
    Ok(res)
}

/// Convert python type to CQL value.
///
/// This function is used to convert parameters, passed from
/// python to convinient `CQLValue` type. All these values are
/// going to be used in parameter bindings for query.
///
/// # Errors
/// It can raise an error if type cannot be extracted,
/// or if type is unsupported.
pub fn py_to_cql_value(item: &PyAny) -> anyhow::Result<Box<dyn Value + Send + Sync>> {
    if item.is_instance_of::<PyString>() {
        return Ok(Box::new(item.extract::<String>()?));
    } else if item.is_instance_of::<PyList>()
        || item.is_exact_instance_of::<PyTuple>()
        || item.is_exact_instance_of::<PySet>()
    {
        let mut items = Vec::new();
        for inner in item.iter()? {
            items.push(py_to_cql_value(inner?)?);
        }
        return Ok(Box::new(items));
    } else if item.is_instance_of::<PyInt>() {
        return Ok(Box::new(item.extract::<i64>()?));
    } else if item.is_instance_of::<PyBool>() {
        return Ok(Box::new(item.extract::<bool>()?));
    } else if item.is_instance_of::<PyFloat>() {
        return Ok(Box::new(item.extract::<f64>()?));
    } else if item.is_instance_of::<PyBytes>() {
        return Ok(Box::new(item.extract::<Vec<u8>>()?));
    } else if item.get_type().name()? == "UUID" {
        return Ok(Box::new(uuid::Uuid::parse_str(
            item.str()?.extract::<&str>()?,
        )?));
    }
    let name = item.get_type();
    Err(anyhow::anyhow!(
        "Unsupported type for parameter binding: {name}"
    ))
}

/// Convert CQL type from database to Python.
///
/// This function takes a CQL value from database
/// response and converts it to some python type,
/// loading it in interpreter, so it can be referenced
/// from python code.
///
/// `cql_type` is the type that database sent to us.
/// Used to parse the value with appropriate parser.
///
///
/// # Errors
///
/// This function can throw an error, if it was unable
/// to parse thr type, or if type is not supported.
#[allow(clippy::too_many_lines)]
pub fn cql_to_py<'a>(
    py: Python<'a>,
    cql_type: &'a ColumnType,
    cql_value: Option<CqlValue>,
) -> anyhow::Result<&'a PyAny> {
    let Some(unwrapped_value) = cql_value else{
        return Ok(py.None().into_ref(py));
    };

    match cql_type {
        ColumnType::Ascii => unwrapped_value
            .as_ascii()
            .ok_or(anyhow::anyhow!("Cannot parse"))
            .map(|val| PyString::new(py, val).as_ref()),
        ColumnType::Boolean => unwrapped_value
            .as_boolean()
            .ok_or(anyhow::anyhow!("Cannot parse"))
            .map(|val| PyBool::new(py, val).as_ref()),
        ColumnType::Blob => unwrapped_value
            .as_blob()
            .ok_or(anyhow::anyhow!("Cannot parse"))
            .map(|val| PyBytes::new(py, val.as_ref()).as_ref()),
        ColumnType::Double => unwrapped_value
            .as_double()
            .ok_or(anyhow::anyhow!("Cannot parse"))
            .map(|val| val.to_object(py).into_ref(py)),
        ColumnType::Float => unwrapped_value
            .as_double()
            .ok_or(anyhow::anyhow!("Cannot parse"))
            .map(|val| val.to_object(py).into_ref(py)),
        ColumnType::Int => unwrapped_value
            .as_int()
            .ok_or(anyhow::anyhow!("Cannot parse"))
            .map(|val| val.to_object(py).into_ref(py)),
        ColumnType::BigInt => unwrapped_value
            .as_bigint()
            .ok_or(anyhow::anyhow!("Cannot parse"))
            .map(|val| val.to_object(py).into_ref(py)),
        ColumnType::Text => unwrapped_value
            .as_text()
            .ok_or(anyhow::anyhow!("Cannot parse"))
            .map(|val| val.to_object(py).into_ref(py)),
        ColumnType::List(column_type) => {
            let items = unwrapped_value
                .as_list()
                .ok_or(anyhow::anyhow!("Cannot parse"))?
                .iter()
                .map(|val| cql_to_py(py, column_type.as_ref(), Some(val.clone())))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(items.to_object(py).into_ref(py))
        }
        ColumnType::Map(key_type, val_type) => {
            let map_values = unwrapped_value
                .as_map()
                .ok_or(anyhow::anyhow!("Cannot parse"))?
                .iter()
                .map(|(key, val)| -> anyhow::Result<(&'a PyAny, &'a PyAny)> {
                    Ok((
                        cql_to_py(py, key_type, Some(key.clone()))?,
                        cql_to_py(py, val_type, Some(val.clone()))?,
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?;

            let res_map = PyDict::new(py);
            for (key, value) in map_values {
                res_map.set_item(key, value)?;
            }
            Ok(res_map)
        }
        ColumnType::Set(column_type) => {
            let items = unwrapped_value
                .as_set()
                .ok_or(anyhow::anyhow!("Cannot parse"))?
                .iter()
                .map(|val| cql_to_py(py, column_type.as_ref(), Some(val.clone())))
                .collect::<Result<Vec<_>, _>>()?;
            let res_set = PySet::new(py, items)?;
            Ok(res_set)
        }
        ColumnType::SmallInt => unwrapped_value
            .as_smallint()
            .ok_or(anyhow::anyhow!("Cannot parse"))
            .map(|val| val.to_object(py).into_ref(py)),
        ColumnType::TinyInt => unwrapped_value
            .as_tinyint()
            .ok_or(anyhow::anyhow!("Cannot parse"))
            .map(|val| val.to_object(py).into_ref(py)),
        ColumnType::Uuid => {
            let uuid_str = unwrapped_value
                .as_uuid()
                .ok_or(anyhow::anyhow!(""))?
                .simple()
                .to_string();
            Ok(py.import("uuid")?.getattr("UUID")?.call1((uuid_str,))?)
        }
        ColumnType::Custom(_) => Err(anyhow::anyhow!("Custom types are not yet supported.")),
        ColumnType::Counter => Err(anyhow::anyhow!("Counter is not yet supported.")),
        ColumnType::Varint => Err(anyhow::anyhow!("Variant is not yet supported.")),
        ColumnType::Time => Err(anyhow::anyhow!("Time is not yet supported.")),
        ColumnType::Timestamp => Err(anyhow::anyhow!("Timestamp is not yet supported.")),
        ColumnType::Inet => Err(anyhow::anyhow!("Inet is not yet supported.")),
        ColumnType::Date => Err(anyhow::anyhow!("Date is not yet supported.")),
        ColumnType::Duration => Err(anyhow::anyhow!("Duration is not yet supported.")),
        ColumnType::Timeuuid => Err(anyhow::anyhow!("TimeUUID is not yet supported.")),
        ColumnType::Tuple(_) => Err(anyhow::anyhow!("Tuple is not yet supported.")),
        ColumnType::Decimal => Err(anyhow::anyhow!("Decimals are not yet supported.")),
        ColumnType::UserDefinedType {
            type_name: _,
            keyspace: _,
            field_types: _,
        } => Err(anyhow::anyhow!("UDT is not yet supported.")),
    }
}

/// Convert `QueryResult` to some Python-native object.
///
/// the `as_class` parameter is supplied by users.
/// It should be a callable python object, that returns
/// some DTO. It's called with key-word only arguments.
///
/// # Errors
///
/// Can result in an error in two main cases.
/// * Resulting rows cannot be parsed as list.
/// * Dict that used as kwargs cannot be downcasted as dict.
pub fn convert_db_response(
    res: QueryResult,
    as_class: Option<PyObject>,
) -> anyhow::Result<Option<Py<PyAny>>> {
    let specs = res.col_specs.clone();
    if let Ok(rows) = res.rows() {
        // We need to enable GIL here,
        // because we're going to create references
        // in Python's heap memory, so everything
        // returned by the query may be accessed from python.
        Python::with_gil(|py| {
            let mut dumped_rows = Vec::new();
            for row in rows {
                let mut map = HashMap::new();
                for (index, col) in row.columns.into_iter().enumerate() {
                    map.insert(
                        specs[index].name.as_str(),
                        // Here we convert returned row to python-native type.
                        cql_to_py(py, &specs[index].typ, col)?,
                    );
                }
                dumped_rows.push(map);
            }
            let py_rows = dumped_rows.to_object(py);
            // If user wants to use custom DTO for rows,
            // we use call it for every row.
            if let Some(parser) = as_class {
                let mut result = Vec::new();
                let py_rows_list = py_rows
                    .downcast::<PyList>(py)
                    .map_err(|_| anyhow::anyhow!("Cannot parse returned results as list."))?;
                for row in py_rows_list {
                    result.push(parser.call(
                        py,
                        (),
                        // Here we pass returned fields as kwargs.
                        Some(row.downcast::<PyDict>().map_err(|_| {
                            anyhow::anyhow!("Cannot prepare data for calling function")
                        })?),
                    )?);
                }
                return Ok(Some(result.to_object(py)));
            }
            Ok(Some(py_rows))
        })
    } else {
        Ok(None)
    }
}
