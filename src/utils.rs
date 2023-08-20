use std::future::Future;

use pyo3::{
    types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PySet, PyString, PyTuple},
    IntoPy, PyAny, PyObject, Python, ToPyObject,
};
use scylla::{
    _macro_internal::{CqlValue, Value},
    frame::response::result::ColumnType,
};

pub fn anyhow_py_future<F, T>(py: Python<'_>, fut: F) -> anyhow::Result<&PyAny>
where
    F: Future<Output = anyhow::Result<T>> + Send + 'static,
    T: IntoPy<PyObject>,
{
    let res = pyo3_asyncio::tokio::future_into_py(py, async { fut.await.map_err(Into::into) })
        .map(Into::into)?;
    Ok(res)
}

pub fn py_to_cql_value(item: &PyAny) -> anyhow::Result<Box<dyn Value + Send>> {
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
