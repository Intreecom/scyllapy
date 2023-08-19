use std::future::Future;

use pyo3::{
    types::{PyBool, PyBytes, PyFloat, PyInt, PyList, PySet, PyString, PyTuple},
    IntoPy, PyAny, PyObject, Python, ToPyObject,
};
use scylla::{
    _macro_internal::{CqlValue, Value},
    frame::response::result::ColumnType,
};

pub fn anyhow_py_future<F, T>(py: Python, fut: F) -> anyhow::Result<&PyAny>
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

#[inline]
pub fn cql_to_py<'a>(
    py: Python<'a>,
    cql_type: &'a ColumnType,
    cql_value: Option<CqlValue>,
) -> anyhow::Result<&'a PyAny> {
    if cql_value.is_none() {
        return Ok(py.None().into_ref(py));
    }
    let unwrapped_value = cql_value.unwrap();
    match cql_type {
        ColumnType::Custom(_) => Err(anyhow::anyhow!("Custom types are not yet supported.")),
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
        ColumnType::Counter => todo!(),
        ColumnType::Date => todo!(),
        ColumnType::Decimal => todo!(),
        ColumnType::Double => todo!(),
        ColumnType::Duration => todo!(),
        ColumnType::Float => todo!(),
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
        ColumnType::Timestamp => todo!(),
        ColumnType::Inet => todo!(),
        ColumnType::List(column_types) => {
            let items = unwrapped_value
                .as_list()
                .ok_or(anyhow::anyhow!("Cannot parse value."))?
                .into_iter()
                .map(|val| cql_to_py(py, column_types.as_ref(), Some(val.to_owned())))
                .collect::<Result<Vec<_>, _>>();
            Ok(items?.to_object(py).into_ref(py))
        }
        ColumnType::Map(_, _) => todo!(),
        ColumnType::Set(_) => todo!(),
        ColumnType::UserDefinedType {
            type_name: _,
            keyspace: _,
            field_types: _,
        } => todo!(),
        ColumnType::SmallInt => todo!(),
        ColumnType::TinyInt => todo!(),
        ColumnType::Time => todo!(),
        ColumnType::Timeuuid => todo!(),
        ColumnType::Tuple(_) => todo!(),
        ColumnType::Uuid => {
            let uuid_str = unwrapped_value
                .as_uuid()
                .ok_or(anyhow::anyhow!(""))?
                .simple()
                .to_string();
            Ok(py.import("uuid")?.getattr("UUID")?.call1((uuid_str,))?)
        }
        ColumnType::Varint => todo!(),
    }
}
