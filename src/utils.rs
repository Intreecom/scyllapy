use std::{collections::HashMap, future::Future, str::FromStr};

use chrono::Duration;
use pyo3::{
    types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PySet, PyString, PyTuple},
    IntoPy, Py, PyAny, PyObject, Python, ToPyObject,
};
use scylla::frame::{
    response::result::{ColumnType, CqlValue},
    value::{SerializedValues, Value},
};

use std::net::IpAddr;

use crate::extra_types::{BigInt, Counter, Double, ScyllaPyUnset, SmallInt, TinyInt};

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

/// This class is used to transfer
/// data between python and rust.
///
/// This enum implements Value interface,
/// and any of it's variants can
/// be bound to query.
#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub enum ScyllaPyCQLDTO {
    Null,
    Unset,
    String(String),
    BigInt(i64),
    Int(i32),
    SmallInt(i16),
    TinyInt(i8),
    Counter(i64),
    Bool(bool),
    Double(eq_float::F64),
    Float(eq_float::F32),
    Bytes(Vec<u8>),
    Date(chrono::NaiveDate),
    Time(chrono::Duration),
    Timestamp(chrono::Duration),
    Uuid(uuid::Uuid),
    Inet(IpAddr),
    List(Vec<ScyllaPyCQLDTO>),
    Map(Vec<(ScyllaPyCQLDTO, ScyllaPyCQLDTO)>),
}

impl Value for ScyllaPyCQLDTO {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), scylla::_macro_internal::ValueTooBig> {
        match self {
            ScyllaPyCQLDTO::String(string) => string.serialize(buf),
            ScyllaPyCQLDTO::BigInt(bigint) => bigint.serialize(buf),
            ScyllaPyCQLDTO::Int(int) => int.serialize(buf),
            ScyllaPyCQLDTO::SmallInt(smallint) => smallint.serialize(buf),
            ScyllaPyCQLDTO::Bool(blob) => blob.serialize(buf),
            ScyllaPyCQLDTO::Double(double) => double.0.serialize(buf),
            ScyllaPyCQLDTO::Float(float) => float.0.serialize(buf),
            ScyllaPyCQLDTO::Bytes(bytes) => bytes.serialize(buf),
            ScyllaPyCQLDTO::Uuid(uuid) => uuid.serialize(buf),
            ScyllaPyCQLDTO::Inet(inet) => inet.serialize(buf),
            ScyllaPyCQLDTO::List(list) => list.serialize(buf),
            ScyllaPyCQLDTO::Counter(counter) => counter.serialize(buf),
            ScyllaPyCQLDTO::TinyInt(tinyint) => tinyint.serialize(buf),
            ScyllaPyCQLDTO::Date(date) => date.serialize(buf),
            ScyllaPyCQLDTO::Time(time) => scylla::frame::value::Time(*time).serialize(buf),
            ScyllaPyCQLDTO::Map(map) => map
                .iter()
                .cloned()
                .collect::<HashMap<_, _>>()
                .serialize(buf),
            ScyllaPyCQLDTO::Timestamp(timestamp) => {
                scylla::frame::value::Timestamp(*timestamp).serialize(buf)
            }
            ScyllaPyCQLDTO::Null => Option::<i16>::None.serialize(buf),
            ScyllaPyCQLDTO::Unset => scylla::frame::value::Unset.serialize(buf),
        }
    }
}

/// Convert Python type to CQL parameter value.
///
/// It converts python object to another type,
/// which can be serialized as Value that can
/// be bound to `Query`.
///
/// # Errors
///
/// May raise an error, if
/// value cannot be converted or unnown type was passed.
pub fn py_to_value(item: &PyAny) -> anyhow::Result<ScyllaPyCQLDTO> {
    if item.is_none() {
        Ok(ScyllaPyCQLDTO::Null)
    } else if item.is_instance_of::<PyString>() {
        Ok(ScyllaPyCQLDTO::String(item.extract::<String>()?))
    } else if item.is_instance_of::<ScyllaPyUnset>() {
        Ok(ScyllaPyCQLDTO::Unset)
    } else if item.is_instance_of::<PyBool>() {
        Ok(ScyllaPyCQLDTO::Bool(item.extract::<bool>()?))
    } else if item.is_instance_of::<PyInt>() {
        Ok(ScyllaPyCQLDTO::Int(item.extract::<i32>()?))
    } else if item.is_instance_of::<PyFloat>() {
        Ok(ScyllaPyCQLDTO::Float(eq_float::F32(item.extract::<f32>()?)))
    } else if item.is_instance_of::<SmallInt>() {
        Ok(ScyllaPyCQLDTO::SmallInt(
            item.extract::<SmallInt>()?.get_value(),
        ))
    } else if item.is_instance_of::<TinyInt>() {
        Ok(ScyllaPyCQLDTO::TinyInt(
            item.extract::<TinyInt>()?.get_value(),
        ))
    } else if item.is_instance_of::<BigInt>() {
        Ok(ScyllaPyCQLDTO::BigInt(
            item.extract::<BigInt>()?.get_value(),
        ))
    } else if item.is_instance_of::<Double>() {
        Ok(ScyllaPyCQLDTO::Double(eq_float::F64(
            item.extract::<Double>()?.get_value(),
        )))
    } else if item.is_instance_of::<Counter>() {
        Ok(ScyllaPyCQLDTO::Counter(
            item.extract::<Counter>()?.get_value(),
        ))
    } else if item.is_instance_of::<PyBytes>() {
        Ok(ScyllaPyCQLDTO::Bytes(item.extract::<Vec<u8>>()?))
    } else if item.get_type().name()? == "UUID" {
        Ok(ScyllaPyCQLDTO::Uuid(uuid::Uuid::parse_str(
            item.str()?.extract::<&str>()?,
        )?))
    } else if item.get_type().name()? == "IPv4Address" || item.get_type().name()? == "IPv6Address" {
        Ok(ScyllaPyCQLDTO::Inet(IpAddr::from_str(
            item.str()?.extract::<&str>()?,
        )?))
    } else if item.get_type().name()? == "date" {
        Ok(ScyllaPyCQLDTO::Date(chrono::NaiveDate::from_str(
            item.call_method0("isoformat")?.extract::<&str>()?,
        )?))
    } else if item.get_type().name()? == "time" {
        Ok(ScyllaPyCQLDTO::Time(
            chrono::NaiveTime::from_str(item.call_method0("isoformat")?.extract::<&str>()?)?
                .signed_duration_since(
                    chrono::NaiveTime::from_num_seconds_from_midnight_opt(0, 0)
                        .ok_or(anyhow::anyhow!("Cannot calculate offset from midnight."))?,
                ),
        ))
    } else if item.get_type().name()? == "datetime" {
        let milliseconds = item.call_method0("timestamp")?.extract::<f64>()? * 1000f64;
        #[allow(clippy::cast_possible_truncation)]
        let timestamp = Duration::milliseconds(milliseconds.trunc() as i64);
        Ok(ScyllaPyCQLDTO::Timestamp(timestamp))
    } else if item.is_instance_of::<PyList>()
        || item.is_instance_of::<PyTuple>()
        || item.is_instance_of::<PySet>()
    {
        let mut items = Vec::new();
        for inner in item.iter()? {
            items.push(py_to_value(inner?)?);
        }
        Ok(ScyllaPyCQLDTO::List(items))
    } else if item.is_instance_of::<PyDict>() {
        let dict = item
            .downcast::<PyDict>()
            .map_err(|err| anyhow::anyhow!("Cannot cast to dict: {err}"))?;
        let mut items = Vec::new();
        for dict_item in dict.items().iter() {
            let item_tuple = dict_item
                .downcast::<PyTuple>()
                .map_err(|err| anyhow::anyhow!("Cannot cast to tuple: {err}"))?;
            items.push((
                py_to_value(item_tuple.get_item(0)?)?,
                py_to_value(item_tuple.get_item(1)?)?,
            ));
        }
        Ok(ScyllaPyCQLDTO::Map(items))
    } else {
        let type_name = item.get_type().name()?;
        Err(anyhow::anyhow!(
            "Unsupported type for parameter binding: {type_name:?}"
        ))
    }
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
    cql_value: Option<&CqlValue>,
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
                .map(|val| cql_to_py(py, column_type.as_ref(), Some(val)))
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
                        cql_to_py(py, key_type, Some(key))?,
                        cql_to_py(py, val_type, Some(val))?,
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
                .map(|val| cql_to_py(py, column_type.as_ref(), Some(val)))
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
        ColumnType::Timeuuid => {
            let uuid_str = unwrapped_value
                .as_timeuuid()
                .ok_or(anyhow::anyhow!("Cannot parse timeuuid"))?
                .as_simple()
                .to_string();
            Ok(py.import("uuid")?.getattr("UUID")?.call1((uuid_str,))?)
        }
        ColumnType::Duration => {
            // We loose some perscision on converting it to
            // python datetime, because in scylla,
            // durations is stored in nanoseconds.
            // But that's ok, because we assume that
            // all values were inserted using
            // same driver. Will fix it on demand.
            let duration = unwrapped_value
                .as_duration()
                .ok_or(anyhow::anyhow!("Cannot parse duration"))?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("microseconds", duration.num_microseconds())?;
            Ok(py
                .import("datetime")?
                .getattr("timedelta")?
                .call((), Some(kwargs))?)
        }
        ColumnType::Timestamp => {
            // Timestamp - num of milliseconds since unix epoch.
            let timestamp = unwrapped_value
                .as_duration()
                .ok_or(anyhow::anyhow!("Cannot parse timestamp"))?;
            #[allow(clippy::cast_precision_loss)]
            let seconds = timestamp.num_seconds() as f64;
            #[allow(clippy::cast_precision_loss)]
            let micros = (timestamp - Duration::seconds(timestamp.num_seconds())).num_milliseconds()
                as f64
                / 1_000f64; // Converting microseconds to seconds to construct timestamp
            Ok(py
                .import("datetime")?
                .getattr("datetime")?
                .call_method1("fromtimestamp", (seconds + micros,))?)
        }
        ColumnType::Inet => Ok(unwrapped_value
            .as_inet()
            .ok_or(anyhow::anyhow!("Cannot parse inet addres"))?
            .to_object(py)
            .into_ref(py)),
        ColumnType::Date => {
            let formatted_date = unwrapped_value
                .as_date()
                .ok_or(anyhow::anyhow!("Cannot parse date"))?
                .format("%Y-%m-%d")
                .to_string();
            Ok(py
                .import("datetime")?
                .getattr("date")?
                .call_method1("fromisoformat", (formatted_date,))?)
        }
        ColumnType::Tuple(types) => {
            if let CqlValue::Tuple(data) = unwrapped_value {
                let mut dumped_elemets = Vec::new();
                for (col_type, col_val) in types.iter().zip(data) {
                    dumped_elemets.push(cql_to_py(py, col_type, col_val.as_ref())?);
                }
                Ok(PyTuple::new(py, dumped_elemets))
            } else {
                Err(anyhow::anyhow!("Cannot parse as tuple."))
            }
        }
        ColumnType::Counter => Ok(unwrapped_value
            .as_counter()
            .ok_or(anyhow::anyhow!("Cannot parse counter"))?
            .0
            .to_object(py)
            .into_ref(py)),
        ColumnType::Time => {
            let duration = unwrapped_value
                .as_duration()
                .ok_or(anyhow::anyhow!("Cannot parse time"))?;
            let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(0, 0)
                .ok_or(anyhow::anyhow!("Cannot calculate midnight time"))?
                + duration;
            Ok(py
                .import("datetime")?
                .getattr("time")?
                .call_method1("fromisoformat", (time.format("%H:%M:%S%.6f").to_string(),))?)
        }
        ColumnType::Custom(_) => Err(anyhow::anyhow!("Custom types are not yet supported.")),
        ColumnType::Varint => Err(anyhow::anyhow!("Variant is not yet supported.")),
        ColumnType::Decimal => Err(anyhow::anyhow!("Decimals are not yet supported.")),
        ColumnType::UserDefinedType {
            type_name: _,
            keyspace: _,
            field_types: _,
        } => Err(anyhow::anyhow!("UDT is not yet supported.")),
    }
}

/// Parse python type to `SerializedValues`.
///
/// Serialized values are used for
/// parameter binding. We parse python types
/// into our own types that are capable
/// of being bound to query and add parsed
/// results to `SerializedValues`.
///
/// # Errors
///
/// May result in error if any of parameters cannot
/// be parsed.
pub fn parse_python_query_params(
    params: Option<&PyAny>,
    allow_dicts: bool,
) -> anyhow::Result<SerializedValues> {
    let mut values = SerializedValues::new();

    let Some(params) = params else {
        return Ok(values);
    };

    // If list was passed, we construct only unnamed parameters.
    // Otherwise it parses dict to named parameters.
    if params.is_instance_of::<PyList>() || params.is_instance_of::<PyTuple>() {
        let params = params.extract::<Vec<&PyAny>>()?;
        for param in params {
            let py_dto = py_to_value(param)?;
            values.add_value(&py_dto)?;
        }
        return Ok(values);
    } else if params.is_instance_of::<PyDict>() {
        if allow_dicts {
            let dict = params.extract::<HashMap<&str, &PyAny>>()?;
            for (name, value) in dict {
                values.add_named_value(name.to_lowercase().as_str(), &py_to_value(value)?)?;
            }
            return Ok(values);
        }
        return Err(anyhow::anyhow!("Dicts are not allowed here."));
    }
    let type_name = params.get_type().name()?;
    Err(anyhow::anyhow!(
        "Unsupported type for paramter binding: {type_name}. Use list, tuple or dict."
    ))
}

/// Map rows, using some python callable.
///
/// This function casts every row to dictionary
/// and passes it as key-word arguments to the
/// `as_class` function. Returned values are
/// then written in a new vector of mapped rows.
///
/// # Errors
/// May result in an error if
/// * `rows` object cannot be casted to `PyList`;
/// * At least one row cannot be casted to dict.
pub fn map_rows<'a>(
    py: Python<'a>,
    rows: &'a Py<PyAny>,
    as_class: &'a Py<PyAny>,
) -> anyhow::Result<Vec<Py<PyAny>>> {
    let mapped_rows = rows
        .downcast::<PyList>(py)
        .map_err(|_| anyhow::anyhow!("Cannot downcast rows to list."))?
        .iter()
        .map(|obj| {
            as_class.call(
                py,
                (),
                Some(
                    obj.downcast::<PyDict>()
                        .map_err(|_| anyhow::anyhow!("Cannot preapre kwargs for mapping."))?,
                ),
            )
        })
        .collect::<anyhow::Result<Vec<_>, _>>()?;
    Ok(mapped_rows)
}
