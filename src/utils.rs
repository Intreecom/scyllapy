use std::{collections::HashMap, future::Future, hash::BuildHasherDefault, str::FromStr};

use pyo3::{
    types::{
        PyAnyMethods, PyBool, PyBytes, PyDict, PyDictMethods, PyFloat, PyInt, PyList,
        PyListMethods, PyModule, PyModuleMethods, PySet, PyString, PyStringMethods, PyTuple,
        PyTupleMethods, PyTypeMethods,
    },
    Bound, IntoPyObject, IntoPyObjectExt, Py, PyAny, PyErr, PyResult, Python,
};
use scylla::{
    frame::{
        response::result::{ColumnSpec, ColumnType, CqlValue},
        value::{CqlDuration, LegacySerializedValues, Value},
    },
    BufMut,
};

use std::net::IpAddr;

use crate::{
    exceptions::rust_err::{ScyllaPyError, ScyllaPyResult},
    extra_types::{BigInt, Counter, Double, ScyllaPyUnset, SmallInt, TinyInt},
};

const DATE_FORMAT: &[::time::format_description::FormatItem<'static>] =
    ::time::macros::format_description!(version = 2, "[year]-[month]-[day]");

/// Add submodule.
///
/// This function is required,
/// because by default for native libs python
/// adds module as an attribute and
/// doesn't add it's submodules in list
/// of all available modules.
///
/// To surpass this issue, we
/// maually update `sys.modules` attribute,
/// adding all submodules.
///
/// # Errors
///
/// May result in an error, if
/// cannot construct modules, or add it,
/// or modify `sys.modules` attr.
pub fn add_submodule(
    py: Python<'_>,
    parent_mod: &Bound<'_, PyModule>,
    name: &'static str,
    module_constuctor: impl FnOnce(Python<'_>, &Bound<'_, PyModule>) -> PyResult<()>,
) -> PyResult<()> {
    let sub_module = PyModule::new(py, name)?;
    module_constuctor(py, &sub_module)?;
    parent_mod.add_submodule(&sub_module)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item(format!("{}.{name}", parent_mod.name()?), &sub_module)?;
    Ok(())
}

/// Small function to integrate custom result type
/// and `pyo3_async_runtimes`.
///
/// It's almost the same as `future_into_py`,
/// but it expects future to return `ScyllaPyResult` type, rather
/// than `PyResult` from `pyo3`. It's useful for using `?` operators all over the place.
///
/// # Errors
///
/// If result of a future was unsuccessful, it propagates the error.
pub fn scyllapy_future<F, T>(py: Python<'_>, fut: F) -> ScyllaPyResult<Bound<'_, PyAny>>
where
    F: Future<Output = ScyllaPyResult<T>> + Send + 'static,
    T: for<'py> IntoPyObject<'py> + Send + 'static,
{
    let res =
        pyo3_async_runtimes::tokio::future_into_py(py, async { fut.await.map_err(Into::into) })?;
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
    Decimal(bigdecimal_04::BigDecimal),
    Duration {
        months: i32,
        days: i32,
        nanoseconds: i64,
    },
    Float(eq_float::F32),
    Bytes(Vec<u8>),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
    Timestamp(chrono::DateTime<chrono::Utc>),
    Uuid(uuid::Uuid),
    Inet(IpAddr),
    List(Vec<Self>),
    Map(Vec<(Self, Self)>),
    // UDT holds serialized bytes according to the protocol.
    Udt(Vec<u8>),
}

impl Value for ScyllaPyCQLDTO {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), scylla::_macro_internal::ValueTooBig> {
        match self {
            Self::String(string) => string.serialize(buf),
            Self::BigInt(bigint) => bigint.serialize(buf),
            Self::Int(int) => int.serialize(buf),
            Self::SmallInt(smallint) => smallint.serialize(buf),
            Self::Bool(blob) => blob.serialize(buf),
            Self::Double(double) => double.0.serialize(buf),
            Self::Float(float) => float.0.serialize(buf),
            Self::Bytes(bytes) => bytes.serialize(buf),
            Self::Uuid(uuid) => uuid.serialize(buf),
            Self::Inet(inet) => inet.serialize(buf),
            Self::List(list) => list.serialize(buf),
            Self::Counter(counter) => counter.serialize(buf),
            Self::TinyInt(tinyint) => tinyint.serialize(buf),
            Self::Date(date) => date.serialize(buf),
            Self::Time(time) => time.serialize(buf),
            Self::Map(map) => map
                .iter()
                .cloned()
                .collect::<HashMap<_, _, BuildHasherDefault<rustc_hash::FxHasher>>>()
                .serialize(buf),
            Self::Timestamp(timestamp) => {
                scylla::frame::value::CqlTimestamp::from(*timestamp).serialize(buf)
            }
            Self::Null => Option::<bool>::None.serialize(buf),
            Self::Udt(udt) => {
                buf.extend(udt);
                Ok(())
            }
            Self::Decimal(decimal) => decimal.serialize(buf),
            Self::Unset => scylla::frame::value::Unset.serialize(buf),
            Self::Duration {
                months,
                days,
                nanoseconds,
            } => CqlDuration {
                months: *months,
                days: *days,
                nanoseconds: *nanoseconds,
            }
            .serialize(buf),
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
#[allow(clippy::too_many_lines)]
pub fn py_to_value(
    item: &Bound<'_, PyAny>,
    column_type: Option<&ColumnType>,
) -> ScyllaPyResult<ScyllaPyCQLDTO> {
    if item.is_none() {
        Ok(ScyllaPyCQLDTO::Null)
    } else if item.is_instance_of::<PyString>() {
        Ok(ScyllaPyCQLDTO::String(item.extract::<String>()?))
    } else if item.is_instance_of::<ScyllaPyUnset>() {
        Ok(ScyllaPyCQLDTO::Unset)
    } else if item.is_instance_of::<PyBool>() {
        Ok(ScyllaPyCQLDTO::Bool(item.extract::<bool>()?))
    } else if item.is_instance_of::<PyInt>() {
        match column_type {
            Some(ColumnType::TinyInt) => Ok(ScyllaPyCQLDTO::TinyInt(item.extract::<i8>()?)),
            Some(ColumnType::SmallInt) => Ok(ScyllaPyCQLDTO::SmallInt(item.extract::<i16>()?)),
            Some(ColumnType::BigInt) => Ok(ScyllaPyCQLDTO::BigInt(item.extract::<i64>()?)),
            Some(ColumnType::Counter) => Ok(ScyllaPyCQLDTO::Counter(item.extract::<i64>()?)),
            Some(_) | None => Ok(ScyllaPyCQLDTO::Int(item.extract::<i32>()?)),
        }
    } else if item.is_instance_of::<PyFloat>() {
        match column_type {
            Some(ColumnType::Double) => Ok(ScyllaPyCQLDTO::Double(eq_float::F64(
                item.extract::<f64>()?,
            ))),
            Some(_) | None => Ok(ScyllaPyCQLDTO::Float(eq_float::F32(item.extract::<f32>()?))),
        }
    } else if item.is_instance_of::<SmallInt>() {
        Ok(ScyllaPyCQLDTO::SmallInt(
            item.extract::<SmallInt>().map_err(PyErr::from)?.get_value(),
        ))
    } else if item.is_instance_of::<TinyInt>() {
        Ok(ScyllaPyCQLDTO::TinyInt(
            item.extract::<TinyInt>().map_err(PyErr::from)?.get_value(),
        ))
    } else if item.is_instance_of::<BigInt>() {
        Ok(ScyllaPyCQLDTO::BigInt(
            item.extract::<BigInt>().map_err(PyErr::from)?.get_value(),
        ))
    } else if item.is_instance_of::<Double>() {
        Ok(ScyllaPyCQLDTO::Double(eq_float::F64(
            item.extract::<Double>().map_err(PyErr::from)?.get_value(),
        )))
    } else if item.is_instance_of::<Counter>() {
        Ok(ScyllaPyCQLDTO::Counter(
            item.extract::<Counter>().map_err(PyErr::from)?.get_value(),
        ))
    } else if item.is_instance_of::<PyBytes>() {
        Ok(ScyllaPyCQLDTO::Bytes(item.extract::<Vec<u8>>()?))
    } else if item.hasattr("__dump_udt__")? {
        let dumped = item.call_method0("__dump_udt__")?;
        let dumped_py = dumped.cast::<PyList>().map_err(|err| {
            ScyllaPyError::BindingError(format!(
                "Cannot get UDT values. __dump_udt__ has returned not a list value. {err}"
            ))
        })?;
        let mut buf = Vec::new();
        // Here we put the size of UDT value.
        // Now it's zero, but we will replace it after serialization.
        buf.put_i32(0);
        for val in dumped_py {
            // Here we serialize all fields.
            py_to_value(&val, None)?
                .serialize(buf.as_mut())
                .map_err(|err| {
                    ScyllaPyError::BindingError(format!(
                        "Cannot serialize UDT field because of {err}"
                    ))
                })?;
        }
        // Then we calculate the size of the UDT value, cast it to i32
        // and put it in the beginning of the buffer.
        let buf_len: i32 = buf.len().try_into().map_err(|_| {
            ScyllaPyError::BindingError("Cannot serialize. UDT value is too big.".into())
        })?;
        // Here we also subtract 4 bytes, because we don't want to count
        // size buffer itself.
        buf[0..4].copy_from_slice(&(buf_len - 4).to_be_bytes()[..]);
        Ok(ScyllaPyCQLDTO::Udt(buf))
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
        Ok(ScyllaPyCQLDTO::Time(chrono::NaiveTime::from_str(
            item.call_method0("isoformat")?.extract::<&str>()?,
        )?))
    } else if item.get_type().name()? == "Decimal" {
        Ok(ScyllaPyCQLDTO::Decimal(
            bigdecimal_04::BigDecimal::from_str(item.str()?.to_str()?).map_err(|err| {
                ScyllaPyError::BindingError(format!("Cannot parse decimal {err}"))
            })?,
        ))
    } else if item.get_type().name()? == "datetime" {
        let milliseconds = item.call_method0("timestamp")?.extract::<f64>()? * 1000f64;
        #[allow(clippy::cast_possible_truncation)]
        let seconds = milliseconds as i64 / 1_000;
        #[allow(clippy::cast_possible_truncation)]
        #[allow(clippy::cast_sign_loss)]
        let nsecs = (milliseconds as i64 % 1_000) as u32 * 1_000_000;
        let timestamp = chrono::DateTime::<chrono::Utc>::from_timestamp(seconds, nsecs).ok_or(
            ScyllaPyError::BindingError("Cannot convert datetime to timestamp.".into()),
        )?;
        Ok(ScyllaPyCQLDTO::Timestamp(timestamp))
    } else if item.get_type().name()? == "relativedelta" {
        let months = item.getattr("months")?.extract::<i32>()?;
        let days = item.getattr("days")?.extract::<i32>()?;
        let nanoseconds = item.getattr("microseconds")?.extract::<i64>()? * 1_000
            + item.getattr("seconds")?.extract::<i64>()? * 1_000_000;
        Ok(ScyllaPyCQLDTO::Duration {
            months,
            days,
            nanoseconds,
        })
    } else if item.is_instance_of::<PyList>()
        || item.is_instance_of::<PyTuple>()
        || item.is_instance_of::<PySet>()
    {
        let mut items = Vec::new();
        for inner in item.try_iter()? {
            items.push(py_to_value(&inner?, column_type)?);
        }
        Ok(ScyllaPyCQLDTO::List(items))
    } else if item.is_instance_of::<PyDict>() {
        let dict = item
            .cast::<PyDict>()
            .map_err(|err| ScyllaPyError::BindingError(format!("Cannot cast to dict: {err}")))?;
        let mut items = Vec::new();
        for dict_item in dict.items() {
            let item_tuple = dict_item.cast::<PyTuple>().map_err(|err| {
                ScyllaPyError::BindingError(format!("Cannot cast to tuple: {err}"))
            })?;
            items.push((
                py_to_value(&item_tuple.get_item(0)?, column_type)?,
                py_to_value(&item_tuple.get_item(1)?, column_type)?,
            ));
        }
        Ok(ScyllaPyCQLDTO::Map(items))
    } else {
        let type_name = item.get_type().name()?;
        Err(ScyllaPyError::BindingError(format!(
            "Unsupported type for parameter binding: {type_name:?}"
        )))
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
    col_name: &'a str,
    cql_type: &'a ColumnType,
    cql_value: Option<&CqlValue>,
) -> ScyllaPyResult<Bound<'a, PyAny>> {
    let Some(unwrapped_value) = cql_value else {
        return Ok(py.None().into_bound(py));
    };
    match cql_type {
        ColumnType::Ascii => unwrapped_value
            .as_ascii()
            .ok_or(ScyllaPyError::ValueDowncastError(col_name.into(), "ASCII"))
            .map(|val| PyString::new(py, val).into_any()),
        ColumnType::Boolean => unwrapped_value
            .as_boolean()
            .ok_or(ScyllaPyError::ValueDowncastError(
                col_name.into(),
                "Boolean",
            ))
            .and_then(|val| val.into_bound_py_any(py).map_err(Into::into)),
        ColumnType::Blob => unwrapped_value
            .as_blob()
            .ok_or(ScyllaPyError::ValueDowncastError(col_name.into(), "Blob"))
            .map(|val| PyBytes::new(py, val.as_ref()).into_any()),
        ColumnType::Double => unwrapped_value
            .as_double()
            .ok_or(ScyllaPyError::ValueDowncastError(col_name.into(), "Double"))
            .and_then(|val| val.into_bound_py_any(py).map_err(Into::into)),
        ColumnType::Float => unwrapped_value
            .as_float()
            .ok_or(ScyllaPyError::ValueDowncastError(col_name.into(), "Float"))
            .and_then(|val| val.into_bound_py_any(py).map_err(Into::into)),
        ColumnType::Int => unwrapped_value
            .as_int()
            .ok_or(ScyllaPyError::ValueDowncastError(col_name.into(), "Int"))
            .and_then(|val| val.into_bound_py_any(py).map_err(Into::into)),
        ColumnType::BigInt => unwrapped_value
            .as_bigint()
            .ok_or(ScyllaPyError::ValueDowncastError(col_name.into(), "BigInt"))
            .and_then(|val| val.into_bound_py_any(py).map_err(Into::into)),
        ColumnType::Text => unwrapped_value
            .as_text()
            .ok_or(ScyllaPyError::ValueDowncastError(col_name.into(), "Text"))
            .and_then(|val| val.into_bound_py_any(py).map_err(Into::into)),
        ColumnType::List(column_type) => {
            let items = unwrapped_value
                .as_list()
                .ok_or(ScyllaPyError::ValueDowncastError(col_name.into(), "List"))?
                .iter()
                .map(|val| cql_to_py(py, col_name, column_type.as_ref(), Some(val)))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(items.into_bound_py_any(py)?)
        }
        ColumnType::Map(key_type, val_type) => {
            let map_values = unwrapped_value
                .as_map()
                .ok_or(ScyllaPyError::ValueDowncastError(col_name.into(), "Map"))?
                .iter()
                .map(
                    |(key, val)| -> ScyllaPyResult<(Bound<'a, PyAny>, Bound<'a, PyAny>)> {
                        Ok((
                            cql_to_py(py, col_name, key_type, Some(key))?,
                            cql_to_py(py, col_name, val_type, Some(val))?,
                        ))
                    },
                )
                .collect::<Result<Vec<_>, _>>()?;

            let res_map = PyDict::new(py);
            for (key, value) in map_values {
                res_map.set_item(key, value)?;
            }
            Ok(res_map.into_any())
        }
        ColumnType::Set(column_type) => {
            let items = unwrapped_value
                .as_set()
                .ok_or(ScyllaPyError::ValueDowncastError(col_name.into(), "Set"))?
                .iter()
                .map(|val| cql_to_py(py, col_name, column_type.as_ref(), Some(val)))
                .collect::<Result<Vec<_>, _>>()?;
            let res_set = PySet::new(py, items)?;
            Ok(res_set.into_any())
        }
        ColumnType::SmallInt => unwrapped_value
            .as_smallint()
            .ok_or(ScyllaPyError::ValueDowncastError(
                col_name.into(),
                "SmallInt",
            ))
            .and_then(|val| val.into_bound_py_any(py).map_err(Into::into)),
        ColumnType::TinyInt => unwrapped_value
            .as_tinyint()
            .ok_or(ScyllaPyError::ValueDowncastError(
                col_name.into(),
                "TinyInt",
            ))
            .and_then(|val| val.into_bound_py_any(py).map_err(Into::into)),
        ColumnType::Uuid => {
            let uuid_str = unwrapped_value
                .as_uuid()
                .ok_or(ScyllaPyError::ValueDowncastError(col_name.into(), "Uuid"))?
                .simple()
                .to_string();
            Ok(py.import("uuid")?.getattr("UUID")?.call1((uuid_str,))?)
        }
        ColumnType::Timeuuid => {
            let uuid_str = unwrapped_value
                .as_timeuuid()
                .ok_or(ScyllaPyError::ValueDowncastError(
                    col_name.into(),
                    "Timeuuid",
                ))?
                .as_ref()
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
            let duration =
                unwrapped_value
                    .as_cql_duration()
                    .ok_or(ScyllaPyError::ValueDowncastError(
                        col_name.into(),
                        "Duration",
                    ))?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("months", duration.months)?;
            kwargs.set_item("days", duration.days)?;
            kwargs.set_item("microseconds", duration.nanoseconds / 1_000)?;
            Ok(py
                .import("dateutil")?
                .getattr("relativedelta")?
                .getattr("relativedelta")?
                .call((), Some(&kwargs))?)
        }
        ColumnType::Timestamp => {
            // Timestamp - num of milliseconds since unix epoch.
            let timestamp =
                unwrapped_value
                    .as_cql_timestamp()
                    .ok_or(ScyllaPyError::ValueDowncastError(
                        col_name.into(),
                        "Timestamp",
                    ))?;
            let milliseconds = timestamp.0;
            if milliseconds < 0 {
                return Err(ScyllaPyError::ValueDowncastError(
                    col_name.into(),
                    "Timestamp cannot be less than 0",
                ));
            }
            let seconds =
                milliseconds
                    .checked_div(1_000)
                    .ok_or(ScyllaPyError::ValueDowncastError(
                        col_name.into(),
                        "Cannot get seconds out of milliseconds.",
                    ))?;
            #[allow(clippy::cast_possible_truncation)]
            #[allow(clippy::cast_sign_loss)]
            let nsecs = (milliseconds % 1_000).checked_mul(1_000_000).ok_or(
                ScyllaPyError::ValueDowncastError(col_name.into(), "Cannot construct nanoseconds"),
            )? as u32;

            let timestamp = chrono::DateTime::<chrono::Utc>::from_timestamp(seconds, nsecs).ok_or(
                ScyllaPyError::ValueDowncastError(
                    col_name.into(),
                    "Cannot construct datetime based on timestamp",
                ),
            )?;
            #[allow(clippy::cast_precision_loss)]
            Ok(py.import("datetime")?.getattr("datetime")?.call_method1(
                "fromtimestamp",
                (timestamp.timestamp_millis() as f64 / 1000f64,),
            )?)
        }
        ColumnType::Inet => unwrapped_value
            .as_inet()
            .ok_or(ScyllaPyError::ValueDowncastError(col_name.into(), "Inet"))
            .and_then(|val| val.into_bound_py_any(py).map_err(Into::into)),
        ColumnType::Date => {
            let formatted_date = unwrapped_value
                .as_date()
                .ok_or(ScyllaPyError::ValueDowncastError(col_name.into(), "Date"))?
                .format(DATE_FORMAT)
                .map_err(|_| ScyllaPyError::ValueDowncastError(col_name.into(), "Date"))?;
            Ok(py
                .import("datetime")?
                .getattr("date")?
                .call_method1("fromisoformat", (formatted_date,))?)
        }
        ColumnType::Tuple(types) => {
            if let CqlValue::Tuple(data) = unwrapped_value {
                let mut dumped_elemets = Vec::new();
                for (col_type, col_val) in types.iter().zip(data) {
                    dumped_elemets.push(cql_to_py(py, col_name, col_type, col_val.as_ref())?);
                }
                Ok(PyTuple::new(py, dumped_elemets)?.into_any())
            } else {
                Err(ScyllaPyError::ValueDowncastError(col_name.into(), "Tuple"))
            }
        }
        ColumnType::Counter => unwrapped_value
            .as_counter()
            .ok_or(ScyllaPyError::ValueDowncastError(
                col_name.into(),
                "Counter",
            ))
            .and_then(|val| val.0.into_bound_py_any(py).map_err(Into::into)),
        ColumnType::Time => {
            let time = unwrapped_value
                .as_time()
                .ok_or(ScyllaPyError::ValueDowncastError(col_name.into(), "Time"))?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("hour", time.hour())?;
            kwargs.set_item("minute", time.minute())?;
            kwargs.set_item("second", time.second())?;
            kwargs.set_item("microsecond", time.microsecond())?;
            Ok(py
                .import("datetime")?
                .getattr("time")?
                .call((), Some(&kwargs))?)
        }
        ColumnType::UserDefinedType {
            type_name,
            keyspace,
            field_types,
        } => {
            let mut fields: HashMap<&str, &ColumnType, _> = HashMap::with_capacity_and_hasher(
                field_types.len(),
                BuildHasherDefault::<rustc_hash::FxHasher>::default(),
            );
            for (field_name, field_type) in field_types {
                fields.insert(field_name.as_str(), field_type);
            }
            let map_values = unwrapped_value
                .as_udt()
                .ok_or(ScyllaPyError::ValueDowncastError(col_name.into(), "UDT"))?
                .iter()
                .map(|(key, val)| -> ScyllaPyResult<(&str, Bound<'a, PyAny>)> {
                    let column_type = fields.get(key.as_str()).ok_or_else(|| {
                        ScyllaPyError::UDTDowncastError(
                            format!("{keyspace}.{type_name}"),
                            col_name.into(),
                            format!("UDT field {key} is not defined in schema"),
                        )
                    })?;
                    Ok((
                        key.as_str(),
                        cql_to_py(py, col_name, column_type, val.as_ref())?,
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?;

            let res_map = PyDict::new(py);
            for (key, value) in map_values {
                res_map.set_item(key, value)?;
            }
            Ok(res_map.into_any())
        }
        ColumnType::Decimal => {
            // Because the `as_decimal` method is not implemented for `CqlValue`,
            // will make a PR.
            let decimal: bigdecimal_04::BigDecimal = match unwrapped_value {
                CqlValue::Decimal(inner) => inner.clone().into(),
                _ => {
                    return Err(ScyllaPyError::ValueDowncastError(
                        col_name.into(),
                        "Decimal",
                    ))
                }
            };
            Ok(py
                .import("decimal")?
                .getattr("Decimal")?
                .call1((decimal.to_scientific_notation(),))?)
        }
        ColumnType::Varint => {
            let bigint: bigdecimal_04::num_bigint::BigInt = match unwrapped_value {
                CqlValue::Varint(inner) => inner.clone().into(),
                _ => return Err(ScyllaPyError::ValueDowncastError(col_name.into(), "Varint")),
            };
            Ok(py
                .import("builtins")?
                .getattr("int")?
                .call1((bigint.to_string(),))?)
        }
        ColumnType::Custom(_) => Err(ScyllaPyError::ValueDowncastError(
            col_name.into(),
            "Unknown",
        )),
    }
}

/// Parse python type to `LegacySerializedValues`.
///
/// Serialized values are used for
/// parameter binding. We parse python types
/// into our own types that are capable
/// of being bound to query and add parsed
/// results to `LegacySerializedValues`.
///
/// # Errors
///
/// May result in error if any of parameters cannot
/// be parsed.
pub fn parse_python_query_params(
    params: Option<&Bound<'_, PyAny>>,
    allow_dicts: bool,
    col_spec: Option<&[ColumnSpec]>,
) -> ScyllaPyResult<LegacySerializedValues> {
    let mut values = LegacySerializedValues::new();

    let Some(params) = params else {
        return Ok(values);
    };

    // If list was passed, we construct only unnamed parameters.
    // Otherwise it parses dict to named parameters.
    if params.is_instance_of::<PyList>() || params.is_instance_of::<PyTuple>() {
        let params = params.extract::<Vec<Bound<'_, PyAny>>>()?;
        for (index, param) in params.iter().enumerate() {
            let coltype = col_spec.and_then(|specs| specs.get(index)).map(|f| &f.typ);
            let py_dto = py_to_value(param, coltype)?;
            values.add_value(&py_dto)?;
        }
        return Ok(values);
    } else if params.is_instance_of::<PyDict>() {
        if allow_dicts {
            let types_map = col_spec
                .map(|specs| {
                    specs
                        .iter()
                        .map(|spec| (spec.name.as_str(), spec.typ.clone()))
                        .collect::<HashMap<_, _, BuildHasherDefault<rustc_hash::FxHasher>>>()
                })
                .unwrap_or_default();
            // let map = HashMap::with_capacity_and_hasher(, hasher)
            let dict = params.extract::<HashMap<
                String,
                Bound<'_, PyAny>,
                BuildHasherDefault<rustc_hash::FxHasher>,
            >>()?;
            for (name, value) in dict {
                values.add_named_value(
                    name.to_lowercase().as_str(),
                    &py_to_value(&value, types_map.get(name.as_str()))?,
                )?;
            }
            return Ok(values);
        }
        return Err(ScyllaPyError::BindingError(
            "Dicts are not allowed here.".into(),
        ));
    }
    let type_name = params.get_type().name()?;
    Err(ScyllaPyError::BindingError(format!(
        "Unsupported type for paramter binding: {type_name}. Use list, tuple or dict."
    )))
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
    rows: &'a Bound<'a, PyAny>,
    as_class: &'a Py<PyAny>,
) -> ScyllaPyResult<Vec<Py<PyAny>>> {
    let mapped_rows = rows
        .cast::<PyList>()
        .map_err(|err| {
            ScyllaPyError::RowsDowncastError(format!("Cannot downcast rows to list. {err}"))
        })?
        .iter()
        .map(|obj| {
            let kwargs = obj.cast::<PyDict>().map_err(|err| {
                ScyllaPyError::RowsDowncastError(format!(
                    "Cannot preapre kwargs for mapping. {err}"
                ))
            })?;
            as_class.call(py, (), Some(kwargs))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(mapped_rows)
}
