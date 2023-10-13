use pyo3::pyclass;
use scylla::statement::{Consistency, SerialConsistency};

/// Consistency levels for queries.
///
/// This class allows to run queries
/// with specific consistency levels.
#[pyclass(name = "Consistency")]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[allow(non_camel_case_types)]
pub enum ScyllaPyConsistency {
    ANY,
    ONE,
    TWO,
    THREE,
    QUORUM,
    ALL,
    LOCAL_QUORUM,
    EACH_QUORUM,
    LOCAL_ONE,
    SERIAL,
    LOCAL_SERIAL,
}

#[pyclass(name = "SerialConsistency")]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[allow(non_camel_case_types)]
pub enum ScyllaPySerialConsistency {
    SERIAL,
    LOCAL_SERIAL,
}

/// Here we define how to convert our Consistency,
/// to the type that is used by scylla library.
impl From<ScyllaPyConsistency> for Consistency {
    fn from(value: ScyllaPyConsistency) -> Self {
        match value {
            ScyllaPyConsistency::ANY => Self::Any,
            ScyllaPyConsistency::ONE => Self::One,
            ScyllaPyConsistency::TWO => Self::Two,
            ScyllaPyConsistency::THREE => Self::Three,
            ScyllaPyConsistency::QUORUM => Self::Quorum,
            ScyllaPyConsistency::ALL => Self::All,
            ScyllaPyConsistency::LOCAL_QUORUM => Self::LocalQuorum,
            ScyllaPyConsistency::EACH_QUORUM => Self::EachQuorum,
            ScyllaPyConsistency::LOCAL_ONE => Self::LocalOne,
            ScyllaPyConsistency::SERIAL => Self::Serial,
            ScyllaPyConsistency::LOCAL_SERIAL => Self::LocalSerial,
        }
    }
}

/// Convertion between python serial consistency
/// and scylla serial consistency.
impl From<ScyllaPySerialConsistency> for SerialConsistency {
    fn from(value: ScyllaPySerialConsistency) -> Self {
        match value {
            ScyllaPySerialConsistency::SERIAL => Self::Serial,
            ScyllaPySerialConsistency::LOCAL_SERIAL => Self::LocalSerial,
        }
    }
}
