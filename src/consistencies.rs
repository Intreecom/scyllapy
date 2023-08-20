use pyo3::pyclass;
use scylla::frame::types::{
    Consistency as ScyllaConsistency, SerialConsistency as ScyllaSerialConsistency,
};

/// Consistency levels for queries.
///
/// This class allows to run queries
/// with specific consistency levels.
#[pyclass]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[allow(non_camel_case_types)]
pub enum Consistency {
    ANY,
    ONE,
    TWO,
    THREE,
    QUORUM,
    ALL,
    LOCAL_QUORUM,
    EACH_QUORUM,
    LOCAL_ONE,
}

#[pyclass]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[allow(non_camel_case_types)]
pub enum SerialConsistency {
    SERIAL,
    LOCAL_SERIAL,
}

/// Here we define how to convert our Consistency,
/// to the type that is used by scylla library.
impl From<Consistency> for ScyllaConsistency {
    fn from(value: Consistency) -> Self {
        match value {
            Consistency::ANY => Self::Any,
            Consistency::ONE => Self::One,
            Consistency::TWO => Self::Two,
            Consistency::THREE => Self::Three,
            Consistency::QUORUM => Self::Quorum,
            Consistency::ALL => Self::All,
            Consistency::LOCAL_QUORUM => Self::LocalQuorum,
            Consistency::EACH_QUORUM => Self::EachQuorum,
            Consistency::LOCAL_ONE => Self::LocalOne,
        }
    }
}

/// Convertion between python serial consistency
/// and scylla serial consistency.
impl From<SerialConsistency> for ScyllaSerialConsistency {
    fn from(value: SerialConsistency) -> Self {
        match value {
            SerialConsistency::SERIAL => Self::Serial,
            SerialConsistency::LOCAL_SERIAL => Self::LocalSerial,
        }
    }
}
