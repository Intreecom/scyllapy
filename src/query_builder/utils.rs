use pyo3::FromPyObject;

use crate::utils::ScyllaPyCQLDTO;

#[derive(FromPyObject, Debug, Clone)]
pub enum Timeout {
    #[pyo3(transparent)]
    Int(i32),
    #[pyo3(transparent)]
    Str(String),
}

#[derive(Clone, Debug)]
pub enum IfCluase {
    Exists,
    Condition {
        clauses: Vec<String>,
        values: Vec<ScyllaPyCQLDTO>,
    },
}

impl IfCluase {
    #[must_use]
    pub fn extend_values(&self, query_values: Vec<ScyllaPyCQLDTO>) -> Vec<ScyllaPyCQLDTO> {
        match self {
            IfCluase::Exists => query_values,
            IfCluase::Condition { clauses: _, values } => {
                query_values.iter().chain(values.iter()).cloned().collect()
            }
        }
    }
}

/// Function for building
/// pretty queries.
///
/// It assembles all query parts in one string,
/// removing empty string and joining all query parts with space character.
pub fn pretty_build<'a>(query_parts: impl IntoIterator<Item = &'a str>) -> String {
    query_parts
        .into_iter()
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}
