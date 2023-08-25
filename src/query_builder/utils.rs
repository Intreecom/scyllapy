use pyo3::FromPyObject;

#[derive(FromPyObject, Debug, Clone)]
pub enum Timeout {
    #[pyo3(transparent)]
    Int(i32),
    #[pyo3(transparent)]
    Str(String),
}
