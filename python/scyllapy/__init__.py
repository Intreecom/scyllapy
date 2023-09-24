from importlib.metadata import version

from ._internal import (
    Batch,
    BatchType,
    Consistency,
    ExecutionProfile,
    InlineBatch,
    PreparedQuery,
    Query,
    QueryResult,
    Scylla,
    SerialConsistency,
)

__version__ = version("scyllapy")

__all__ = [
    "__version__",
    "Scylla",
    "Consistency",
    "Query",
    "SerialConsistency",
    "PreparedQuery",
    "Batch",
    "BatchType",
    "QueryResult",
    "extra_types",
    "InlineBatch",
    "ExecutionProfile",
]
