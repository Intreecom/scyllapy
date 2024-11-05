from importlib.metadata import version

from . import extra_types
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
    SSLVerifyMode,
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
    "SSLVerifyMode",
    "extra_types",
    "InlineBatch",
    "ExecutionProfile",
]
