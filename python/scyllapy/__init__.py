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
    "Batch",
    "BatchType",
    "Consistency",
    "ExecutionProfile",
    "InlineBatch",
    "PreparedQuery",
    "Query",
    "QueryResult",
    "SSLVerifyMode",
    "Scylla",
    "SerialConsistency",
    "__version__",
    "extra_types",
]
