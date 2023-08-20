from ._internal import Scylla, Consistency, Query, SerialConsistency
from importlib.metadata import version

__version__ = version("scyllapy")

__all__ = [
    "__version__",
    "Scylla",
    "Consistency",
    "Query",
    "SerialConsistency",
]
