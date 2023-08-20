from ._internal import Scylla, Consistency
from importlib.metadata import version

__version__ = version("scyllapy")

__all__ = ["Scylla", "Consistency", "__version__"]
