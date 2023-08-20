from ._internal import Scylla
from importlib.metadata import version

__version__ = version("scyllapy")

__all__ = ["Scylla", "__version__"]
