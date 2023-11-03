import dataclasses
from typing import Any, List

from ._internal.extra_types import BigInt, Counter, Double, SmallInt, TinyInt, Unset

try:
    import pydantic
except ImportError:
    pydantic = None


class ScyllaPyUDT:
    """
    Class for declaring UDT models.

    This class is a mixin for models like dataclasses and pydantic models,
    or classes that have `__slots__` attribute.

    It can be further extended to support other model types.
    """

    def __dump_udt__(self) -> List[Any]:
        """
        Method to dump UDT models to a dict.

        This method returns a list of values in the order of the UDT fields.
        Because in the protocol, UDT fields should be sent in the same order as
        they were declared.
        """
        if dataclasses.is_dataclass(self):
            values = []
            for field in dataclasses.fields(self):
                values.append(getattr(self, field.name))
            return values
        if pydantic is not None and isinstance(self, pydantic.BaseModel):
            values = []
            for param in self.__class__.__signature__.parameters:
                values.append(getattr(self, param))
            return values
        if hasattr(self, "__slots__"):
            values = []
            for slot in self.__slots__:
                values.append(getattr(self, slot))
            return values
        raise ValueError("Unsupported model type")


__all__ = ("BigInt", "Counter", "Double", "SmallInt", "TinyInt", "Unset", "ScyllaPyUDT")
