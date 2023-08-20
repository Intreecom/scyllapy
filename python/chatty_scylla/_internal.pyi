from typing import Any, Callable, Iterable, Literal, Optional, TypeVar, overload

T = TypeVar("T")

class Scylla:
    def __init__(
        self,
        contact_points: list[str],
        username: str,
        password: str,
        keyspace: str,
        cert_data: str | None = None,
    ) -> None: ...
    async def startup(self) -> None: ...
    async def shutdown(self) -> None: ...
    @overload
    async def execute(
        self,
        query: str,
        params: Optional[Iterable[Any]] = None,
        as_class: Literal[None] = None,
    ) -> list[dict[str, Any]]: ...
    @overload
    async def execute(
        self,
        query: str,
        params: Optional[Iterable[Any]] = None,
        as_class: Optional[Callable[..., T]] = None,
    ) -> list[T]: ...
    async def execute(
        self,
        query: str,
        params: Optional[Iterable[Any]] = None,
        as_class: Any = None,
    ) -> Any: ...
