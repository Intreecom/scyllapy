from typing import Any, Callable, Iterable, Literal, Optional, TypeVar, overload

T = TypeVar("T")

class Scylla:
    """
    Scylla class.

    This class represents scylla cluster.
    And has internal connection pool.

    Everything that can beconfigured, shown below.
    """

    def __init__(
        self,
        contact_points: list[str],
        username: str | None = None,
        password: str | None = None,
        keyspace: str | None = None,
        ssl_cert: str | None = None,
        conn_timeout: int | None = None,
    ) -> None:
        """
        Configure cluster for later use.

        :param contact_points: List of known nodes. (Hosts and ports).
            ["192.168.1.1:9042", "my_keyspace.node:9042"]
        :param username: Plain text auth username.
        :param password: Plain text auth password.
        :param ssl_cert: Certficiate string to use
            for connection. AWS requires it.
        :param conn_timeout: Timeout in seconds.
        """
    async def startup(self) -> None:
        """Initialize the custer."""
    async def shutdown(self) -> None:
        """Shutdown the cluster."""
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
    ) -> Any:
        """
        Execute a query.

        This function takes a query string,
        and list of parameters.

        Parameters in query can be specified as ? signs.

        await scylla.execute("SELECT * FROM table WHERE id = ?", [11])

        :param query: query to use.
        :param params: list of query parameters.
        :param as_class: DTO class to use for parsing rows (Can be pydantic model or dataclass).
        """
