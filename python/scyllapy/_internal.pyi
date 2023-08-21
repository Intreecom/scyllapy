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
    async def prepare(self, query: str | Query) -> PreparedQuery: ...
    @overload
    async def execute(
        self,
        query: str | Query | PreparedQuery,
        params: Optional[Iterable[Any]] = None,
        as_class: Literal[None] = None,
    ) -> list[dict[str, Any]]: ...
    @overload
    async def execute(
        self,
        query: str | Query | PreparedQuery,
        params: Optional[Iterable[Any]] = None,
        as_class: Optional[Callable[..., T]] = None,
    ) -> list[T]: ...
    async def execute(
        self,
        query: str | Query,
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

class Query:
    """
    Query class.

    It's used for fine-tuning specific queries.
    If you don't need a specific consistency, or
    any other parameter, you can pass a string instead.
    """

    query: str
    consistency: Consistency | None
    serial_consistency: SerialConsistency | None
    request_timeout: int | None

    def __init__(
        self,
        query: str,
        consistency: Consistency | None = None,
        serial_consistency: SerialConsistency | None = None,
        request_timeout: int | None = None,
        timestamp: int | None = None,
        is_idempotent: bool | None = None,
        tracing: bool | None = None,
    ) -> None: ...
    def with_consistency(self, consistency: Consistency | None) -> Query: ...
    def with_serial_consistency(
        self,
        serial_consistency: SerialConsistency | None,
    ) -> Query: ...
    def with_request_timeout(self, request_timeout: int | None) -> Query: ...
    def with_timestamp(self, timestamp: int | None) -> Query: ...
    def with_is_idempotent(self, is_idempotent: bool | None) -> Query: ...
    def with_tracing(self, tracing: bool | None) -> Query: ...

class Consistency:
    """Consistency for query."""

    ANY: "Consistency"
    ONE: "Consistency"
    TWO: "Consistency"
    THREE: "Consistency"
    QUORUM: "Consistency"
    ALL: "Consistency"
    LOCAL_QUORUM: "Consistency"
    EACH_QUORUM: "Consistency"
    LOCAL_ONE: "Consistency"

class SerialConsistency:
    """Serial consistency for query."""

    SERIAL: "SerialConsistency"
    LOCAL_SERIAL: "SerialConsistency"

class PreparedQuery:
    """Class that represents prepared statement."""
