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
        write_coalescing: bool | None = None,
        pool_size_per_host: int | None = None,
        pool_size_per_shard: int | None = None,
        keepalive_interval: int | None = None,
        keepalive_timeout: int | None = None,
        tcp_keepalive_interval: int | None = None,
        tcp_nodelay: bool | None = None,
        disallow_shard_aware_port: bool | None = None,
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
        :param write_coalescing:
            If true, the driver will inject a small delay before flushing data
            to the socket - by rescheduling the task that writes data to the socket.
            This gives the task an opportunity to collect more write requests
            and write them in a single syscall, increasing the efficiency.
        :param pool_size_per_host: how many connections should be established
            to the node.
        :param pool_size_per_host: how many connections should be established
            to each shard of the node.
        :param keepalive_interval: How ofter to send keepalive messages,
            when connection is idling. In seconds.
        :param keepalive_timeout: sets keepalive timeout.
        :param tcp_keepalive_interval: Sets TCP keepalive interval.
        :param tcp_nodelay: sets TCP nodelay flag.
        :param disallow_shard_aware_port: If true, prevents the driver from connecting to the shard-aware port,
            even if the node supports it.
        """
    async def startup(self) -> None:
        """Initialize the custer."""
    async def shutdown(self) -> None:
        """Shutdown the cluster."""
    async def prepare(self, query: str | Query) -> PreparedQuery: ...
    async def execute(
        self,
        query: str | Query | PreparedQuery,
        params: Optional[Iterable[Any] | dict[str, Any]] = None,
    ) -> QueryResult:
        """
        Execute a query.

        This function takes a query string,
        and list of parameters.

        Parameters in query can be specified as ? signs.

        await scylla.execute("SELECT * FROM table WHERE id = ?", [11])

        Or you can use named parameters and pass dict to execute. Like this:

        await scylla.execute("SELECT * FROM table WHERE id = :id", {"id": 11})

        :param query: query to use.
        :param params: list of query parameters.
        :param as_class: DTO class to use for parsing rows (Can be pydantic model or dataclass).
        """
    async def batch(
        self,
        batch: Batch,
        params: Optional[Iterable[Iterable[Any] | dict[str, Any]]] = None,
    ) -> QueryResult:
        """
        Execute a batch statement.

        Batch statements are useful for grouping multiple queries
        together and executing them in one query.

        Each element of a list associated

        It may speed up you application.
        """
    async def use_keyspace(self, keyspace: str) -> None:
        """Change current keyspace for all connections."""
    async def get_keyspace(self) -> str | None:
        """Get current keyspace."""

class QueryResult:
    trace_id: str | None

    @overload
    def all(self, as_class: Literal[None] = None) -> list[dict[str, Any]]: ...
    @overload
    def all(self, as_class: Callable[..., T] | None = None) -> list[T]: ...
    def all(self, as_class: Any = None) -> Any: ...
    @overload
    def first(self, as_class: Literal[None] = None) -> dict[str, Any] | None: ...
    @overload
    def first(self, as_class: Callable[..., T] | None = None) -> T | None: ...
    def first(self, as_class: Any = None) -> Any: ...
    def scalars(self) -> list[Any]: ...
    def scalar(self) -> Any | None: ...

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
    is_idempotent: bool | None
    tracing: bool | None

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

class BatchType:
    """Possible BatchTypes."""

    COUNTER: BatchType
    LOGGED: BatchType
    UNLOGGED: BatchType

class Batch:
    """Class for batching queries together."""

    consistency: Consistency | None
    serial_consistency: SerialConsistency | None
    request_timeout: int | None
    is_idempotent: bool | None
    tracing: bool | None

    def __init__(
        self,
        batch_type: BatchType = BatchType.UNLOGGED,
        consistency: Consistency | None = None,
        serial_consistency: SerialConsistency | None = None,
        request_timeout: int | None = None,
        timestamp: int | None = None,
        is_idempotent: bool | None = None,
        tracing: bool | None = None,
    ) -> None: ...
    def add_query(self, query: Query | PreparedQuery | str) -> None: ...

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

    SERIAL: SerialConsistency
    LOCAL_SERIAL: SerialConsistency

class PreparedQuery:
    """Class that represents prepared statement."""
