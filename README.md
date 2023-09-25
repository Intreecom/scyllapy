[![PyPI](https://img.shields.io/pypi/v/scyllapy?style=for-the-badge)](https://pypi.org/project/scyllapy/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/scyllapy?style=for-the-badge)](https://pypistats.org/packages/scyllapy)


# Async Scylla driver for python

Python driver for ScyllaDB written in Rust. Though description says it's for scylla,
however it can be used with Cassandra and AWS keyspaces as well.

This driver uses official [ScyllaDB driver](https://github.com/scylladb/scylla-rust-driver) for [Rust](https://github.com/rust-lang/rust/) and exposes python API to interact with it.

## Installation

To install it, use your favorite package manager for python packages:

```bash
pip install scyllapy
```

Also, you can build from sources. To do it, install stable rust, [maturin](https://github.com/PyO3/maturin) and openssl libs.

```bash
maturin build --release --out dist
# Then install whl file from dist folder.
pip install dist/*
```

## Usage

The usage is pretty straitforward. Create a Scylla instance, run startup and start executing queries.

```python
import asyncio

from scyllapy import Scylla


async def main():
    scylla = Scylla(["localhost:9042"], keyspace="keyspace")
    await scylla.startup()
    await scylla.execute("SELECT * FROM table")
    await scylla.shutdown()

if __name__ == "__main__":
    asyncio.run(main())

```

## Parametrizing queries

While executing queries sometimes you may want to fine-tune some parameters, or dynamically pass values to the query.

Passing parameters is simple. You need to add a paramters list to the query.

```python
    await scylla.execute(
        "INSERT INTO otps(id, otp) VALUES (?, ?)",
        [uuid.uuid4(), uuid.uuid4().hex],
    )
```

Queries can be modified further by using `Query` class. It allows you to define
consistency for query or enable tracing.

```python
from scyllapy import Scylla, Query, Consistency, SerialConsistency

async def make_query(scylla: Scylla) -> None:
    query = Query(
        "SELECT * FROM table",
        consistency=Consistency.ALL,
        serial_consistency=SerialConsistency.LOCAL_SERIAL,
        request_timeout=1,
        timestamp=int(time.time()),
        is_idempotent=False,
        tracing=True,
    )
    result = await scylla.execute(query)
    print(result.all())
```

Also, with queries you can tweak random parameters for a specific execution.

```python
query = Query("SELECT * FROM table")

new_query = query.with_consistency(Consistency.ALL)
```

All `with_` methods create new query, copying all other parameters.

## Named parameters

Also, you can provide named parameters to querties, by using name
placeholders instead of `?`.

For example:

```python
async def insert(scylla: Scylla):
    await scylla.execute(
        "INSERT INTO table(id, name) VALUES (:id, :name)",
        params={"id": uuid.uuid4(), "name": uuid.uuid4().hex}
    )
```

Important note: All variables should be in snake_case.
Otherwise the error may be raised or parameter may not be placed in query correctly.
This happens, because scylla makes all parameters in query lowercase.

The scyllapy makes all parameters lowercase, but you may run into problems,
if you use multiple parameters that differ only in cases of some letters.


## Preparing queries

Also, queries can be prepared. You can either prepare raw strings, or `Query` objects.

```python
from scyllapy import Scylla, Query, PreparedQuery


async def prepare(scylla: Scylla, query: str | Query) -> PreparedQuery:
    return await scylla.prepare(query)
```

You can execute prepared queries by passing them to `execute` method.

```python
async def run_prepared(scylla: Scylla) -> None:
    prepared = await scylla.prepare("INSERT INTO memse(title) VALUES (?)")
    await scylla.execute(prepared, ("American joke",))
```

### Batching

We support batches. Batching can help a lot when you have lots of queries that you want to execute at the same time.

```python
from scyllapy import Scylla, Batch


async def run_batch(scylla: Scylla, num_queries: int) -> None:
    batch = Batch()
    for _ in range(num_queries):
        batch.add_query("SELECT * FROM table WHERE id = ?")
    await scylla.batch(batch, [(i,) for i in range(num_queries)])
```

Here we pass query as strings. But you can also add Prepared statements or Query objects.

Also, note that we pass list of lists as parametes for execute. Each element of
the list is going to be used in the query with the same index. But named parameters
are not supported for batches.

```python
async def run_batch(scylla: Scylla, num_queries: int) -> None:
    batch = Batch()
    batch.add_query("SELECT * FROM table WHERE id = :id")
    await scylla.batch(batch, [{"id": 1}])  # Will rase an error!
```

## Pagination

Sometimes you want to query lots of data. For such cases it's better not to
fetch all results at once, but fetch them using pagination. It reduces load
not only on your application, but also on a cluster.

To execute query with pagination, simply add `paged=True` in execute method.
After doing so, `execute` method will return `IterableQueryResult`, instead of `QueryResult`.
Instances of `IterableQueryResult` can be iterated with `async for` statements.
You, as a client, won't see any information about pages, it's all handeled internally within a driver.

Please note, that paginated queries are slower to fetch all rows, but much more memory efficent for large datasets.

```python
    result = await scylla.execute("SELECT * FROM table", paged=True)
    async for row in result:
        print(row)

```

Of course, you can change how results returned to you, by either using `scalars` or
`as_cls`. For example:

```python
async def func(scylla: Scylla) -> None:
    rows = await scylla.execute("SELECT id FROM table", paged=True)
    # Will print ids of each returned row.
    async for test_id in rows.scalars():
        print(test_id)

```

```python
from dataclasses import dataclass

@dataclass
class MyDTO:
    id: int
    val: int

async def func(scylla: Scylla) -> None:
    rows = await scylla.execute("SELECT * FROM table", paged=True)
    # Will print ids of each returned row.
    async for my_dto in rows.as_cls(MyDTO):
        print(my_dto.id, my_dto.val)

```

## Execution profiles

You can define profiles using `ExecutionProfile` class. After that the
profile can be used while creating a cluster or when defining queries.

```python
from scyllapy import Consistency, ExecutionProfile, Query, Scylla, SerialConsistency
from scyllapy.load_balancing import LoadBalancingPolicy, LatencyAwareness

default_profile = ExecutionProfile(
    consistency=Consistency.LOCAL_QUORUM,
    serial_consistency=SerialConsistency.LOCAL_SERIAL,
    request_timeout=2,
)

async def main():
    query_profile = ExecutionProfile(
        consistency=Consistency.ALL,
        serial_consistency=SerialConsistency.SERIAL,
        # Load balancing cannot be constructed without running event loop.
        # If you won't do it inside async funcion, it will result in error.
        load_balancing_policy=await LoadBalancingPolicy.build(
            token_aware=True,
            prefer_rack="rack1",
            prefer_datacenter="dc1",
            permit_dc_failover=True,
            shuffling_replicas=True,
            latency_awareness=LatencyAwareness(
                minimum_measurements=10,
                retry_period=1000,
                exclusion_threshold=1.4,
                update_rate=1000,
                scale=2,
            ),
        ),
    )

    scylla = Scylla(
        ["192.168.32.4"],
        default_execution_profile=default_profile,
    )
    await scylla.startup()
    await scylla.execute(
        Query(
            "SELECT * FROM system_schema.keyspaces;",
            profile=query_profile,
        )
    )
```

### Results

Every query returns a class that represents returned rows. It allows you to not fetch
and parse actual data if you don't need it. **Please be aware** that if your query was
not expecting any rows in return. Like for `Update` or `Insert` queries. The `RuntimeError` is raised when you call `all` or `first`.

```python
result = await scylla.execute("SELECT * FROM table")
print(result.all())
```

If you were executing query with tracing, you can get tracing id from results.

```python
result = await scylla.execute(Query("SELECT * FROM table", tracing=True))
print(result.trace_id)
```

Also it's possible to parse your data using custom classes. You
can use dataclasses or Pydantic.

```python
from dataclasses import dataclass

@dataclass
class MyDTO:
    id: uuid.UUID
    name: str

result = await scylla.execute("SELECT * FROM inbox")
print(result.all(as_class=MyDTO))
```

Or with pydantic.

```python
from pydantic import BaseModel

class MyDTO(BaseModel):
    user_id: uuid.UUID
    chat_id: uuid.UUID

result = await scylla.execute("SELECT * FROM inbox")
print(result.all(as_class=MyDTO))
```

## Extra types

Since Rust enforces typing, it's hard to identify which value
user tries to pass as a parameter. For example, `1` that comes from python
can be either `tinyint`, `smallint` or even `bigint`. But we cannot say for sure
how many bytes should we send to server. That's why we created some extra_types to
eliminate any possible ambigousnity.

You can find these types in `extra_types` module from scyllapy.

```python
from scyllapy import Scylla, extra_types

async def execute(scylla: Scylla) -> None:
    await scylla.execute(
        "INSERT INTO table(id, name) VALUES (?, ?)",
        [extra_types.BigInt(1), "memelord"],
    )
```


# Query building

ScyllaPy gives you ability to build queries,
instead of working with raw cql. The main advantage that it's harder to make syntax error,
while creating queries.

Base classes for Query building can be found in `scyllapy.query_builder`.

Usage example:

```python
from scyllapy import Scylla
from scyllapy.query_builder import Insert, Select, Update, Delete


async def main(scylla: Scylla):
    await scylla.execute("CREATE TABLE users(id INT PRIMARY KEY, name TEXT)")

    user_id = 1

    # We create a user with id and name.
    await Insert("users").set("id", user_id).set(
        "name", "user"
    ).if_not_exists().execute(scylla)

    # We update it's name to be user2
    await Update("users").set("name", "user2").where("id = ?", [user_id]).execute(
        scylla
    )

    # We select all users with id = user_id;
    res = await Select("users").where("id = ?", [user_id]).execute(scylla)
    # Verify that it's correct.
    assert res.first() == {"id": 1, "name": "user2"}

    # We delete our user.
    await Delete("users").where("id = ?", [user_id]).if_exists().execute(scylla)

    res = await Select("users").where("id = ?", [user_id]).execute(scylla)

    # Verify that user is deleted.
    assert not res.all()

    await scylla.execute("DROP TABLE users")

```

Also, you can pass built queries into InlineBatches. You cannot use queries built with query_builder module with default batches. This constraint is exists, because we
need to use values from within your queries and should ignore all parameters passed in
`batch` method of scylla.

Here's batch usage example.

```python
from scyllapy import Scylla, InlineBatch
from scyllapy.query_builder import Insert


async def execute_batch(scylla: Scylla) -> None:
    batch = InlineBatch()
    for i in range(10):
        Insert("users").set("id", i).set("name", "test").add_to_batch(batch)
    await scylla.batch(batch)

```

## Paging

Queries that were built with QueryBuilder also support paged returns.
But it supported only for select, because update, delete and insert should
not return anything and it makes no sense implementing it.
To make built `Select` query return paginated iterator, add paged parameter in execute method.

```python
    rows = await Select("test").execute(scylla, paged=True)
    async for row in rows:
        print(row['id'])
```