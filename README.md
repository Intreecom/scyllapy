# Scylla driver for python

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

## For Alpine users

The binaries for `linux-musl` target are not yet ready. But you can easily build
the whl file by yourself.

```bash
apk add musl-dev libressl-dev rust cargo
export X86_64_ALPINE_LINUX_MUSL_OPENSSL_NO_VENDOR=1

pip install scyllapy

# Or you can build wheel to install it later.
pip wheel scyllapy
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