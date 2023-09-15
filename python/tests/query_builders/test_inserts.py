import pytest
from tests.utils import random_string

from scyllapy import Scylla
from scyllapy.query_builder import Insert


@pytest.mark.anyio
async def test_insert_success(scylla: Scylla) -> None:
    table_name = random_string(4)
    await scylla.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name TEXT)")
    await Insert(table_name).set("id", 1).set("name", "random").execute(scylla)
    result = await scylla.execute(f"SELECT * FROM {table_name}")
    assert result.all() == [{"id": 1, "name": "random"}]


@pytest.mark.anyio
async def test_insert_if_not_exists(scylla: Scylla) -> None:
    table_name = random_string(4)
    await scylla.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name TEXT)")
    await Insert(table_name).set("id", 1).set("name", "random").execute(scylla)
    await Insert(table_name).set("id", 1).set(
        "name",
        "random2",
    ).if_not_exists().execute(scylla)
    res = await scylla.execute(f"SELECT * FROM {table_name}")
    assert res.all() == [{"id": 1, "name": "random"}]


@pytest.mark.anyio
async def test_insert_request_params(scylla: Scylla) -> None:
    table_name = random_string(4)
    await scylla.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name TEXT)")
    await Insert(table_name).set("id", 1).set("name", "random").execute(scylla)
    res = (
        await Insert(table_name)
        .set("id", 1)
        .set("name", "random2")
        .request_params(
            tracing=True,
        )
        .execute(scylla)
    )
    assert res.trace_id
