import uuid

import pytest
from tests.utils import random_string

from scyllapy import Scylla
from scyllapy.query_builder import Select


@pytest.mark.anyio
async def test_select_success(scylla: Scylla) -> None:
    table_name = random_string(4)
    await scylla.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name TEXT)")
    await scylla.execute(
        f"INSERT INTO {table_name}(id, name) VALUES (?, ?)",
        [1, "meme"],
    )
    res = await Select(table_name).execute(scylla)
    assert res.all() == [{"id": 1, "name": "meme"}]


@pytest.mark.anyio
async def test_select_aliases(scylla: Scylla) -> None:
    table_name = random_string(4)
    await scylla.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name TEXT)")
    name = uuid.uuid4().hex
    await scylla.execute(f"INSERT INTO {table_name}(id, name) VALUES (?, ?)", [1, name])
    res = await Select(table_name).only("name as testname").execute(scylla)
    assert res.all() == [{"testname": name}]


@pytest.mark.anyio
async def test_select_simple_where(scylla: Scylla) -> None:
    table_name = random_string(4)
    await scylla.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name TEXT)")
    name = uuid.uuid4().hex
    await scylla.execute(
        f"INSERT INTO {table_name}(id, name) VALUES (?, ?)",
        [1, uuid.uuid4().hex],
    )
    await scylla.execute(f"INSERT INTO {table_name}(id, name) VALUES (?, ?)", [2, name])

    res = await Select(table_name).where("id = ?", [2]).execute(scylla)
    assert res.all() == [{"id": 2, "name": name}]


@pytest.mark.anyio
async def test_select_multiple_filters(scylla: Scylla) -> None:
    table_name = random_string(4)
    await scylla.execute(
        f"CREATE TABLE {table_name} (id INT, name TEXT, PRIMARY KEY (id, name))",
    )
    name = uuid.uuid4().hex
    await scylla.execute(
        f"INSERT INTO {table_name}(id, name) VALUES (?, ?)",
        [1, uuid.uuid4().hex],
    )
    await scylla.execute(f"INSERT INTO {table_name}(id, name) VALUES (?, ?)", [2, name])

    res = (
        await Select(table_name)
        .where("id = ?", [2])
        .where("name = ?", [name])
        .execute(scylla)
    )
    assert res.all() == [{"id": 2, "name": name}]


@pytest.mark.anyio
async def test_allow_filtering(scylla: Scylla) -> None:
    table_name = random_string(4)
    await scylla.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name TEXT)")
    name = uuid.uuid4().hex
    await scylla.execute(
        f"INSERT INTO {table_name}(id, name) VALUES (?, ?)",
        [1, uuid.uuid4().hex],
    )
    await scylla.execute(f"INSERT INTO {table_name}(id, name) VALUES (?, ?)", [2, name])

    res = (
        await Select(table_name)
        .where("id = ?", [2])
        .where("name = ?", [name])
        .allow_filtering()
        .execute(scylla)
    )
    assert res.all() == [{"id": 2, "name": name}]


@pytest.mark.anyio
async def test_limit(scylla: Scylla) -> None:
    table_name = random_string(4)
    await scylla.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name TEXT)")
    for i in range(10):
        await scylla.execute(
            f"INSERT INTO {table_name}(id, name) VALUES (?, ?)",
            [i, uuid.uuid4().hex],
        )
    res = await Select(table_name).limit(3).execute(scylla)
    assert len(res.all()) == 3


@pytest.mark.anyio
async def test_order_by(scylla: Scylla) -> None:
    table_name = random_string(4)
    await scylla.execute(
        f"CREATE TABLE {table_name} (id INT, iid INT, PRIMARY KEY(id, iid))",
    )
    for i in range(10):
        await scylla.execute(
            f"INSERT INTO {table_name}(id, iid) VALUES (?, ?)",
            [0, i],
        )
    res = (
        await Select(table_name)
        .only("iid")
        .where("id = ?", [0])
        .order_by("iid")
        .execute(scylla)
    )
    ids = [row["iid"] for row in res.all()]
    assert ids == list(range(10))
