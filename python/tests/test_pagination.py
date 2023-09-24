from dataclasses import dataclass

import pytest
from tests.utils import random_string

from scyllapy import Scylla
from scyllapy.query_builder import Select


@pytest.mark.anyio
async def test_scalars(scylla: Scylla) -> None:
    table_name = random_string(4)
    await scylla.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} (id INT PRIMARY KEY)",
    )
    vals = list(range(10))
    for i in vals:
        await scylla.execute(f"INSERT INTO {table_name}(id) VALUES (?)", [i])
    res = await scylla.execute(f"SELECT id FROM {table_name}", paged=True)
    async for col in res.scalars():
        assert col in vals


@pytest.mark.anyio
async def test_dicts(scylla: Scylla) -> None:
    table_name = random_string(4)
    await scylla.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} (id INT PRIMARY KEY, val INT)",
    )
    vals = list(range(10))
    for i in vals:
        await scylla.execute(
            f"INSERT INTO {table_name}(id, val) VALUES (?, ?)",
            [i, -i],
        )
    res = await scylla.execute(f"SELECT id, val FROM {table_name}", paged=True)
    async for row in res:
        assert row["id"] in vals
        assert row["val"] == -row["id"]


@pytest.mark.anyio
async def test_dtos(scylla: Scylla) -> None:
    @dataclass
    class TestDTO:
        id: int
        val: int

    table_name = random_string(4)
    await scylla.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} (id INT PRIMARY KEY, val INT)",
    )
    vals = list(range(10))
    for i in vals:
        await scylla.execute(
            f"INSERT INTO {table_name}(id, val) VALUES (?, ?)",
            [i, -i],
        )
    res = await scylla.execute(f"SELECT id, val FROM {table_name}", paged=True)
    async for row in res.as_cls(TestDTO):
        assert row.id in vals
        assert row.val == -row.id


@pytest.mark.anyio
async def test_paged_select_qb(scylla: Scylla) -> None:
    table_name = random_string(4)
    await scylla.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} (id INT PRIMARY KEY, val INT)",
    )
    vals = list(range(10))
    for i in vals:
        await scylla.execute(
            f"INSERT INTO {table_name}(id, val) VALUES (?, ?)",
            [i, -i],
        )
    res = await Select(table_name).execute(scylla, paged=True)
    async for row in res:
        assert row["id"] in vals
        assert row["val"] == -row["id"]
