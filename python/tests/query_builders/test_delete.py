import pytest
from tests.utils import random_string

from scyllapy import Scylla
from scyllapy.query_builder import Delete


@pytest.mark.anyio
async def test_success(scylla: Scylla) -> None:
    table_name = random_string(4)
    await scylla.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name TEXT)")
    await scylla.execute(
        f"INSERT INTO {table_name}(id, name) VALUES (?, ?)",
        [1, "meme"],
    )
    await Delete(table_name).where("id = ?", [1]).execute(scylla)
    res = await scylla.execute(f"SELECT * FROM {table_name}")
    assert not res.all()


@pytest.mark.anyio
async def test_if_exists(scylla: Scylla) -> None:
    table_name = random_string(4)
    await scylla.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name TEXT)")
    await scylla.execute(
        f"INSERT INTO {table_name}(id, name) VALUES (?, ?)",
        [1, "meme"],
    )
    await Delete(table_name).where("id = ?", [1]).if_exists().execute(scylla)
    res = await scylla.execute(f"SELECT * FROM {table_name}")
    assert not res.all()


@pytest.mark.anyio
async def test_custom_if(scylla: Scylla) -> None:
    table_name = random_string(4)
    await scylla.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name TEXT)")
    await scylla.execute(
        f"INSERT INTO {table_name}(id, name) VALUES (?, ?)",
        [1, "meme"],
    )
    await Delete(table_name).where("id = ?", [1]).if_("name != ?", [None]).execute(
        scylla,
    )
    res = await scylla.execute(f"SELECT * FROM {table_name}")
    assert not res.all()


@pytest.mark.anyio
async def test_custom_custom_if(scylla: Scylla) -> None:
    table_name = random_string(4)
    await scylla.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name TEXT)")
    await scylla.execute(
        f"INSERT INTO {table_name}(id, name) VALUES (?, ?)",
        [1, "meme"],
    )
    await Delete(table_name).where("id = ?", [1]).if_("name != ?", [None]).execute(
        scylla,
    )
    res = await scylla.execute(f"SELECT * FROM {table_name}")
    assert not res.all()
