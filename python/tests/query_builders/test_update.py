import pytest
from tests.utils import random_string

from scyllapy import Scylla
from scyllapy.query_builder import Update


@pytest.mark.anyio
async def test_success(scylla: Scylla) -> None:
    table_name = random_string(4)
    await scylla.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name TEXT)")
    await scylla.execute(
        f"INSERT INTO {table_name}(id, name) VALUES (?, ?)",
        [1, "meme"],
    )
    await Update(table_name).set("name", "meme2").where("id = ?", [1]).execute(scylla)
    res = await scylla.execute(f"SELECT * FROM {table_name}")
    assert res.all() == [{"id": 1, "name": "meme2"}]


@pytest.mark.anyio
async def test_ifs(scylla: Scylla) -> None:
    table_name = random_string(4)
    await scylla.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name TEXT)")
    await scylla.execute(
        f"INSERT INTO {table_name}(id, name) VALUES (?, ?)",
        [1, "meme"],
    )
    await Update(table_name).set("name", "meme2").if_("name = ?", ["meme"]).where(
        "id = ?",
        [1],
    ).execute(scylla)
    res = await scylla.execute(f"SELECT * FROM {table_name}")
    assert res.all() == [{"id": 1, "name": "meme2"}]


@pytest.mark.anyio
async def test_if_exists(scylla: Scylla) -> None:
    table_name = random_string(4)
    await scylla.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name TEXT)")
    await Update(table_name).set("name", "meme2").if_exists().where(
        "id = ?",
        [1],
    ).execute(scylla)
    res = await scylla.execute(f"SELECT * FROM {table_name}")
    assert res.all() == []
