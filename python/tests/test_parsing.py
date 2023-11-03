import pytest
from tests.utils import random_string

from scyllapy import Scylla


@pytest.mark.anyio
async def test_udt_parsing(scylla: Scylla) -> None:
    table_name = random_string(4)
    await scylla.execute(f"CREATE TYPE test_udt{table_name} (id int, name text)")
    await scylla.execute(
        f"CREATE TABLE {table_name} "
        f"(id int PRIMARY KEY, udt_col frozen<test_udt{table_name}>)",
    )
    await scylla.execute(
        f"INSERT INTO {table_name} (id, udt_col) VALUES (1, {{id: 1, name: 'test'}})",
    )
    res = await scylla.execute(f"SELECT * FROM {table_name}")
    assert res.all() == [{"id": 1, "udt_col": {"id": 1, "name": "test"}}]
