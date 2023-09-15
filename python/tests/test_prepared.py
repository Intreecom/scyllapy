import pytest
from tests.utils import random_string

from scyllapy import Scylla


@pytest.mark.anyio
async def test_prepared(scylla: Scylla) -> None:
    table_name = random_string(4)
    await scylla.execute(f"CREATE TABLE {table_name}(id INT, PRIMARY KEY (id))")
    await scylla.execute(f"INSERT INTO {table_name}(id) VALUES (?)", [1])

    query = f"SELECT * FROM {table_name}"
    prepared = await scylla.prepare(query)
    res = await scylla.execute(query)
    prepared_res = await scylla.execute(prepared)

    assert res.all() == prepared_res.all()
