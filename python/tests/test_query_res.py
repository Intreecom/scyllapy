import pytest
from tests.utils import random_string

from scyllapy import Scylla


@pytest.mark.anyio
async def test_results_len(scylla: Scylla) -> None:
    table_name = random_string(4)
    await scylla.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY)")
    for i in range(10):
        await scylla.execute(f"INSERT INTO {table_name}(id) VALUES (?)", [i])
    res = await scylla.execute(f"SELECT id FROM {table_name}")

    assert len(res) == 10
