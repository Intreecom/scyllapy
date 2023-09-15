import pytest
from tests.utils import random_string

from scyllapy import Batch, Scylla


@pytest.mark.anyio
async def test_batches(scylla: Scylla) -> None:
    table_name = random_string(4)
    await scylla.execute(f"CREATE TABLE {table_name}(id INT, PRIMARY KEY (id))")

    batch = Batch()
    num_queries = 10
    for _ in range(num_queries):
        batch.add_query(f"INSERT INTO {table_name}(id) VALUES (?)")

    await scylla.batch(batch, [[i] for i in range(num_queries)])

    res = await scylla.execute(f"SELECT id FROM {table_name}")
    assert set(res.scalars()) == set(range(num_queries))
