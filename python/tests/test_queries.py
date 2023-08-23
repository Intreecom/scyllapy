from dataclasses import dataclass
import pytest
from scyllapy import Scylla
from tests.utils import random_string


@pytest.mark.anyio
async def test_empty_scalars(scylla: Scylla):
    table_name = random_string(4)
    await scylla.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY)")
    res = await scylla.execute(f"SELECT id FROM {table_name}")

    assert res.all() == []
    assert res.scalars() == []


@pytest.mark.anyio
async def test_as_class(scylla: Scylla):
    @dataclass
    class TestDTO:
        id: int

    table_name = random_string(4)
    await scylla.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY)")
    await scylla.execute(f"INSERT INTO {table_name}(id) VALUES (?)", [42])
    res = await scylla.execute(f"SELECT id FROM {table_name}")

    assert res.all(as_class=TestDTO) == [TestDTO(id=42)]
