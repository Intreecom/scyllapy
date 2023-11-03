from dataclasses import dataclass

import pytest
from tests.utils import random_string

from scyllapy import Scylla


@pytest.mark.anyio
async def test_empty_scalars(scylla: Scylla) -> None:
    table_name = random_string(4)
    await scylla.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY)")
    res = await scylla.execute(f"SELECT id FROM {table_name}")

    assert res.all() == []
    assert res.scalars() == []


@pytest.mark.anyio
async def test_as_class(scylla: Scylla) -> None:
    @dataclass
    class TestDTO:
        id: int

    table_name = random_string(4)
    await scylla.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY)")
    await scylla.execute(f"INSERT INTO {table_name}(id) VALUES (?)", [42])
    res = await scylla.execute(f"SELECT id FROM {table_name}")

    assert res.all(as_class=TestDTO) == [TestDTO(id=42)]


@pytest.mark.anyio
async def test_udt_as_dataclass(scylla: Scylla) -> None:
    @dataclass
    class UDTType:
        id: int
        name: str

    @dataclass
    class TestDTO:
        id: int
        udt_col: UDTType

        def __post_init__(self) -> None:
            if not isinstance(self.udt_col, UDTType):
                self.udt_col = UDTType(**self.udt_col)

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
    assert res.all(as_class=TestDTO) == [
        TestDTO(id=1, udt_col=UDTType(id=1, name="test")),
    ]
