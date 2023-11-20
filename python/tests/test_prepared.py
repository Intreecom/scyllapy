from typing import Any, Callable

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


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("type_name", "test_val", "cast_func"),
    [
        ("SET<TEXT>", ["one", "two"], set),
        ("SET<TEXT>", {"one", "two"}, set),
        ("SET<TEXT>", ("one", "two"), set),
        ("LIST<TEXT>", ("1", "2"), list),
        ("LIST<TEXT>", ["1", "2"], list),
        ("LIST<TEXT>", {"1", "2"}, list),
        ("MAP<TEXT, TEXT>", {"one": "two"}, dict),
        ("MAP<INT, BIGINT>", {1: 2}, dict),
        ("SET<BIGINT>", {1, 2}, set),
        ("TUPLE<INT, INT>", (1, 2), tuple),
        ("TUPLE<INT, TEXT, FLOAT>", (1, "two", 3.0), tuple),
        ("SET<TEXT>", [], lambda x: set(x) if x else None),
        ("LIST<TEXT>", [], lambda x: list(x) if x else None),
        ("MAP<TEXT, TEXT>", {}, lambda x: dict(x) if x else None),
        ("SET<INT>", [1], set),
        ("LIST<INT>", [1], list),
        ("MAP<TEXT, INT>", {"key": 1}, dict),
        ("SET<INT>", list(range(1000)), set),
        ("SET<INT>", [2147483647], set),
    ],
)
async def test_prepared_collections(
    scylla: Scylla,
    type_name: str,
    test_val: Any,
    cast_func: Callable[[Any], Any],
) -> None:
    table_name = random_string(4)
    await scylla.execute(
        f"CREATE TABLE {table_name} (id INT, coll {type_name}, PRIMARY KEY (id))",
    )

    insert_query = f"INSERT INTO {table_name}(id, coll) VALUES (?, ?)"
    prepared = await scylla.prepare(insert_query)
    await scylla.execute(prepared, [1, test_val])

    result = await scylla.execute(f"SELECT * FROM {table_name}")
    rows = result.all()
    assert len(rows) == 1
    assert rows[0] == {"id": 1, "coll": cast_func(test_val)}
