from typing import Any

import pytest
from tests.utils import random_string

from scyllapy import Scylla, extra_types
from scyllapy.exceptions import ScyllaPyDBError


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("type_cls", "type_name", "test_val"),
    [
        (extra_types.TinyInt, "TINYINT", 1),
        (extra_types.SmallInt, "SMALLINT", 1),
        (extra_types.BigInt, "BIGINT", 1),
        (extra_types.Double, "DOUBLE", 1.0),
    ],
)
async def test_int_types(
    scylla: Scylla,
    type_cls: Any,
    type_name: str,
    test_val: Any,
) -> None:
    table_name = random_string(4)

    await scylla.execute(
        f"CREATE TABLE {table_name} (id {type_name}, PRIMARY KEY (id))",
    )
    insert_query = f"INSERT INTO {table_name}(id) VALUES (?)"
    with pytest.raises(ScyllaPyDBError):
        await scylla.execute(insert_query, [test_val])

    await scylla.execute(insert_query, [type_cls(test_val)])

    result = await scylla.execute(f"SELECT * FROM {table_name}")
    rows = result.all()
    assert len(rows) == 1
    assert rows[0] == {"id": test_val}


@pytest.mark.anyio
async def test_counter(scylla: Scylla) -> None:
    table_name = random_string(4)
    await scylla.execute(
        f"CREATE TABLE {table_name} (id INT, count COUNTER, PRIMARY KEY (id))",
    )

    query = f"UPDATE {table_name} SET count = count + ? WHERE id = ?"

    with pytest.raises(ScyllaPyDBError):
        await scylla.execute(query, [1, 1])

    await scylla.execute(query, [extra_types.Counter(1), 1])

    res = await scylla.execute(f"SELECT * FROM {table_name}")
    rows = res.all()
    assert len(rows) == 1
    assert rows[0] == {"id": 1, "count": 1}


@pytest.mark.anyio
async def test_unset(scylla: Scylla) -> None:
    table_name = random_string(4)
    await scylla.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name TEXT)")

    await scylla.execute(
        f"INSERT INTO {table_name}(id, name) VALUES (?, ?)",
        [1, extra_types.Unset()],
    )
