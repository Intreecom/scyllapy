import datetime
import ipaddress
from typing import Any, Callable
import uuid
import pytest
from scyllapy import Scylla
from tests.utils import random_string


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("type_name", "test_val"),
    [
        ("INT", 1),
        ("TEXT", "mytext"),
        ("VARCHAR", "text2"),
        ("ASCII", "randomtext"),
        ("BLOB", b"random_bytes"),
        ("BOOLEAN", True),
        ("BOOLEAN", False),
        ("DATE", datetime.date.today()),
        ("TIME", datetime.time(22, 30, 11, 403)),
        ("TIMEUUID", uuid.uuid1()),
        ("UUID", uuid.uuid1()),
        ("UUID", uuid.uuid3(uuid.uuid4(), "name")),
        ("UUID", uuid.uuid4()),
        ("UUID", uuid.uuid5(uuid.uuid4(), "name")),
        ("INET", ipaddress.ip_address("192.168.1.1")),
        ("INET", ipaddress.ip_address("2001:db8::8a2e:370:7334")),
    ],
)
async def test_bindings(
    scylla: Scylla,
    type_name: str,
    test_val: Any,
) -> None:
    table_name = random_string(4)
    await scylla.execute(
        f"CREATE TABLE {table_name} (id {type_name}, PRIMARY KEY (id))"
    )
    insert_query = f"INSERT INTO {table_name}(id) VALUES (?)"
    await scylla.execute(insert_query, [test_val])

    result = await scylla.execute(f"SELECT * FROM {table_name}")
    rows = result.all()
    assert len(rows) == 1
    assert rows[0] == {"id": test_val}


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
    ],
)
async def test_collections(
    scylla: Scylla,
    type_name: str,
    test_val: Any,
    cast_func: Callable[[Any], Any],
) -> None:
    table_name = random_string(4)
    await scylla.execute(
        f"CREATE TABLE {table_name} (id INT, coll {type_name}, PRIMARY KEY (id))"
    )
    insert_query = f"INSERT INTO {table_name}(id, coll) VALUES (?, ?)"

    await scylla.execute(insert_query, [1, test_val])

    result = await scylla.execute(f"SELECT * FROM {table_name}")
    rows = result.all()
    assert len(rows) == 1
    assert rows[0] == {"id": 1, "coll": cast_func(test_val)}
