import os
from typing import AsyncGenerator

import pytest
from tests.utils import random_string

from scyllapy import Scylla


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    """
    Anyio backend.

    Backend for anyio pytest plugin.
    :return: backend name.
    """
    return "asyncio"


@pytest.fixture(scope="session")
def scylla_url() -> str:
    return os.environ.get("SCYLLA_URL", "localhost:9042")


@pytest.fixture(scope="session")
async def keyspace(scylla_url: str) -> AsyncGenerator[str, None]:
    keyspace_name = random_string(5)
    scylla = Scylla(contact_points=[scylla_url])
    await scylla.startup()
    await scylla.execute(
        f"CREATE keyspace {keyspace_name} WITH replication = "
        "{'class': 'SimpleStrategy', 'replication_factor': 1}",
    )

    yield keyspace_name

    await scylla.execute(f"DROP KEYSPACE {keyspace_name}")


@pytest.fixture(scope="session")
async def scylla(scylla_url: str, keyspace: str) -> AsyncGenerator[Scylla, None]:
    scylla = Scylla(
        contact_points=[scylla_url],
        keyspace=keyspace,
    )
    await scylla.startup()

    yield scylla

    await scylla.shutdown()
