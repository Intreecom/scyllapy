# Scylla driver for python

Simlpe to use driver for scyllaDB written in Rust.
It uses official driver for scyllaDB for Rust internally and
integrates it in Python.

```python
from scyllapy import Scylla

async def main():
    scylla = Scylla(["172.18.0.5:9042"], username="user", password="pass", keyspace="keyspace")
    await scylla.startup()
    result = await scylla.execute("SELECT * FROM table")
```

You can use parameters in queries.

```python
    await scylla.execute("SELECT * FORM table WHERE id IN ? AND name = ?", ([1, 2, 3], "name"))
```

You can set row type, by passing as_class.

```python
import asyncio
from dataclasses import dataclass
import uuid

from scyllapy import Scylla


@dataclass
class InboxDTO:
    user_id: uuid.UUID
    chat_id: uuid.UUID


async def main():
    scylla = Scylla(["172.18.0.5:9042"], keyspace="chat_api")
    await scylla.startup()
    results = await scylla.execute("SELECT * FROM inbox")
    print(results.all(as_class=InboxDTO))
    await scylla.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

```

It will print:

```log
[InboxDTO(user_id=UUID('cbec7f6f-a1d3-45be-b1c1-08187ac6b188'), chat_id=UUID('72b14d17-eab3-4c12-bd97-3e80b8ab35c3'))]
```