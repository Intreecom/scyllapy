import pytest
from tests.utils import random_string

from scyllapy import Consistency, ExecutionProfile, Query, Scylla
from scyllapy.exceptions import ScyllaPyDBError


@pytest.mark.anyio
async def test_wrong_consistency(scylla: Scylla) -> None:
    profile = ExecutionProfile(consistency=Consistency.ANY)
    table_name = random_string(4)
    await scylla.execute(f"CREATE TABLE {table_name}(id INT PRIMARY KEY)")
    query = Query(f"SELECT * FROM {table_name} WHERE id = ?", profile=profile)
    with pytest.raises(ScyllaPyDBError, match=".*only supported for writes.*"):
        await scylla.execute(query, [1])

    await scylla.execute(query.with_profile(None), [1])
