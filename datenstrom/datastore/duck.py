from typing import List

import duckdb
from pyarrow import Table

from datenstrom.common.schema.atomic import AtomicEvent
from datenstrom.datastore.arrow import AtomicArrowConverter


converter = AtomicArrowConverter()


def get_duck_relation_from_atomic_events(
        events: List[AtomicEvent]) -> duckdb.DuckDBPyRelation:
    table = converter.to_table(events)
    return duckdb.from_arrow(table)
