from datenstrom.common.schema.atomic import ATOMIC_EVENT_SCHEMA
from datenstrom.datastore.arrow import jsonschema_to_arrow_schema


def test_atomic_storage_schema():
    arrow_schema = jsonschema_to_arrow_schema(ATOMIC_EVENT_SCHEMA)
    assert arrow_schema