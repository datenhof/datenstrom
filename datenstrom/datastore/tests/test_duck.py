from datenstrom.common.schema.atomic import AtomicEvent, SelfDescribingContext, SelfDescribingEvent
from datenstrom.datastore.duck import get_duck_relation_from_atomic_events


def test_atomic_storage_schema():
    ae = AtomicEvent(
        event_id="123",
        collector_host="localhost",
        platform="test",
        event_vendor="io.datenstrom",
        event_name="atomic",
        event_version="1-0-0",
        tstamp="2021-01-01T00:00:00.000Z",
        collector_tstamp="2021-01-01T00:00:00.000Z",
        etl_tstamp="2021-01-01T00:00:00.000Z",
        v_collector="test",
        v_etl="test",
        event=SelfDescribingEvent(
            schema="iglu:io.datenstrom/page_view/jsonschema/1-0-0",
            data={
                "event_key": "somedata",
            }
        ),
        contexts=[
            SelfDescribingContext(
                schema="iglu:io.datenstrom/context/jsonschema/1-0-0",
                data={
                    "test": "test",
                }
            )
        ]
    )
    duck_rel = get_duck_relation_from_atomic_events([ae])
    assert duck_rel is not None
    result = duck_rel.fetchall()
    assert len(result) == 1
    assert result[0][0] == "123"
    assert result[0][1] == "localhost"
    result = duck_rel.query("atomic", "SELECT struct_extract(contexts[1], 'schema') FROM atomic")
    context_schema = result.fetchone()
    assert context_schema[0] == "iglu:io.datenstrom/context/jsonschema/1-0-0"
    result = duck_rel.query("atomic", "SELECT event.schema, json(event.data)->>'$.event_key' FROM atomic")
    event = result.fetchone()
    assert event[0] == "iglu:io.datenstrom/page_view/jsonschema/1-0-0"
    assert event[1] == "somedata"
