from datenstrom.common.schema.atomic import AtomicEvent, SelfDescribingEvent, SelfDescribingContext

def test_atomic_json():
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
                "test": "test",
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

    json_string = ae.model_dump_json()
    o = AtomicEvent.model_validate_json(json_string)
    assert json_string == o.model_dump_json()


def test_atomic_avro():
    ae = AtomicEvent(
        event_id="123",
        collector_host="localhost",
        platform="test",
        event_vendor="io.datenstrom",
        event_name="page_view",
        event_version="1-0-0",
        tstamp="2021-01-01T00:00:00.000Z",
        collector_tstamp="2021-01-01T00:00:00.000Z",
        etl_tstamp="2021-01-01T00:00:00.000Z",
        v_collector="test",
        v_etl="test",
        event=SelfDescribingEvent(
            schema="iglu:io.datenstrom/page_view/jsonschema/1-0-0",
            data={
                "test": "test",
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

    json_string = ae.model_dump_json()
    avro_bytes = ae.to_avro()
    avro_size = len(avro_bytes)
    json_size = len(json_string)
    assert avro_size < json_size
    o2 = AtomicEvent.from_avro(avro_bytes)
    assert json_string == o2.model_dump_json()
