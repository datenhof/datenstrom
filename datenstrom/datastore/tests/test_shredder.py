from pytest import raises
from datenstrom.datastore.shredder import flatten_atomic_event, get_json_path, get_field_dict, FieldTransformation
from datenstrom.common.schema.atomic import AtomicEvent, SelfDescribingEvent, SelfDescribingContext


def test_atomic_shredder():
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
                "test": "test_e",
            }
        ),
        contexts=[
            SelfDescribingContext(
                schema="iglu:io.datenstrom/context/jsonschema/1-0-0",
                data={
                    "test": "test_c",
                }
            )
        ]
    )
    data = flatten_atomic_event(ae, all_schemas=True)
    assert data["event"]["test"] == "test_e"
    assert data["context_io_datenstrom_context_1"]["test"] == "test_c"

    event_transformations = [
        FieldTransformation("test", "event_test")
    ]
    context_transformations = [
        FieldTransformation("test", "context_test")
    ]

    data2 = flatten_atomic_event(ae, all_schemas=True, event_transformations=event_transformations,
                                 context_transformations=context_transformations)
    assert data2["event"]["test"] == "test_e"
    assert data2["context_io_datenstrom_context_1"]["test"] == "test_c"
    assert data2["event_test"] == "test_e"
    assert data2["context_test"] == "test_c"


def test_json_path():
    testdict = {
        "test": {
            "test2": "test3"
        }
    }
    assert get_json_path(testdict, "test.test2") == "test3"
    assert get_json_path(testdict, "test", None) == {"test2": "test3"}
    with raises(KeyError):
        get_json_path(testdict, "test.test3")
    assert get_json_path(testdict, "test.test3", None) == None
    assert get_json_path(testdict, "test2.test", "default") == "default"


def test_field_dict():
    testdict = {
        "test": {
            "test2": "test3"
        }
    }
    transformations = [
        FieldTransformation(
            path="test.test2",
            field="out"
        ),
        FieldTransformation(
            path="test",
            field="out2"
        ),
        FieldTransformation(
            path="test3",
            field="out3"
        ),
    ]
    assert get_field_dict(testdict, transformations) == {"out": "test3", "out2": {"test2": "test3"}}
