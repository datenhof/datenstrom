import json

from datenstrom.common.registry import SchemaRegistry
from datenstrom.common.schema.atomic import AtomicEvent, SelfDescribingContext, SelfDescribingEvent


iglu_base_schema = "iglu:com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0"
iglu_base_schema_json = '{"description":"Meta-schema for self-describing JSON schema","allOf":[{"properties":{"self":{"type":"object","properties":{"vendor":{"type":"string","pattern":"^[a-zA-Z0-9-_.]+$"},"name":{"type":"string","pattern":"^[a-zA-Z0-9-_]+$"},"format":{"type":"string","pattern":"^[a-zA-Z0-9-_]+$"},"version":{"type":"string","pattern":"^[0-9]+-[0-9]+-[0-9]+$"}},"required":["vendor","name","format","version"],"additionalProperties":false}},"required":["self"]},{"$ref":"http://json-schema.org/draft-04/schema#"}],"self":{"vendor":"com.snowplowanalytics.self-desc","name":"schema","format":"jsonschema","version":"1-0-0"},"$schema":"http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#"}'
iglu_base_schema_data = json.loads(iglu_base_schema_json)


class DummyConfig:
    iglu_schema_registries = [
        "http://iglucentral.com/schemas/",
    ]


def test_jsonschema():
    r = SchemaRegistry(DummyConfig())
    res = r._get_validator_and_check_schema(iglu_base_schema_data)
    assert res


def test_atomic_schema():
    r = SchemaRegistry(DummyConfig())

    ae = AtomicEvent(
        event_id="123",
        collector_host="localhost",
        platform="test",
        event_vendor="io.datenstrom",
        event_name="atomic",
        event_format="jsonschema",
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
        contexts={
            "iglu:io.datenstrom/context/jsonschema/1-0-0":
            SelfDescribingContext(
                schema="iglu:io.datenstrom/context/jsonschema/1-0-0",
                data={
                    "test": "test",
                }
            )
        }
    )
    res = r.validate("iglu:io.datenstrom/atomic/jsonschema/1-0-0", ae.model_dump(mode="json", by_alias=True))
