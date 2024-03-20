import json

from datenstrom.common.schema.atomic import AtomicEvent, SelfDescribingContext, SelfDescribingEvent
from datenstrom.common.registry.iglu import IgluSchema, BaseIgluRegistry
from datenstrom.common.registry.manager import RegistryManager


iglu_base_schema = "iglu:com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0"
iglu_base_schema_json = '{"description":"Meta-schema for self-describing JSON schema","allOf":[{"properties":{"self":{"type":"object","properties":{"vendor":{"type":"string","pattern":"^[a-zA-Z0-9-_.]+$"},"name":{"type":"string","pattern":"^[a-zA-Z0-9-_]+$"},"format":{"type":"string","pattern":"^[a-zA-Z0-9-_]+$"},"version":{"type":"string","pattern":"^[0-9]+-[0-9]+-[0-9]+$"}},"required":["vendor","name","format","version"],"additionalProperties":false}},"required":["self"]},{"$ref":"http://json-schema.org/draft-04/schema#"}],"self":{"vendor":"com.snowplowanalytics.self-desc","name":"schema","format":"jsonschema","version":"1-0-0"},"$schema":"http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#"}'
iglu_base_schema_data = json.loads(iglu_base_schema_json)


class DummyConfig:
    iglu_schema_registries = [
        "http://iglucentral.com/schemas/",
    ]
    default_cache_ttl = 3600
    none_cache_ttl = 60


def test_iglu_parsing():
    s = IgluSchema.from_string(iglu_base_schema)
    assert s.vendor == "com.snowplowanalytics.self-desc"
    assert s.name == "schema"
    assert s.format == "jsonschema"
    assert s.version == "1-0-0"


def test_jsonschema():
    r = BaseIgluRegistry()
    res = r._get_validator_and_check_schema(iglu_base_schema_data)
    assert res


def test_atomic_schema():
    r = RegistryManager(DummyConfig())

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
    r.validate("iglu:io.datenstrom/atomic/jsonschema/1-0-0", ae.model_dump(mode="json", by_alias=True))
    assert r.is_valid("iglu:io.datenstrom/atomic/jsonschema/1-0-0", ae.model_dump(mode="json", by_alias=True))


def test_structured_event():
    r = RegistryManager(DummyConfig())
    schema = "iglu:io.datenstrom/structured_event/jsonschema/1-0-0"
    data1 = {
        "invalid": "test"
    }
    data2 = {
        "category": "abc",
        "action": "act",
        "value": "1"
    }
    assert not r.is_valid(schema, data1)
    assert r.is_valid(schema, data2)


def test_remote():
    r = RegistryManager(DummyConfig())
    schema = "iglu:com.snowplowanalytics.mobile/deep_link/jsonschema/1-0-0"
    data1 = {
        "invalid": "test"
    }
    data2 = {
        "url": "https://www.example.com"
    }
    data3 = {
        "url": "https://www.example.com",
        "referrer": "https://www.example.com"
    }
    data4 = {
        "url": "https://www.example.com",
        "refesdsdrrer": "https://www.example.com"
    }
    assert not r.is_valid(schema, data1)
    assert r.is_valid(schema, data2)
    assert r.is_valid(schema, data3)
    assert not r.is_valid(schema, data4)

    assert r.get_schema_fields(schema) == ["url", "referrer"]
    parts = r.get_schema_parts(schema)
    assert parts.vendor == "com.snowplowanalytics.mobile"
    assert parts.name == "deep_link"
    assert parts.format == "jsonschema"
    assert parts.version == "1-0-0"