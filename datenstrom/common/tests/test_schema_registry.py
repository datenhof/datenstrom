import json

from datenstrom.common.registry import SchemaRegistry


iglu_base_schema = "iglu:com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0"
iglu_base_schema_json = '{"description":"Meta-schema for self-describing JSON schema","allOf":[{"properties":{"self":{"type":"object","properties":{"vendor":{"type":"string","pattern":"^[a-zA-Z0-9-_.]+$"},"name":{"type":"string","pattern":"^[a-zA-Z0-9-_]+$"},"format":{"type":"string","pattern":"^[a-zA-Z0-9-_]+$"},"version":{"type":"string","pattern":"^[0-9]+-[0-9]+-[0-9]+$"}},"required":["vendor","name","format","version"],"additionalProperties":false}},"required":["self"]},{"$ref":"http://json-schema.org/draft-04/schema#"}],"self":{"vendor":"com.snowplowanalytics.self-desc","name":"schema","format":"jsonschema","version":"1-0-0"},"$schema":"http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#"}'
iglu_base_schema_data = json.loads(iglu_base_schema_json)

def test_jsonschema():
    r = SchemaRegistry()
    assert r.get_validator_cls(iglu_base_schema_data["$schema"])
    res = r.is_valid(iglu_base_schema, iglu_base_schema_data)
    assert res
