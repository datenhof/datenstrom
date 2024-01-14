import pyarrow

from typing import Dict, Any, List

from datenstrom.common.schema.atomic import AtomicEvent, ATOMIC_EVENT_SCHEMA
from datenstrom.common.schema.utils import SchemaField, get_json_schema_fields


def get_pa_type(t: str) -> pyarrow.DataType:
    if t == "string":
        return pyarrow.string()
    elif t == "integer":
        return pyarrow.int64()
    elif t == "number":
        return pyarrow.float64()
    elif t == "boolean":
        return pyarrow.bool_()
    elif t == "datetime":
        return pyarrow.timestamp("ns")
    elif t == "object":
        return pyarrow.struct
    elif t == "array":
        return pyarrow.list_
    elif t == "map":
        return pyarrow.map_
    elif t == "any":
        # for any we will have to use string and json encode/decode
        return pyarrow.string()
    else:
        raise ValueError(f"Unknown type: {t}")


def field_to_pafield(field: SchemaField) -> pyarrow.Field:
    if field.type == "object":
        return pyarrow.field(
            name=field.name,
            type=pyarrow.struct([field_to_pafield(f) for f in field.children])
        )
    elif field.type == "map":
        if field.children_type == "object":
            return pyarrow.field(
                name=field.name,
                type=pyarrow.map_(
                    pyarrow.string(),
                    pyarrow.struct([field_to_pafield(f) for f in field.children])
                )
            )
        else:
            return pyarrow.field(
                name=field.name,
                type=pyarrow.map_(
                    pyarrow.string(),
                    get_pa_type(field.children_type)
                )
            )
    elif field.type == "array":
        if field.children_type == "object":
            return pyarrow.field(
                name=field.name,
                type=pyarrow.list_(
                    pyarrow.struct(
                        [field_to_pafield(f) for f in field.children]
                    )
                ),
            )
        else:
            return pyarrow.field(
                name=field.name,
                type=pyarrow.list_(
                    get_pa_type(field.children_type)
                ),
            )
    else:
        return pyarrow.field(name=field.name, type=get_pa_type(field.type))


def jsonschema_to_arrow_schema(schema: Dict[str, Any]) -> pyarrow.Schema:
    schema_fields = get_json_schema_fields(schema)
    pafields = [field_to_pafield(f) for f in schema_fields]
    return pyarrow.schema(pafields)


class AtomicArrowConverter():
    def __init__(self):
        self.atomic_arrow_schema = jsonschema_to_arrow_schema(ATOMIC_EVENT_SCHEMA)

    def to_table(self, events: List[AtomicEvent]) -> pyarrow.Table:
        event_dicts = [e.to_hive_serializable() for e in events]
        table = pyarrow.Table.from_pylist(event_dicts, schema=self.atomic_arrow_schema)
        return table