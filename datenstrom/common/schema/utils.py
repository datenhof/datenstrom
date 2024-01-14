from typing import List, Any, Dict, Optional

from dataclasses import dataclass, field


@dataclass
class SchemaField():
    name: str
    type: str
    children_type: str = None
    children: List["SchemaField"] = field(default_factory=list)


def get_data_type(t: str, f: Optional[str]) -> str:
    # get type (first != null in list if multiple)
    if isinstance(t, list):
        t = next(x for x in t if x != "null")
    # if t is string, check if it's a date
    if t == "string" and f == "date-time":
        return "datetime"
    return t

def get_json_schema_fields(schema: Dict[str, Any]) -> List[SchemaField]:
    fields = []
    for name, field in schema["properties"].items():
        # get type (first != null in list if multiple)
        # if there is no type we assume any
        type = get_data_type(field.get("type", "any"), field.get("format"))
        if type == "object":
            # check if this is a map (has additionalProperties)
            if "additionalProperties" in field and isinstance(field["additionalProperties"], dict):
                fields.append(
                    SchemaField(
                        name=name,
                        type="map",
                        children_type="object",
                        children=get_json_schema_fields(field["additionalProperties"]),
                    )
                )
            else:
                fields.append(
                    SchemaField(
                        name=name,
                        type="object",
                        children=get_json_schema_fields(field),
                    )
                )
        elif type == "array":
            # get inner type (first != null in list if multiple)
            inner_type = get_data_type(field["items"]["type"], field["items"].get("format"))
            if isinstance(inner_type, list):
                inner_type = next(t for t in inner_type if t != "null")
            if inner_type == "object":
                fields.append(
                    SchemaField(
                        name=name,
                        type="array",
                        children_type="object",
                        children=get_json_schema_fields(field["items"]),
                    )
                )
            else:
                fields.append(
                    SchemaField(
                        name=name,
                        type="array",
                        children_type=inner_type,
                    )
                )
        else:
            fields.append(
                SchemaField(
                    name=name,
                    type=type,
                )
            )
    return fields
