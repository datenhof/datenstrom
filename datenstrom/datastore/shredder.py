import re
from typing import Any, List, NamedTuple, Type, Optional, Dict

from datenstrom.common.registry.iglu import IgluSchema
from datenstrom.common.schema.atomic import AtomicEvent, SelfDescribingEvent, SelfDescribingContext


__empty = object()


class FieldTransformation(NamedTuple):
    path: str
    field: str


def fix_schema_name(schema: IgluSchema) -> str:
    snake_case_organization = schema.vendor.replace('.', '_').lower()
    snake_case_name = re.sub('([^A-Z_])([A-Z])', r'\g<1>_\g<2>', schema.name).lower()
    model = schema.version.split('-')[0]
    return f"{snake_case_organization}_{snake_case_name}_{model}"


def iglu_string_to_schema(iglu_string: str) -> IgluSchema:
    return IgluSchema.from_string(iglu_string)


def get_json_path(d: Dict[str, Any], path: str, default: Any = __empty) -> Any:
    keys = path.split('.')
    rv = d
    for key in keys:
        if isinstance(rv, dict):
            if default is __empty:
                rv = rv[key]
            else:
                rv = rv.get(key, default)
        elif isinstance(rv, list):
            try:
                rv = rv[int(key)]
            except (IndexError):
                if default is __empty:
                    raise
                else:
                    rv = default
        else:
            if default is __empty:
                raise KeyError(f"Key {key} not found in path {path}")
            else:
                return default
    return rv


def build_whitelist(schema_names: List[str]) -> List[Type[IgluSchema]]:
    white_list = []
    for schema_name in schema_names:
        if schema_name.startswith("iglu:"):
            path = schema_name[5:]
        else:
            path = schema_name
        path_parts = path.split("/")
        version = "*"
        if len(path_parts) < 2:
            raise ValueError(f"Invalid schema name: {schema_name}. Provide at least com.vendor/name")
        elif len(path_parts) < 4:
            vendor = path_parts[0]
            name = path_parts[1]
        elif len(path_parts) == 4:
            vendor = path_parts[0]
            name = path_parts[1]
            # parse version
            version = path_parts[3]
        else:
            raise ValueError(f"Invalid schema name: {schema_name}")
        white_list.append(IgluSchema(type="iglu", vendor=vendor, name=name,
                                     format="jsonschema", version=version))
    return white_list


def flatten_atomic_event(event: AtomicEvent, schema_names: Optional[List[str]] = None,
                         all_schemas: bool = False, event_transformations: Optional[List[FieldTransformation]] = None,
                         context_transformations: Optional[List[FieldTransformation]] = None) -> Dict[str, Any]:
    """
    Flatten an atomic event. The contexts and the self desribing event are
    flattened.
    """
    if not all_schemas and not schema_names:
        raise ValueError("schema_names must be provided if all_schemas is False")

    # parse schema names
    if schema_names:
        white_list = build_whitelist(schema_names)
    else:
        white_list = []

    common_fields = event.model_dump(mode="json")
    common_fields.pop("event")
    common_fields.pop("contexts")

    # flatten event
    event_fields = {}
    event_schema = IgluSchema.from_string(event.event.schema_name)
    if all_schemas:
        event_fields = event.event.model_dump(mode="json")["data"]
    else:
        for w in white_list:
            if event_schema.vendor == w.vendor and event_schema.name == w.name:
                if w.version == "*" or event_schema.version.startswith(w.version):
                    event_fields = event.event.model_dump(mode="json")["data"]
                    break
    common_fields["event"] = event_fields
    # apply transformations
    if event_transformations:
        get_field_dict(event_fields, event_transformations, output_dict=common_fields)

    # flatten contexts
    context_fields = {}
    for context in event.contexts:
        context_schema = IgluSchema.from_string(context.schema_name)
        if all_schemas:
            new_name = "context_" + fix_schema_name(context_schema)
            context_fields[new_name] = context.model_dump(mode="json")["data"]
            continue
        else:
            for w in white_list:
                if context_schema.vendor == w.vendor and context_schema.name == w.name:
                    if w.version == "*" or context_schema.version.startswith(w.version):
                        new_name = "context_" + fix_schema_name(context_schema)
                        context_fields[new_name] = context.model_dump(mode="json")["data"]
                        break
    # apply transformations
    if context_transformations:
        for _, context_data in context_fields.items():
            get_field_dict(context_data, context_transformations, output_dict=common_fields)

    # merge fields
    common_fields.update(context_fields)
    return common_fields


def get_field_dict(input_dict: Dict[str, Any], transformations: List[FieldTransformation],
                   output_dict: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Get the fields from the transformations in a new dict.
    """
    if output_dict is None:
        output_dict = {}
    for t in transformations:
        if t.field in output_dict:
            raise ValueError(f"Field {t.field} already exists in output_dict")
        value = get_json_path(input_dict, t.path, None)
        if value is not None:
            output_dict[t.field] = value
    return output_dict