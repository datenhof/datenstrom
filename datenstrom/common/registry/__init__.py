from typing import List, Optional, NamedTuple, Any, Type
from functools import lru_cache

from jsonschema import Draft202012Validator
from jsonschema.protocols import Validator
from jsonschema.exceptions import SchemaError, ValidationError
import orjson
import requests

from datenstrom.common.schema.events import STATIC_JSON_SCHEMAS


IGLU_BASE_URL = "http://iglucentral.com/schemas/"


class SchemaNotFound(Exception):
    pass


class IgluSchema(NamedTuple):
    vendor: str
    name: str
    format: str
    version: str

    def to_path(self) -> str:
        return f"{self.vendor}/{self.name}/{self.format}/{self.version}"

    def to_iglu(self) -> str:
        return f"iglu:{self.to_path()}"


class SchemaRegistry:
    def __init__(self, registry_configs: Optional[List[str]] = None):
        self.registry_configs = registry_configs

    # iglu schema format: iglu:com.adjust/install/jsonschema/1-0-0
    def parse_iglu_schema(self, schema: str) -> IgluSchema:
        # check if schema starts with iglu
        if not schema.startswith("iglu:"):
            raise ValueError("Invalid iglu schema")
        # remove the iglu prefix
        schema = schema[5:]
        # split schema into parts
        schema_parts = schema.split("/")
        # check if schema has 4 parts
        if len(schema_parts) != 4:
            raise ValueError("Invalid iglu schema")
        # first part is the vendor
        vendor = schema_parts[0]
        # second part is the name
        name = schema_parts[1]
        # third part is the format - it should be jsonschema
        format = schema_parts[2]
        if format != "jsonschema":
            raise ValueError("Invalid iglu schema")
        # fourth part is the version
        version = schema_parts[3]
        # return the parsed schema
        return IgluSchema(vendor, name, format, version)

    @lru_cache(maxsize=128)
    def get_validator_cls(self, meta_schema: str) -> Type[Validator]:
        # for now we will always use the latest validator
        print("Loading validator vor meta schema: ", meta_schema)
        return Draft202012Validator

    def _load_iglu_schema(self, iglu_schema: IgluSchema) -> Validator:
        # check if we can get it from static schemas
        path = iglu_schema.to_path()
        if path in STATIC_JSON_SCHEMAS:
            print(f"Hit static schema: {path}")
            return Draft202012Validator(schema=STATIC_JSON_SCHEMAS[path])
        
        # get it from the webs
        url = IGLU_BASE_URL + path
        print(f"Loading schema: {url}")
        # load the schema
        r = requests.get(url)
        # check if the request was successful
        if r.status_code != 200:
            raise SchemaNotFound(f"Failed to load schema: {url}")
        # parse response json
        response = r.json()
        # get the meta schema
        meta_schema_name = response["$schema"]
        # validate the schema itself
        validator_cls = self.get_validator_cls(meta_schema_name)
        validator_cls.check_schema(response)
        # return the correctly loaded schema and validator
        return validator_cls(schema=response)

    @lru_cache(maxsize=512)
    def get_iglu_schema_validator(self, schema: str) -> Optional[Validator]:
        s = self.parse_iglu_schema(schema)
        try:
            return self._load_iglu_schema(s)
        except SchemaError as e:
            print(f"Failed to load schema: {s}")
            print(e)
            return None
        except SchemaNotFound:
            print(f"Schema not found: {s}")
            return None

    def validate(self, schema: str, data: Any) -> None:
        validator = self.get_iglu_schema_validator(schema)
        try:
            validator.validate(data)
        except ValidationError as e:
            raise ValueError(f"Failed to validate data: {e}")

    def is_valid(self, schema: str, data: Any) -> bool:
        validator = self.get_iglu_schema_validator(schema)
        return validator.is_valid(data)

    def get_fields(self, schema: str) -> List[str]:
        validator = self.get_iglu_schema_validator(schema)
        return list(validator.schema["properties"].keys())