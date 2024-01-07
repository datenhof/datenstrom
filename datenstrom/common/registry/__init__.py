from typing import List, Optional, NamedTuple, Any, Type
from functools import lru_cache

from jsonschema import Draft202012Validator
from jsonschema.protocols import Validator
from jsonschema.exceptions import SchemaError, ValidationError
import orjson
import requests

from datenstrom.common.schema.events import STATIC_JSON_SCHEMAS


IGLU_BASE_URL = "http://iglucentral.com/schemas/"
IGNORE_META_SCHEMA = True
MAX_SCHEMA_SIZE =  32 * 1024  # 32kb
CACHE_SIZE = 1024


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
    def __init__(self, config: Optional[Any] = None):
        self.config = config

    def validate(self, schema: str, data: Any) -> None:
        t = self.get_schema_type(schema)
        if t == "iglu":
            validator = self.get_iglu_schema_validator(schema)
        else:
            raise NotImplementedError(f"Schema type not supported: {t}")
        try:
            validator.validate(data)
        except ValidationError as e:
            raise ValueError(f"Failed to validate data: {e}")

    def is_valid(self, schema: str, data: Any) -> bool:
        t = self.get_schema_type(schema)
        if t == "iglu":
            validator = self.get_iglu_schema_validator(schema)
        else:
            raise NotImplementedError(f"Schema type not supported: {t}")
        return validator.is_valid(data)

    def get_schema_fields(self, schema: str) -> List[str]:
        t = self.get_schema_type(schema)
        if t == "iglu":
            validator = self.get_iglu_schema_validator(schema)
        else:
            raise NotImplementedError(f"Schema type not supported: {t}")
        return list(validator.schema["properties"].keys())

    def get_schema_type(self, schema: str) -> str:
        if schema.startswith("iglu:"):
            return "iglu"
        raise ValueError("Invalid schema - only supporting iglu schemas")

    # IGLU SCHEMA

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

    def _get_validator_cls(self, meta_schema: str) -> Type[Validator]:
        # for now we will always use the latest validator
        if IGNORE_META_SCHEMA:
            return Draft202012Validator
        raise NotImplementedError("Selecting validator based on meta schema is not implemented")

    def _load_iglu_schema(self, iglu_schema: IgluSchema) -> Validator:
        # check if we can get it from static schemas
        path = iglu_schema.to_path()
        if path in STATIC_JSON_SCHEMAS:
            print(f"Hit static schema: {path}")
            return STATIC_JSON_SCHEMAS[path]
        
        # get it from the webs
        url = IGLU_BASE_URL + path
        print(f"Loading schema: {url}")
        # load the schema
        r = requests.get(url)
        # check if the request was successful
        if r.status_code != 200:
            raise SchemaNotFound(f"Failed to load schema: {url}")
        # get content length
        content_length = int(r.headers.get("content-length", 0))
        if content_length > MAX_SCHEMA_SIZE:
            raise SchemaError(f"Schema ({iglu_schema}) too large: {content_length} bytes")
        # parse response json
        return r.json()

    def _get_validator_and_check_schema(self, schema: Any) -> Type[Validator]:
        # get the meta schema
        meta_schema_name = schema["$schema"]
        # validate the schema itself
        validator_cls = self._get_validator_cls(meta_schema_name)
        validator_cls.check_schema(schema)
        return validator_cls(schema)

    @lru_cache(maxsize=CACHE_SIZE)
    def get_iglu_schema_validator(self, schema: str) -> Optional[Validator]:
        iglu_schema = self.parse_iglu_schema(schema)
        try:
            s = self._load_iglu_schema(iglu_schema)
            # validate the schema itself and return the validator
            return self._get_validator_and_check_schema(s)
        except SchemaError as e:
            print(f"Failed to load schema: {schema}")
            print(e)
            return None
        except SchemaNotFound:
            print(f"Schema not found: {schema}")
            return None
