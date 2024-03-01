from typing import Any, List, NamedTuple, Type, Optional, Dict
from jsonschema import Draft202012Validator
from jsonschema.protocols import Validator
from jsonschema.exceptions import SchemaError, ValidationError
from functools import lru_cache
import requests

from datenstrom.common.cache import TTLCache, cachedmethod
from datenstrom.common.schema.atomic import ATOMIC_EVENT_SCHEMA
from datenstrom.common.schema.events import STATIC_JSON_SCHEMAS
from datenstrom.common.registry.base import SchemaValidationError, InvalidSchemaError


IGLU_BASE_URL = "http://iglucentral.com/schemas/"
IGNORE_META_SCHEMA = True
MAX_SCHEMA_SIZE =  128 * 1024  # 128kb


class IgluSchema(NamedTuple):
    type: str
    vendor: str
    name: str
    format: str
    version: str

    @classmethod
    def from_string(cls, schema: str) -> "IgluSchema":
        if not schema.startswith("iglu:"):
            raise ValueError(f"Invalid schema (not iglu): {schema}")
        path = schema[5:]
        path_parts = path.split("/")
        if len(path_parts) != 4:
            raise ValueError(f"Invalid schema path: {path}")
        if path_parts[2] != "jsonschema":
            raise ValueError(f"Invalid schema format: {path_parts[2]}")
        return cls(type="iglu",
                   vendor=path_parts[0],
                   name=path_parts[1],
                   format=path_parts[2],
                   version=path_parts[3])

    def to_string(self) -> str:
        return f"iglu:{self.vendor}/{self.name}/{self.format}/{self.version}"

    def to_path(self) -> str:
        return f"{self.vendor}/{self.name}/{self.format}/{self.version}"

    def hashkey(self) -> str:
        return self.to_string()


class IgluSchemaEntry(NamedTuple):
    schema: IgluSchema
    schema_object: Any
    validator: Any

    def get_fields(self) -> List[str]:
        return list(self.schema_object["properties"].keys())

    def validate(self, data: Any) -> None:
        try:
            self.validator.validate(data)
        except ValidationError as e:
            raise SchemaValidationError(f"Failed to validate {self.schema.to_string()}: {e.message}")

    def is_valid(self, data: Any) -> bool:
        return self.validator.is_valid(data)


class BaseIgluRegistry:
    def _get_validator_cls(self, meta_schema: str) -> Type[Validator]:
        # for now we will always use the latest validator
        if IGNORE_META_SCHEMA:
            return Draft202012Validator
        raise NotImplementedError("Selecting validator based on meta schema is not implemented")

    def _get_validator_and_check_schema(self, schema: Any) -> Type[Validator]:
        # get the meta schema
        meta_schema_name = schema["$schema"]
        # validate the schema itself
        validator_cls = self._get_validator_cls(meta_schema_name)
        validator_cls.check_schema(schema)
        return validator_cls(schema)

    def get(self, schema: str) -> Optional[IgluSchemaEntry]:
        iglu = IgluSchema.from_string(schema)
        try:
            return self.get_schema(iglu)
        except SchemaError as e:
            print(f"Failed to load schema: {e}")
            raise InvalidSchemaError(f"Invalid Schema: {e}")

    def get_schema(self, schema: IgluSchema) -> Optional[IgluSchemaEntry]:
        raise NotImplementedError("Method not implemented")


class HardcodedIgluRegistry(BaseIgluRegistry):
    def __init__(self, additional_schemas: Optional[Dict[str, Any]] = None) -> None:
        self.schemas = {}
        if additional_schemas is not None:
            self.update(additional_schemas)
        self.schemas.update(STATIC_JSON_SCHEMAS)

    def get_schema(self, schema: IgluSchema) -> Optional[IgluSchemaEntry]:
        p = schema.to_path()
        if p == "io.datenstrom/atomic/jsonschema/1-0-0":
            schema_object = dict(ATOMIC_EVENT_SCHEMA)
            return IgluSchemaEntry(schema=schema, schema_object=schema_object,
                                   validator=self._get_validator_and_check_schema(schema_object))
        if p in self.schemas:
            schema_object = self.schemas[p]
            return IgluSchemaEntry(schema=schema, schema_object=schema_object,
                                   validator=self._get_validator_and_check_schema(schema_object))
        return None


class RemoteIgluRegistry(BaseIgluRegistry):
    def __init__(self, url: str, cache_size: Optional[int] = 1024,
                 cache_ttl: Optional[int] = 3600, cache_ttl_none: Optional[int] = 60) -> None:
        self.url = url
        self.cache = TTLCache(maxsize=cache_size, ttl=cache_ttl, none_ttl=cache_ttl_none)

    @cachedmethod(lambda self: self.cache, key=lambda s, iglus: iglus.hashkey())
    def _load_iglu_schema(self, iglu_schema: IgluSchema) -> Optional[Dict[str, Any]]:
        # TODO: Implement better retry logic
        full_url = self.url + iglu_schema.to_path()
        print(f"Loading schema: {full_url}")
        # load the schema
        r = requests.get(full_url)
        # check if the request was successful
        if r.status_code < 200 or r.status_code >= 300:
            return None
        # get content length
        content_length = int(r.headers.get("content-length", 0))
        if content_length > MAX_SCHEMA_SIZE:
            print(f"Schema ({iglu_schema}) too large: {content_length} bytes")
            return None
        # parse response json
        return dict(r.json())

    def get_schema(self, schema: IgluSchema) -> Optional[IgluSchemaEntry]:
        schema_object = self._load_iglu_schema(schema)
        if schema_object:
            return IgluSchemaEntry(schema=schema, schema_object=schema_object,
                                   validator=self._get_validator_and_check_schema(schema_object))
        return None
