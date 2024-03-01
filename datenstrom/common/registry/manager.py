from typing import Optional, NamedTuple, Any, List
from functools import lru_cache

from datenstrom.settings import BaseConfig
from datenstrom.common.registry.iglu import (
    IgluSchemaEntry, BaseIgluRegistry,
    RemoteIgluRegistry, IgluSchema, HardcodedIgluRegistry
)
from datenstrom.common.registry.base import SchemaNotFound


VALIDATOR_CACHE_SIZE = 100


class RegistryEntry(NamedTuple):
    url: str
    type: str
    registry: BaseIgluRegistry


class RegistryManager:
    def __init__(self, config: Optional[BaseConfig] = None):
        self.registries = []
        if config is not None:
            self.setup(config)

    def setup(self, config: BaseConfig) -> None:
        self.config = config
        self.iglu_registries = config.iglu_schema_registries
        self.cache_ttl = config.default_cache_ttl
        self.cache_ttl_none = config.none_cache_ttl

        self.registries = []
        # add static registry
        self.registries.append(
            RegistryEntry(url="hardcoded", type="iglu",
                          registry=HardcodedIgluRegistry())
        )

        # setup iglu schema registries
        if not self.iglu_registries:
            raise ValueError("No valid iglu Schema registries found in config")

        for registry_url in self.iglu_registries:
            print(f"Adding IGLU schema registry: {registry_url}")
            self.add_registry(url=registry_url, type="iglu")

    def add_registry(self, url: str, type: str) -> None:
        # add a registry to the list if it is not already present
        if url not in [r.url for r in self.registries]:
            self.registries.append(RegistryEntry(url=url, type=type, registry=RemoteIgluRegistry(url=url)))

    def validate(self, schema: str, data: Any) -> None:
        t = self.get_schema_type(schema)
        entry = self.get_iglu_schema(schema)
        entry.validate(data)

    def is_valid(self, schema: str, data: Any) -> bool:
        t = self.get_schema_type(schema)
        entry = self.get_iglu_schema(schema)
        return entry.is_valid(data)

    def get_schema_fields(self, schema: str) -> List[str]:
        t = self.get_schema_type(schema)
        entry = self.get_iglu_schema(schema)
        return entry.get_fields()

    def get_schema_type(self, schema: str) -> str:
        if schema.startswith("iglu:"):
            return "iglu"
        raise ValueError("Invalid schema - only supporting iglu schemas")

    def get_schema_parts(self, schema: str) -> IgluSchema:
        t = self.get_schema_type(schema)
        entry = self.get_iglu_schema(schema)
        return entry.schema

    @lru_cache(maxsize=VALIDATOR_CACHE_SIZE)
    def get_iglu_schema(self, schema: str) -> IgluSchemaEntry:
        for registry in self.registries:
            if registry.type == "iglu":
                schema_entry = registry.registry.get(schema)
                if schema_entry:
                    return schema_entry
        raise SchemaNotFound(f"Schema not found in any registry: {schema}")