import orjson

from typing import Optional, Any, Dict, List
from datetime import datetime
from io import BytesIO

from pydantic import BaseModel, Field
from fastavro import parse_schema, schemaless_writer, schemaless_reader


class SelfDescribingEvent(BaseModel):
    schema_name: str = Field(alias="schema")
    data: dict[str, Any]

    # overwrite parent model_dump method with by_alias=True
    def model_dump(self, by_alias: bool = True, **kwargs) -> Dict[str, Any]:
        return super().model_dump(by_alias=by_alias, **kwargs)

    # overwrite parent model_dump_json method with by_alias=True
    def model_dump_json(self, by_alias: bool = True, **kwargs) -> str:
        return super().model_dump_json(by_alias=by_alias, **kwargs)


class SelfDescribingContext(BaseModel):
    schema_name: str = Field(alias="schema")
    data: dict[str, Any]

    # overwrite parent model_dump method with by_alias=True
    def model_dump(self, by_alias: bool = True, **kwargs) -> Dict[str, Any]:
        return super().model_dump(by_alias=by_alias, **kwargs)

    # overwrite parent model_dump_json method with by_alias=True
    def model_dump_json(self, by_alias: bool = True, **kwargs) -> str:
        return super().model_dump_json(by_alias=by_alias, **kwargs)


class AtomicEvent(BaseModel):
    event_id: str

    # Application Fields
    collector_host: str
    collector_auth: Optional[str] = None
    app_id: Optional[str] = None
    platform: str

    # metadata
    event_vendor: str
    event_name: str
    event_version: str

    # Date and Time Fields
    tstamp: datetime
    collector_tstamp: datetime
    dvce_created_tstamp: Optional[datetime] = None
    dvce_sent_tstamp: Optional[datetime] = None
    true_tstamp: Optional[datetime] = None
    etl_tstamp: datetime

    # Versioning
    v_tracker: Optional[str] = None
    v_collector: str
    v_etl: str
    name_tracker: Optional[str] = None

    # User
    user_ipaddress: Optional[str] = None
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    session_idx: Optional[int] = None
    domain_userid: Optional[str] = None
    domain_sessionid: Optional[str] = None
    domain_sessionidx: Optional[int] = None
    network_userid: Optional[str] = None

    # Location
    geo_country: Optional[str] = None
    geo_region: Optional[str] = None
    geo_city: Optional[str] = None

    # Common
    useragent: Optional[str] = None
    language: Optional[str] = None

    # Identifiers
    device_id: Optional[str] = None
    tenant_id: Optional[str] = None

    # Structured Event
    category: Optional[str] = None
    action: Optional[str] = None
    label: Optional[str] = None
    property: Optional[str] = None
    value: Optional[str] = None

    # Data
    contexts: List[SelfDescribingContext] = Field(default_factory=list)
    event: SelfDescribingEvent

    def to_hive_serializable(self):
        dict_data = self.model_dump(by_alias=True)
        # convert contexts to json
        for i, context in enumerate(dict_data["contexts"]):
            schema = context["schema"]
            data = context["data"]
            dict_data["contexts"][i] = {
                "schema": schema,
                "data": orjson.dumps(data).decode("utf-8")
            }
        # convert event to json
        dict_data["event"]["data"] = orjson.dumps(dict_data["event"]["data"]).decode("utf-8")
        return dict_data

    # overwrite parent model_dump method with by_alias=True
    def model_dump(self, by_alias: bool = True, **kwargs) -> Dict[str, Any]:
        return super().model_dump(by_alias=by_alias, **kwargs)

    # overwrite parent model_dump_json method with by_alias=True
    def model_dump_json(self, by_alias: bool = True, **kwargs) -> str:
        return super().model_dump_json(by_alias=by_alias, **kwargs)

    def to_avro(self):
        return to_avro(self)

    @classmethod
    def from_avro(cls, b: bytes):
        return from_avro(b)


ATOMIC_EVENT_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "description": "Schema for an atomic event in datenstrom",
    "self": {
        "vendor": "io.datenstrom",
        "name": "atomic",
        "format": "jsonschema",
        "version": "1-0-0"
    },
    "type": "object",
    "properties": {
        "event_id": {
            "type": "string",
            "description": "Unique ID for the event"
        },
        "collector_host": {
            "type": "string",
            "description": "Hostname of the collector"
        },
        "collector_auth": {
            "type": ["string", "null"],
            "description": "Client authentication subject"
        },
        "app_id": {
            "type": ["string", "null"],
            "description": "Application ID"
        },
        "platform": {
            "type": "string",
            "description": "Platform of the event"
        },

        "event_vendor": {
            "type": "string",
            "description": "Vendor of the event schema"
        },
        "event_name": {
            "type": "string",
            "description": "Name of the event schema"
        },
        "event_version": {
            "type": "string",
            "description": "Version of the event schema"
        },

        "tstamp": {
            "type": "string",
            "format": "date-time",
            "description": "Best timestamp of the event"
        },
        "collector_tstamp": {
            "type": "string",
            "format": "date-time",
            "description": "Timestamp when the event was collected"
        },
        "dvce_created_tstamp": {
            "type": ["string", "null"],
            "format": "date-time",
            "description": "Timestamp when the event was created"
        },
        "dvce_sent_tstamp": {
            "type": ["string", "null"],
            "format": "date-time",
            "description": "Timestamp when the event was sent"
        },
        "true_tstamp": {
            "type": ["string", "null"],
            "format": "date-time",
            "description": "Real timestamp of the event"
        },
        "etl_tstamp": {
            "type": "string",
            "format": "date-time",
            "description": "Timestamp when the event was processed"
        },

        "v_tracker": {
            "type": ["string", "null"],
            "description": "Version of the tracker"
        },
        "v_collector": {
            "type": "string",
            "description": "Version of the collector"
        },
        "v_etl": {
            "type": "string",
            "description": "Version of the ETL"
        },
        "name_tracker": {
            "type": ["string", "null"],
            "description": "Name of the tracker"
        },

        "user_ipaddress": {
            "type": ["string", "null"],
            "description": "IP address of the user"
        },
        "user_id": {
            "type": ["string", "null"],
            "description": "ID of the user"
        },
        "session_id": {
            "type": ["string", "null"],
            "description": "ID of the session"
        },
        "session_idx": {
            "type": ["integer", "null"],
            "description": "Index of the session"
        },
        "domain_userid": {
            "type": ["string", "null"],
            "description": "ID of the domain user"
        },
        "domain_sessionid": {
            "type": ["string", "null"],
            "description": "ID of the domain session"
        },
        "domain_sessionidx": {
            "type": ["integer", "null"],
            "description": "Index of the domain session"
        },
        "network_userid": {
            "type": ["string", "null"],
            "description": "ID of the network user"
        },

        "geo_country": {
            "type": ["string", "null"],
            "description": "Country of the user"
        },
        "geo_region": {
            "type": ["string", "null"],
            "description": "Region of the user"
        },
        "geo_city": {
            "type": ["string", "null"],
            "description": "City of the user"
        },

        "useragent": {
            "type": ["string", "null"],
            "description": "User agent of the user"
        },
        "language": {
            "type": ["string", "null"],
            "description": "Language of the user"
        },

        "device_id": {
            "type": ["string", "null"],
            "description": "ID of the device"
        },
        "tenant_id": {
            "type": ["string", "null"],
            "description": "ID of the tenant"
        },

        "category": {
            "type": ["string", "null"],
            "description": "Category of the event"
        },
        "action": {
            "type": ["string", "null"],
            "description": "Action of the event"
        },
        "label": {
            "type": ["string", "null"],
            "description": "Label of the event"
        },
        "property": {
            "type": ["string", "null"],
            "description": "Property of the event"
        },
        "value": {
            "type": ["string", "null"],
            "description": "Value of the event"
        },

        "contexts": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "schema": {
                        "type": "string",
                        "pattern": "^iglu:[a-zA-Z0-9-_.]+/[a-zA-Z0-9-_]+/[a-zA-Z0-9-_]+/[0-9]+-[0-9]+-[0-9]+$"
                    },
                    "data": {}
                },
                "required": ["schema", "data"],
            },
            "description": "Contexts of the event"
        },
        "event": {
            "type": "object",
            "properties": {
                "schema": {
                    "type": "string",
                    "pattern": "^iglu:[a-zA-Z0-9-_.]+/[a-zA-Z0-9-_]+/[a-zA-Z0-9-_]+/[0-9]+-[0-9]+-[0-9]+$"
                },
                "data": {}
            },
            "required": ["schema", "data"],
            "additionalProperties": False,
            "description": "Event data"
        },
    },
    "required": [
        "event_id",
        "collector_host",
        "platform",
        "event_vendor",
        "event_name",
        "event_version",
        "tstamp",
        "collector_tstamp",
        "etl_tstamp",
        "v_collector",
        "v_etl",
        "event",
    ],
}


ATOMIC_AVRO_SCHEMA = {
    "type": "record",
    "name": "AtomicEvent",
    "namespace": "io.datenstrom",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "collector_host", "type": "string"},
        {"name": "collector_auth", "type": ["null", "string"]},
        {"name": "app_id", "type": ["null", "string"]},
        {"name": "platform", "type": "string"},
        {"name": "event_vendor", "type": "string"},
        {"name": "event_name", "type": "string"},
        {"name": "event_version", "type": "string"},
        {"name": "tstamp", "type": "string"},
        {"name": "collector_tstamp", "type": "string"},
        {"name": "dvce_created_tstamp", "type": ["null", "string"]},
        {"name": "dvce_sent_tstamp", "type": ["null", "string"]},
        {"name": "true_tstamp", "type": ["null", "string"]},
        {"name": "etl_tstamp", "type": "string"},
        {"name": "v_tracker", "type": ["null", "string"]},
        {"name": "v_collector", "type": "string"},
        {"name": "v_etl", "type": "string"},
        {"name": "name_tracker", "type": ["null", "string"]},
        {"name": "user_ipaddress", "type": ["null", "string"]},
        {"name": "user_id", "type": ["null", "string"]},
        {"name": "session_id", "type": ["null", "string"]},
        {"name": "session_idx", "type": ["null", "long"]},
        {"name": "domain_userid", "type": ["null", "string"]},
        {"name": "domain_sessionid", "type": ["null", "string"]},
        {"name": "domain_sessionidx", "type": ["null", "long"]},
        {"name": "network_userid", "type": ["null", "string"]},
        {"name": "geo_country", "type": ["null", "string"]},
        {"name": "geo_region", "type": ["null", "string"]},
        {"name": "geo_city", "type": ["null", "string"]},
        {"name": "useragent", "type": ["null", "string"]},
        {"name": "language", "type": ["null", "string"]},
        {"name": "device_id", "type": ["null", "string"]},
        {"name": "tenant_id", "type": ["null", "string"]},
        {"name": "category", "type": ["null", "string"]},
        {"name": "action", "type": ["null", "string"]},
        {"name": "label", "type": ["null", "string"]},
        {"name": "property", "type": ["null", "string"]},
        {"name": "value", "type": ["null", "string"]},
        {"name": "contexts", "type": {"type": "array", "items": {
            "type": "record", 
            "name": "self_describing_context", "fields": [
                {"name": "schema", "type": "string"},
                {"name": "data", "type": "string"}
            ]
        }}},
        {"name": "event", "type": {
            "type": "record", "name": "self_describing_event", "fields": [
                {"name": "schema", "type": "string"},
                {"name": "data", "type": "string"}
            ]
        }}
    ]
}

ATOMIC_AVRO = parse_schema(ATOMIC_AVRO_SCHEMA)


def to_avro(e: AtomicEvent):
    o = BytesIO()
    d = e.model_dump(mode="json", by_alias=True)
    # json encode context and event data
    for i, context in enumerate(d["contexts"]):
        d["contexts"][i]["data"] = orjson.dumps(context["data"]).decode("utf-8")
    d["event"]["data"] = orjson.dumps(d["event"]["data"]).decode("utf-8")
    print(d)
    schemaless_writer(o, ATOMIC_AVRO, d)
    return o.getvalue()


def from_avro(b: bytes) -> AtomicEvent:
    i = BytesIO(b)
    try:
        d = schemaless_reader(i, ATOMIC_AVRO)
    except EOFError:
        raise ValueError(f"Invalid AVRO message: {b}")
    # json decode context and event data
    for i, context in enumerate(d["contexts"]):
        d["contexts"][i]["data"] = orjson.loads(context["data"])
    d["event"]["data"] = orjson.loads(d["event"]["data"])
    return AtomicEvent.model_validate(d)
