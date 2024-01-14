import orjson

from typing import Optional, Any, Dict
from datetime import datetime
from pydantic import BaseModel, Field


class SelfDescribingEvent(BaseModel):
    schema_name: str = Field(alias="schema")
    data: dict[str, Any]


class SelfDescribingContext(BaseModel):
    schema_name: str = Field(alias="schema")
    data: dict[str, Any]


class AtomicEvent(BaseModel):
    event_id: str

    # Application Fields
    collector_host: str
    tenant: Optional[str] = None
    collector_auth: Optional[str] = None
    app_id: Optional[str] = None
    platform: str

    # metadata
    event_vendor: str
    event_name: str
    event_format: str
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

    # Structured Event
    category: Optional[str] = None
    action: Optional[str] = None
    label: Optional[str] = None
    property: Optional[str] = None
    value: Optional[str] = None

    # Data
    contexts: Dict[str, SelfDescribingContext] = Field(default_factory=dict)
    event: SelfDescribingEvent

    def to_hive_serializable(self):
        dict_data = self.model_dump(by_alias=True)
        # convert contexts to json
        for s, context in dict_data["contexts"].items():
            schema = context["schema"]
            data = context["data"]
            dict_data["contexts"][s] = {
                "schema": schema,
                "data": orjson.dumps(data).decode("utf-8")
            }
        # convert event to json
        dict_data["event"]["data"] = orjson.dumps(dict_data["event"]["data"]).decode("utf-8")
        return dict_data


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
        "tenant": {
            "type": ["string", "null"],
            "description": "Tenant ID"
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
        "event_format": {
            "type": "string",
            "description": "Format of the event schema"
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
            "type": "object",
            "additionalProperties": {
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
        "event_format",
        "event_version",
        "tstamp",
        "collector_tstamp",
        "etl_tstamp",
        "v_collector",
        "v_etl",
        "event",
    ],
}