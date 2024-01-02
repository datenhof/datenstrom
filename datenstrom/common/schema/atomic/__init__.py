from typing import Optional, Any, List
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
    identifier: Optional[str] = None
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

    # Data
    contexts: List[SelfDescribingContext] = Field(default_factory=list)
    event: SelfDescribingEvent
