import orjson

from typing import List, Optional, Dict, Any, Callable
from urllib.parse import parse_qs

from datenstrom.common.schema.raw import CollectorPayload
from datenstrom.common.schema.atomic import AtomicEvent
from datenstrom.common.registry.manager import RegistryManager
from datenstrom.processing.enrichments.transformer import TransformEnrichment, transform_tstamp
from datenstrom.processing.enrichments.base import TemporaryAtomicEvent, BaseEnrichment, RemoteEnrichmentConfig
from datenstrom.processing.enrichments.postprocessing import PostProcessingEnrichment
from datenstrom.processing.enrichments.geoip import GeoIPEnrichment
from datenstrom.processing.enrichments.tenant import TenantEnrichment
from datenstrom.processing.enrichments.payload import ContextExtractionEnrichment, EventExtractionEnrichment
from datenstrom.processing.enrichments.campaign import CampaignEnrichment
from datenstrom.processing.enrichments.device import DeviceEnrichment
from datenstrom.processing.enrichments.pii_processing import PIIProcessor
from datenstrom.processing.version import VERSION
from datenstrom.common.cache import CachedRequestClient


httpclient = CachedRequestClient(maxsize=2048, ttl=3600, none_ttl=300)


SP_PAYLOAD_SCHEMA_START = "iglu:com.snowplowanalytics.snowplow/payload_data/jsonschema/1"

PV_SCHEMA = "iglu:io.datenstrom/page_view/jsonschema/1-0-0"
PP_SCHEMA = "iglu:io.datenstrom/page_ping/jsonschema/1-0-0"
SE_SCHEMA = "iglu:io.datenstrom/structured_event/jsonschema/1-0-0"
TR_SCHEMA = "iglu:io.datenstrom/transaction/jsonschema/1-0-0"
TI_SCHEMA = "iglu:io.datenstrom/transaction_item/jsonschema/1-0-0"


def get_iglu_schema_for_event_type(event_type: str) -> str:
        if event_type == "pv":
            return PV_SCHEMA
        if event_type == "pp":
            return PP_SCHEMA
        if event_type == "se":
            return SE_SCHEMA
        if event_type == "tr":
            return TR_SCHEMA
        if event_type == "ti":
            return TI_SCHEMA
        raise ValueError(f"Invalid event type: {event_type}")


class ProcessingInfoEnrichment(BaseEnrichment):
    def __init__(self, config: Any) -> None:
        super().__init__(config=config)

    def enrich(self, event: TemporaryAtomicEvent) -> None:
        # add processing info
        event.set_value("v_etl", VERSION)
        event.set_value("v_collector", event.raw_event.collector)
        event.set_value("collector_tstamp", transform_tstamp(event.raw_event.timestamp))
        event.set_value("collector_host", event.raw_event.hostname)


class RawProcessor():
    def __init__(self, config: Optional[Any] = None) -> None:
        self.registry = RegistryManager(config=config)
        self.enrichments = []
        self.config = config or {}
        self.setup_enrichments(self.config)

    def setup_enrichments(self, config: Optional[Any] = None) -> None:
        self.enrichments.append(ProcessingInfoEnrichment(config=config))
        self.enrichments.append(TransformEnrichment(config=config))
        self.enrichments.append(EventExtractionEnrichment(config=config, registry=self.registry))
        self.enrichments.append(ContextExtractionEnrichment(config=config, registry=self.registry))
        if config.get("tenant_lookup_endpoint"):
            self.enrichments.append(TenantEnrichment(config=config))
        if config.get("geoip_enabled"):
            self.enrichments.append(GeoIPEnrichment(config=config))
        if config.get("campaign_enrichment_enabled"):
            self.enrichments.append(CampaignEnrichment(config=config))
        if config.get("device_enrichment_enabled"):
            self.enrichments.append(DeviceEnrichment(config=config))
        self.enrichments.append(PostProcessingEnrichment(config=config))

    def extract_events_from_body(self, body: bytes,
                                 content_type: Optional[str] = None,
                                 schema: Optional[str] = None) -> List[Dict[str, Any]]:
        if content_type and "application/x-www-form-urlencoded" in content_type:
            raise NotImplementedError("Form data not supported yet")

        data = orjson.loads(body)
        # data should always be a dict
        if not isinstance(data, dict):
            raise ValueError("Invalid body (not a dict)")

        # payload data with multiple inner events
        if ("schema" in data and data["schema"] and
            data["schema"].startswith(SP_PAYLOAD_SCHEMA_START)):
            # we have a payload_data schema
            # this could be multiple events - we have to flatten them
            # we need to have a data field
            if "data" not in data:
                raise ValueError("Missing data in body")
            self.registry.validate(schema=data["schema"], data=data["data"])
            return list(data["data"])

        if schema:
            # we have a schema - the body should be a single event of this schema
            self.registry.validate(schema=schema, data=data)
            return [{"schema": schema, "event": data}]

        # self describing event - we need schema and data
        if "schema" not in data:
            raise ValueError("Missing schema in body")
        if "data" not in data:
            raise ValueError("Missing data in body")
        return [{"schema": data["schema"], "event": data["data"]}]

    def get_remote_config(self, hostname: str) -> Optional[RemoteEnrichmentConfig]:
        if self.config.remote_config_endpoint:
            url = f"{self.config.remote_config_endpoint}?hostname={hostname}"
            config = httpclient.get_json(url, timeout=5)
            if config:
                if "enable_full_ip" in config and isinstance(config["enable_full_ip"], bool):
                    return RemoteEnrichmentConfig(enable_full_ip=config["enable_full_ip"])
        return None

    def process_raw_event(self, raw_event: CollectorPayload) -> List[AtomicEvent]:
        # get config for this host/collector
        remote_config = None
        if raw_event.hostname:
            remote_config = self.get_remote_config(raw_event.hostname)

        # prepare the initial event dict
        event_dict = {}
        if raw_event.ipAddress:
            event_dict["ip"] = raw_event.ipAddress
        if raw_event.userAgent:
            event_dict["ua"] = raw_event.userAgent
        if raw_event.networkUserId:
            event_dict["nuid"] = raw_event.networkUserId

        # get information from query string
        if raw_event.querystring:
            qs = parse_qs(raw_event.querystring)
            # flatten the query string
            qs = {k: v[0] for k, v in qs.items()}
            # add query string to event dict
            event_dict.update(qs)

        # see if we have an explicit schema or an event type in the query parameters
        if "e" in event_dict:
            # we have an event type set that might tell us the schema
            event_type = event_dict["e"]
            if event_type != "ue":
                event_dict["schema"] = get_iglu_schema_for_event_type(event_type)

        all_events = []
        # check if we have a body
        # if we have a body it might be multiple events
        if raw_event.body:
            raw_event_dicts = self.extract_events_from_body(raw_event.body,
                                                            content_type=raw_event.contentType,
                                                            schema=event_dict.get("schema"))
            for raw_event_dict in raw_event_dicts:
                ed = dict(event_dict)
                ed.update(raw_event_dict)
                all_events.append(ed)
        # if we dont have a body, we have a single event
        else:
            all_events.append(event_dict)

        # we now have the initial data for all events
        # they should contain a schema that is validated
        atomic_events = []
        for e in all_events:
            # try to get the schema again from the inner event_type
            if "e" in e:
                event_type = e["e"]
                if event_type != "ue":
                    e["schema"] = get_iglu_schema_for_event_type(event_type)

            te = TemporaryAtomicEvent(raw_event=raw_event, initial_data=e)
            # run enrichments
            for enrichment in self.enrichments:
                enrichment.enrich(te)

            # run pii redaction
            PIIProcessor(remote_config).run(te)

            atomic_events.append(te.to_atomic_event())
        return atomic_events
