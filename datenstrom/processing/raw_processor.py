import orjson
import base64

from typing import List, Optional, Dict, Any, Callable
from urllib.parse import parse_qs

from datenstrom.common.schema.raw import CollectorPayload
from datenstrom.common.schema.atomic import AtomicEvent, SelfDescribingContext, SelfDescribingEvent
from datenstrom.common.registry import SchemaRegistry
from datenstrom.processing.enrichments.transformer import TransformEnrichment, transform_tstamp
from datenstrom.processing.enrichments.base import TemporaryAtomicEvent, BaseEnrichment
from datenstrom.processing.enrichments.postprocessing import PostProcessingEnrichment
from datenstrom.processing.enrichments.geoip import GeoIPEnrichment
from datenstrom.processing.version import VERSION

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


def read_base64_json(data: str) -> Any:
    # add missing padding to make it a multiple of 4
    missing_padding = len(data) % 4
    if missing_padding:
        data += "=" * (4 - missing_padding)
    return orjson.loads(base64.b64decode(data))


class ProcessingInfoEnrichment(BaseEnrichment):
    def __init__(self, config: Any,
                 tenant_getter: Optional[Callable] = None) -> None:
        self.tenant_getter = tenant_getter
        super().__init__(config=config)

    def enrich(self, event: TemporaryAtomicEvent) -> None:
        # add processing info
        event.set_value("v_etl", VERSION)
        event.set_value("v_collector", event.raw_event.collector)
        event.set_value("collector_tstamp", transform_tstamp(event.raw_event.timestamp))
        event.set_value("collector_host", event.raw_event.hostname)

        # getting tenant information
        if self.tenant_getter:
            tenant = self.tenant_getter(event.raw_event)
            if tenant:
                event.set_value("tenant", tenant)


class EventExtractionEnrichment(BaseEnrichment):
    def __init__(self, config: Any, registry: SchemaRegistry) -> None:
        self.registry = registry
        super().__init__(config=config)

    def enrich(self, event: TemporaryAtomicEvent) -> None:
        # at this point we should have a schema or have a self describing event
        # in the ue_pr or ue_px field
        print(event.raw_event)

        if "schema" not in event:
            if "ue_px" in event:
                # self describing event in base64 encoded json
                self_describing_event = read_base64_json(event["ue_px"])
            elif "ue_pr" in event:
                # self describing event in json
                self_describing_event = orjson.loads(event["ue_pr"])
            else:
                raise ValueError("No schema and no schema and no self describing event")

            # check if we have a schema and data
            if "schema" not in self_describing_event:
                raise ValueError("Missing schema in self describing event")
            if "data" not in self_describing_event:
                raise ValueError("Missing data in self describing event")
            
            # at this point we should have the schema of an unstructured event
            # the real event is nested inside the data field
            # we could validate the unstructred schema here
            # self.registry.validate(schema=self_describing_event["schema"],
            #                        data=self_describing_event["data"])
            
            inner_event = self_describing_event["data"]
            # check if we have a schema and data
            if "schema" not in inner_event:
                raise ValueError("Missing schema in inner self describing event")
            if "data" not in inner_event:
                raise ValueError("Missing data in inner self describing event")

            # at this point we should have the schema of the real event
            # and can validate it
            self.registry.validate(schema=inner_event["schema"],
                                   data=inner_event["data"])
            # set
            event["schema"] = inner_event["schema"]
            event.set_event(SelfDescribingEvent(schema=inner_event["schema"],
                                                data=inner_event["data"]))

        schema = event["schema"]
        # parse the schema
        iglu_schema = self.registry.parse_iglu_schema(schema)
        event.set_value("event_vendor", iglu_schema.vendor)
        event.set_value("event_name", iglu_schema.name)
        event.set_value("event_format", iglu_schema.format)
        event.set_value("event_version", iglu_schema.version)

        # TODO custom logic for
        # - page_view
        # - page_ping
        # - structured_event
        # - transaction
        # - transaction_item

        # check if there is already an event
        if not event.has_event():
            if "event" in event:
                # we already have an event - just cast it to self describing and validate it
                self.registry.validate(schema=schema, data=event["event"])
                event.set_event(SelfDescribingEvent(schema=schema, data=event["event"]))
            else:
                # check if we can validate the schema with the data we have
                event_data = {k: v for k, v in event.temp_data.items() if k in self.registry.get_fields(schema)}
                self.registry.validate(schema=schema, data=event_data)
                # filter fields taht are part of the schema and create the event
                event.set_event(SelfDescribingEvent(schema=schema, data=event_data))


class ContextExtractionEnrichment(BaseEnrichment):
    def __init__(self, config: Any, registry: SchemaRegistry) -> None:
        self.registry = registry
        super().__init__(config=config)

    def enrich(self, event: TemporaryAtomicEvent) -> None:
        # context can be in co (json object) or cx (base64 encoded)
        if "cx" in event:
            # we have a base64 encoded string
            context = read_base64_json(event["cx"])
        elif "co" in event:
            # we have a json string
            context = orjson.loads(event["co"])
        else:
            return

        # context should have schema and data fields
        if "schema" not in context:
            raise ValueError("Missing schema in contexts")
        if "data" not in context:
            raise ValueError("Missing data in contexts")
        
        # we could check if schema is:
        # iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0
        # but for now we just assume it

        context_list = context["data"]
        for c in context_list:
            if "schema" not in c:
                raise ValueError("Missing schema in contexts")
            if "data" not in c:
                raise ValueError("Missing data in contexts")
            schema = c["schema"]
            data = c["data"]
            # validate the context
            self.registry.validate(schema=schema, data=data)
            event.add_context(SelfDescribingContext(schema=schema, data=data))


class RawProcessor():
    def __init__(self, config: Optional[Any] = None) -> None:
        self.registry = SchemaRegistry()
        self.enrichments = []
        self.config = config or {}
        self.setup_enrichments(self.config)

    def setup_enrichments(self, config: Optional[Any] = None) -> None:
        self.enrichments.append(ProcessingInfoEnrichment(config=config))
        self.enrichments.append(TransformEnrichment(config=config))
        self.enrichments.append(EventExtractionEnrichment(config=config, registry=self.registry))
        self.enrichments.append(ContextExtractionEnrichment(config=config, registry=self.registry))
        if config.get("geoip_enabled"):
            self.enrichments.append(GeoIPEnrichment(config=config))
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

    def process_raw_event(self, raw_event: CollectorPayload) -> List[AtomicEvent]:
        print("Processing raw event")

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
            print("Processing single GET event")
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

            # check the event and the contexts against the schema
            se = te.get_event()
            # we need to have an event at this point
            if not se:
                raise ValueError("Missing event data")
            self.registry.validate(schema=se.schema_name, data=se.data)

            for c in te.get_contexts():
                self.registry.validate(schema=c.schema_name, data=c.data)

            atomic_events.append(te.to_atomic_event())
        return atomic_events

        # now we can do enrichments for every event
        # TODO
        print("Done processing raw event")
        return [AtomicEvent.model_validate(e) for e in all_events]
