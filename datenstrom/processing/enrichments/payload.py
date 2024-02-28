from typing import Any

import orjson
import base64

from datenstrom.processing.enrichments.base import BaseEnrichment, TemporaryAtomicEvent
from datenstrom.common.schema.atomic import AtomicEvent, SelfDescribingContext, SelfDescribingEvent
from datenstrom.common.registry import SchemaRegistry

from datenstrom.processing.enrichments.transformer import (
    run_transformations,
    PAGE_VIEW_TRANSFORMATIONS,
    PAGE_PING_TRANSFORMATIONS,
    TRANSACTION_TRANSFORMATIONS,
    TRANSACTION_ITEM_TRANSFORMATIONS,
    STRUCTURED_EVENT_TRANSFORMATIONS,
)


def read_base64_json(data: str) -> Any:
    # add missing padding to make it a multiple of 4
    missing_padding = len(data) % 4
    if missing_padding:
        data += "=" * (4 - missing_padding)
    return orjson.loads(base64.b64decode(data))


class EventExtractionEnrichment(BaseEnrichment):
    def __init__(self, config: Any, registry: SchemaRegistry) -> None:
        self.registry = registry
        super().__init__(config=config)

    def enrich(self, event: TemporaryAtomicEvent) -> None:
        # at this point we should have a schema or have a self describing event
        # in the ue_pr or ue_px field
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
        schema_parts = self.registry.get_schema_parts(schema)
        event.set_value("event_vendor", schema_parts.vendor)
        event.set_value("event_name", schema_parts.name)
        event.set_value("event_format", schema_parts.format)
        event.set_value("event_version", schema_parts.version)

        # TODO custom logic for
        if event["event_name"] == "page_view":
            run_transformations(event, PAGE_VIEW_TRANSFORMATIONS)
        elif event["event_name"] == "page_ping":
            run_transformations(event, PAGE_PING_TRANSFORMATIONS)
        elif event["event_name"] == "structured_event":
            run_transformations(event, STRUCTURED_EVENT_TRANSFORMATIONS)
        elif event["event_name"] == "transaction":
            run_transformations(event, TRANSACTION_TRANSFORMATIONS)
        elif event["event_name"] == "transaction_item":
            run_transformations(event, TRANSACTION_ITEM_TRANSFORMATIONS)

        # check if there is already an event
        if not event.has_event():
            if "event" in event:
                # we already have an event - just cast it to self describing and validate it
                self.registry.validate(schema=schema, data=event["event"])
                event.set_event(SelfDescribingEvent(schema=schema, data=event["event"]))
            else:
                # check if we can validate the schema with the data we have
                event_data = {k: v for k, v in event.temp_data.items()
                              if k in self.registry.get_schema_fields(schema)}
                self.registry.validate(schema=schema, data=event_data)
                # filter fields taht are part of the schema and create the event
                event.set_event(SelfDescribingEvent(schema=schema, data=event_data))

        # Flatten Structured Event
        sd_event = event.get_event()
        if "category" in sd_event.data:
            event.set_value("category", sd_event.data["category"])
        if "action" in sd_event.data:
            event.set_value("action", sd_event.data["action"])
        if "label" in sd_event.data:
            event.set_value("label", sd_event.data["label"])
        if "property" in sd_event.data:
            event.set_value("property", sd_event.data["property"])
        if "value" in sd_event.data:
            event.set_value("value", sd_event.data["value"])


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

            # Flatten session context
            schema_parts = self.registry.get_schema_parts(schema)
            if schema_parts.name == "client_session":
                if "sessionId" in data:
                    event.set_value("session_id", data["sessionId"])
                if "sessionIndex" in data:
                    event.set_value("session_idx", data["sessionIndex"])
