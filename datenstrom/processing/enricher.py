from typing import List

from datenstrom.settings import BaseConfig, get_settings
from datenstrom.common.schema.raw import CollectorPayload, ErrorPayload
from datenstrom.processing.processor import RawEventProcessor
from datenstrom.processing.raw_processor import RawProcessor
from datenstrom.common.registry.base import SchemaNotFound, SchemaValidationError, InvalidSchemaError


class Enricher(RawEventProcessor):
    def __init__(self, config: BaseConfig):
        super().__init__(config)

        # Setting up sink
        if config.atomic_event_transport:
            transport = config.atomic_event_transport
        else:
            transport = config.transport

        if transport == "sqs":
            from datenstrom.connectors.sinks.sqs import SQSSink
            self.sink = SQSSink(config=config, queue_type="events")
            self.error_sink = SQSSink(config=config, queue_type="errors")
        elif transport == "kafka":
            from datenstrom.connectors.sinks.kafka import KafkaSink
            self.sink = KafkaSink(config=config, queue_type="events")
            self.error_sink = KafkaSink(config=config, queue_type="errors")
        elif transport == "dev":
            from datenstrom.connectors.sinks.dev import DevSink
            self.sink = DevSink(config=config, queue_type="events")
            self.error_sink = DevSink(config=config, queue_type="errors")
        else:
            raise ValueError(f"Cannot use sink {transport} as enricher sink.")

        self.raw_processor = RawProcessor(config=config)

    def enrich(self, event: CollectorPayload) -> List[bytes]:
        atomic_events = self.raw_processor.process_raw_event(event)
        output_messages = []
        for a in atomic_events:
            json_string = a.model_dump_json(by_alias=True)
            output_messages.append(json_string.encode("utf-8"))
        return output_messages

    def process_single(self, event: CollectorPayload) -> bool:
        try:
            enriched_events = self.enrich(event)
        except SchemaNotFound as e:
            print(f"schema not found: {e}")
            error = ErrorPayload(collector_domain=event.hostname,
                                 reason=f"schema not found: {e}",
                                 payload=event.to_json().encode("utf-8"))
            self.error_sink.write([error.to_bytes()])
            return False
        except SchemaValidationError as e:
            print(f"data validation error: {e}")
            error = ErrorPayload(collector_domain=event.hostname,
                                 reason=f"data validation error: {e}",
                                 payload=event.to_json().encode("utf-8"))
            self.error_sink.write([error.to_bytes()])
            return False
        except InvalidSchemaError as e:
            print(f"invalid schema: {e}")
            error = ErrorPayload(collector_domain=event.hostname,
                                 reason=f"invalid schema: {e}",
                                 payload=event.to_json().encode("utf-8"))
            self.error_sink.write([error.to_bytes()])
            return False
        except ValueError as e:
            print(f"invalid event data: {e}")
            error = ErrorPayload(collector_domain=event.hostname,
                                 reason=f"invalid event data: {e}",
                                 payload=event.to_json().encode("utf-8"))
            self.error_sink.write([error.to_bytes()])
            return False

        self.sink.write(enriched_events)
        return True

    def process(self, raw_events: List[CollectorPayload]) -> List[bool]:
        return [self.process_single(e) for e in raw_events]


if __name__ == "__main__":
    config = get_settings()
    enricher = Enricher(config=config)
    enricher.run()
