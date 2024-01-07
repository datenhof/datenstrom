from typing import List

from datenstrom.collector.settings import config
from datenstrom.processing.raw_processor import RawProcessor
from datenstrom.common.schema.raw import CollectorPayload
from datenstrom.connectors.sources.sqs import SQSSource
from datenstrom.connectors.sinks.sqs import SQSSink
from signal import signal, SIGINT, SIGTERM


class SignalHandler:
    def __init__(self):
        self.received_signal = False
        signal(SIGINT, self._signal_handler)
        signal(SIGTERM, self._signal_handler)

    def _signal_handler(self, signal, frame):
        print(f"handling signal {signal}, exiting gracefully")
        self.received_signal = True


class Enricher:
    def __init__(self, config):
        self.config = config
        assert config.sink == "sqs"
        self.raw_processor = RawProcessor(config=config)
        self.source = SQSSource(config=config, queue_type="raw")
        self.sink = SQSSink(config=config, queue_type="events")
        # self.error_sink = SQSSink(config=config, queue_type="errors")

    def enrich(self, message: bytes) -> List[bytes]:
        if config.record_format == "avro":
            payload = CollectorPayload.from_avro(message)
        elif config.record_format == "thrift":
            payload = CollectorPayload.from_thrift(message)
        else:
            raise ValueError(f"Unknown record format: {config.record_format}")
        atomic_events = self.raw_processor.process_raw_event(payload)
        output_messages = []
        for a in atomic_events:
            json_string = a.model_dump_json()
            output_messages.append(json_string.encode("utf-8"))
        return output_messages

    def run(self):
        signal_handler = SignalHandler()
        while not signal_handler.received_signal:
            messages = self.source.read()
            for message in messages:
                try:
                    enriched_messages = self.enrich(message.data())
                except Exception as e:
                    print(f"exception while processing message: {repr(e)}")
                    continue
                self.sink.write(enriched_messages)
                print(f"processed {len(enriched_messages)} messages")
                message.ack()


if __name__ == "__main__":
    enricher = Enricher(config=config)
    enricher.run()
