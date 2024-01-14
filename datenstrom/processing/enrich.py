import time

from typing import List

from datenstrom.processing.raw_processor import RawProcessor
from datenstrom.common.schema.raw import CollectorPayload
from datenstrom.connectors.sources.sqs import SQSSource
from datenstrom.connectors.sinks.sqs import SQSSink
from datenstrom.common.registry import SchemaNotFound, SchemaError
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

    def _process(self, message: bytes) -> List[bytes]:
        if self.config.record_format == "avro":
            payload = CollectorPayload.from_avro(message)
        elif self.config.record_format == "thrift":
            payload = CollectorPayload.from_thrift(message)
        else:
            raise ValueError(f"Unknown record format: {self.config.record_format}")
        atomic_events = self.raw_processor.process_raw_event(payload)
        output_messages = []
        for a in atomic_events:
            json_string = a.model_dump_json(by_alias=True)
            output_messages.append(json_string.encode("utf-8"))
        return output_messages

    def process_message(self, message: bytes) -> List[bytes]:
        try:
            return self._process(message)
        except SchemaNotFound as e:
            print(f"schema not found: {e}")
            # self.error_sink.write([message])
            return []
        except SchemaError as e:
            print(f"schema error: {e}")
            # self.error_sink.write([message])
            return []
        except ValueError as e:
            print(f"invalid message: {e}")
            # self.error_sink.write([message])
            return []

    def run(self):
        signal_handler = SignalHandler()
        while not signal_handler.received_signal:
            messages = self.source.read()
            t0 = time.time()
            counter = 0
            for message in messages:
                enriched_messages = self.process_message(message.data())
                self.sink.write(enriched_messages)
                counter += len(enriched_messages)
                message.ack()
            t = (time.time() - t0) * 1000.0
            if counter > 0:
                print(f"processed {counter} messages in {t:.2f} milliseconds")
