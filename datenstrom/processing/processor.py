import time

from typing import List, Literal, Union, Optional

from datenstrom.common.schema.raw import CollectorPayload, ErrorPayload
from datenstrom.common.schema.atomic import AtomicEvent
from datenstrom.connectors.sinks.dev import DevSink
# from datenstrom.common.registry import SchemaNotFound, SchemaError
from signal import signal, SIGINT, SIGTERM
from datenstrom.settings import BaseConfig


class SignalHandler:
    def __init__(self):
        self.received_signal = False
        signal(SIGINT, self._signal_handler)
        signal(SIGTERM, self._signal_handler)

    def _signal_handler(self, signal, frame):
        print(f"handling signal {signal}, exiting gracefully")
        self.received_signal = True


class BaseProcessor:
    def __init__(self, config: BaseConfig, queue_type: Literal["raw", "events", "errors"]):
        self.config = config

        if queue_type == "events":
            if config.atomic_event_transport:
                transport = config.atomic_event_transport
            else:
                transport = config.transport
        else:
            transport = config.transport

        # Setting up source
        if transport == "sqs":
            from datenstrom.connectors.sources.sqs import SQSSource
            self.source = SQSSource(config=config, queue_type=queue_type)
        elif transport == "kafka":
            from datenstrom.connectors.sources.kafka import KafkaSource
            self.source = KafkaSource(config=config, queue_type=queue_type)
        elif transport == "dev":
            self.source = None
        else:
            raise ValueError(f"Cannot use source {transport} as processor source.")

        self._default_error_sink = DevSink(config=config, queue_type="errors")
        self.error_sink = self._default_error_sink

        if queue_type == "raw":
            self._decoder = self._decode_raw_message
            self._processor = self.process_raw
        elif queue_type == "events":
            self._decoder = self._decode_event_message
            self._processor = self.process_events
        elif queue_type == "errors":
            self._decoder = self._decode_error_message
            self._processor = self.process_errors

    def _decode_error_message(self, message: bytes) -> Union[ErrorPayload, bytes]:
        try:
            ev = ErrorPayload.model_validate_json(message)
        except ValueError as e:
            ev = message
        return ev

    def _decode_raw_message(self, message: bytes) -> Optional[CollectorPayload]:
        try:
            if self.config.record_format == "avro":
                payload = CollectorPayload.from_avro(message)
            elif self.config.record_format == "thrift":
                payload = CollectorPayload.from_thrift(message)
        except ValueError as e:
            print(f"cannot decode message: {e}")
            error = ErrorPayload(
                collector_domain="unknown",
                reason=f"cannot decode message: {e}",
                payload=message,
            )
            self.error_sink.write([error.to_bytes()])
            return None
        return payload

    def _decode_event_message(self, message: bytes) -> Optional[AtomicEvent]:
        try:
            ev = AtomicEvent.model_validate_json(message)
        except ValueError as e:
            print(f"cannot decode message: {e}")
            error = ErrorPayload(
                collector_domain="unknown",
                reason=f"cannot decode message: {e}",
                payload=message,
            )
            self.error_sink.write([error.to_bytes()])
            return None
        return ev

    def process_events(self, events: List[AtomicEvent]) -> List[bool]:
        raise NotImplementedError("process_events not implemented")
    
    def process_raw(self, raw_events: List[CollectorPayload]) -> List[bool]:
        raise NotImplementedError("process_raw not implemented")

    def run(self):
        signal_handler = SignalHandler()
        while not signal_handler.received_signal:
            messages = self.source.read()
            if len(messages) == 0:
                continue
            t0 = time.time()
            decoded_messages = []
            for message in messages:
                decoded_message = self._decoder(message.data())
                if decoded_message:
                    decoded_messages.append(decoded_message)
            results = self._processor(decoded_messages)
            success_counter = results.count(True)
            error_counter = len(messages) - success_counter
            # acknowledge messages
            # TODO: implement batch ack
            for message in messages:
                message.ack()
            t = (time.time() - t0) * 1000.0
            if success_counter > 0 or error_counter > 0:
                print(f"processed success={success_counter}, error={error_counter} in {t:.2f} milliseconds", flush=True)


class RawEventProcessor(BaseProcessor):
    def __init__(self, config: BaseConfig):
        super().__init__(config, queue_type="raw")

    def process_raw(self, raw_events: List[CollectorPayload]) -> List[bool]:
        return self.process(raw_events)
    
    def process(self, raw_events: List[CollectorPayload]) -> List[bool]:
        raise NotImplementedError("please implement process method")


class AtomicEventProcessor(BaseProcessor):
    def __init__(self, config: BaseConfig):
        super().__init__(config, queue_type="events")

    def process_events(self, events: List[AtomicEvent]) -> List[bool]:
        return self.process(events)
    
    def process(self, events: List[AtomicEvent]) -> List[bool]:
        raise NotImplementedError("please implement process method")


class ErrorEventProcessor(BaseProcessor):
    def __init__(self, config: BaseConfig):
        super().__init__(config, queue_type="errors")

    def process_errors(self, errors: List[Union[ErrorPayload, bytes]]) -> bool:
        return self.process(errors)

    def process(self, error: Union[ErrorPayload, bytes]) -> bool:
        raise NotImplementedError("please implement process method")