import time

from typing import List, Literal, Any, Optional

from datenstrom.common.schema.raw import CollectorPayload
from datenstrom.common.schema.atomic import AtomicEvent
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
        else:
            raise ValueError(f"Cannot use source {transport} as processor source.")

        if queue_type == "raw":
            self._decoder = self._decode_raw_message
            self._processor = self.process_raw
        elif queue_type == "events":
            self._decoder = self._decode_event_message
            self._processor = self.process_event

    def _decode_raw_message(self, message: bytes) -> Optional[CollectorPayload]:
        try:
            if self.config.record_format == "avro":
                payload = CollectorPayload.from_avro(message)
            elif self.config.record_format == "thrift":
                payload = CollectorPayload.from_thrift(message)
        except ValueError as e:
            print(f"invalid message: {e}")
            # self.error_sink.write([message])
            return None
        return payload

    def _decode_event_message(self, message: bytes) -> Optional[AtomicEvent]:
        ev = AtomicEvent.model_validate_json(message)
        print(ev)
        return ev

    def process_event(self, event: AtomicEvent) -> bool:
        raise NotImplementedError("process_event not implemented")
    
    def process_raw(self, raw: CollectorPayload) -> bool:
        raise NotImplementedError("process_raw not implemented")

    def run(self):
        signal_handler = SignalHandler()
        while not signal_handler.received_signal:
            messages = self.source.read()
            t0 = time.time()
            success_counter = 0
            error_counter = 0
            for message in messages:
                decoded_message = self._decoder(message.data())
                if not decoded_message:
                    continue
                result = self._processor(decoded_message)
                if result:
                    success_counter += 1
                else:
                    error_counter += 1
                message.ack()
            t = (time.time() - t0) * 1000.0
            if success_counter > 0 or error_counter > 0:
                print(f"processed success={success_counter}, error={error_counter} in {t:.2f} milliseconds", flush=True)


class RawEventProcessor(BaseProcessor):
    def __init__(self, config: BaseConfig):
        super().__init__(config, queue_type="raw")

    def process_raw(self, raw: CollectorPayload) -> bool:
        return self.process(raw)
    
    def process(self, raw: CollectorPayload) -> bool:
        raise NotImplementedError("please implement process method")


class AtomicEventProcessor(BaseProcessor):
    def __init__(self, config: BaseConfig):
        super().__init__(config, queue_type="events")

    def process_event(self, event: AtomicEvent) -> bool:
        return self.process(event)
    
    def process(self, raw: AtomicEvent) -> bool:
        raise NotImplementedError("please implement process method")


# class ErrorEventProcessor(BaseProcessor):
#     def __init__(self, config: BaseConfig):
#         super().__init__(config, queue_type="error")

#     def process_raw(self, raw: CollectorPayload) -> bool:
#         return self.process(raw)
    
#     def process(self, raw: CollectorPayload) -> bool:
#         raise NotImplementedError("please implement process method")