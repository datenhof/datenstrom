import os
import signal

from typing import List, Any
from datetime import datetime, timezone, timedelta
from confluent_kafka import Producer
from threading import Thread

from datenstrom.connectors.sinks.base import Sink


COUNTER_RESET_INTERVAL = timedelta(seconds=60)
MAX_ERRORS_PER_INTERVAL = 10


class KafkaSink(Sink):
    """Kafka sink class."""

    def __init__(self, config: Any, queue_type: str):
        """Initialize."""
        super().__init__(config, queue_type=queue_type)
        if not config.kafka_brokers:
            raise ValueError("Missing Kafka brokers")
        self.bootstrap_servers = config.kafka_brokers

        if queue_type == "raw":
            if not config.kafka_topic_raw:
                raise ValueError("Missing kafka_topic_raw config")
            self.topic= config.kafka_topic_raw
        elif queue_type == "events":
            if not config.kafka_topic_events:
                raise ValueError("Missing kafka_topic_events config")
            self.topic = config.kafka_topic_events
        elif queue_type == "errors":
            if not config.kafka_topic_errors:
                raise ValueError("Missing kafka_topic_errors config")
            self.topic = config.kafka_topic_errors

        self.counter = dict(ok=0, err=0, last_reset=datetime.now(timezone.utc))
        self._cancelled = False
        self._producer = Producer({
            'bootstrap.servers': self.bootstrap_servers
        })
        self._thread = Thread(target=self._run)

    def _run(self):
        while not self._cancelled:
            self._producer.poll(0.1)

    def count_ok(self):
        self.counter["ok"] += 1
        print(f"KafkaSink: {self.counter}")
        # check if counter needs to be reset
        now = datetime.now(timezone.utc)
        if now - self.counter["last_reset"] > COUNTER_RESET_INTERVAL:
            # print counter
            self.counter["last_reset"] = now
            self.counter["ok"] = 0
            self.counter["err"] = 0

    def count_err(self):
        self.counter["err"] += 1
        # check if we have to many errors
        if self.counter["err"] > MAX_ERRORS_PER_INTERVAL:
            print(f"KafkaSink: too many errors, crashing")
            os.kill(os.getpid(), signal.SIGINT)

    def write(self, data: List[bytes]):
        """Write data to the development sink."""
        for d in data:
            self._producer.produce(self.topic, d, callback=self.ack)

    def ack(self, err, msg):
        if err:
            print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
            self.count_err()
        else:
            self.count_ok()

    def close(self):
        """Close the sink."""
        self._cancelled = True
        self._thread.join()
