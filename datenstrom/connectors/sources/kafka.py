import base64

from typing import List, Any

from confluent_kafka import Consumer
from confluent_kafka import Message as ConfluentMessage
from datenstrom.connectors.sources.base import Source, Message


class KafkaMessage(Message):
    def __init__(self, message: ConfluentMessage, queue_type: str):
        self.message = message
        self.queue_type = queue_type
        self.is_acknowledged = False

    def data(self):
        return self.message.value()

    def ack(self):
        self.is_acknowledged = True


class KafkaSource(Source):
    def __init__(self, config: Any, queue_type: str):
        """Initialize."""
        super().__init__(config, queue_type=queue_type)

        if self.queue_type == "raw":
            self.topic = config.kafka_topic_raw
        else:
            raise ValueError(f"Unknown queue type {queue_type} for source.")

        if not config.kafka_brokers:
            raise ValueError("Missing Kafka brokers")
        self.bootstrap_servers = config.kafka_brokers

        self.consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': f"datenstrom-{self.queue_type}",
            'auto.offset.reset': 'earliest',  # TODO: change to latest
            'enable.auto.commit': False,
        })
        self.consumer.subscribe([self.topic])
        self.last_batch = []


    def commit_message(self, message):
        self.consumer.commit(message, asynchronous=False)


    def read(self) -> List[Message]:
        # check if last batch is acknowledged
        if self.last_batch:
            for message in self.last_batch:
                if not message.is_acknowledged:
                    raise ValueError("Message in last batch is not acknowledged.")
            self.consumer.commit(asynchronous=False)
            self.last_batch = []

        messages = self.consumer.consume(num_messages=10, timeout=1)
        self.last_batch = [KafkaMessage(message, queue_type=self.queue_type) for message in messages]
        return self.last_batch
