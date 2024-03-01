import base64
import boto3

from typing import List, Any

from datenstrom.connectors.sources.base import Source, Message


class SQSMessage(Message):
    def __init__(self, message, queue_type: str):
        self.message = message
        self.queue_type = queue_type

    def data(self):
        if self.queue_type == "raw":
            return base64.b64decode(self.message.body)
        return self.message.body.encode("utf-8")

    def ack(self):
        self.message.delete()


class SQSSource(Source):
    def __init__(self, config: Any, queue_type: str):
        """Initialize."""
        super().__init__(config, queue_type=queue_type)
        self.sqs = boto3.resource("sqs")

        if self.queue_type == "raw":
            self.queue = self.sqs.Queue(config.sqs_queue_raw)
        elif self.queue_type == "events":
            self.queue = self.sqs.Queue(config.sqs_queue_events)
        elif self.queue_type == "errors":
            self.queue = self.sqs.Queue(config.sqs_queue_errors)
        else:
            raise ValueError(f"Unknown queue type {queue_type} for source.")

    def read(self) -> List[Message]:
        messages = self.queue.receive_messages(
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
        )
        return [SQSMessage(message, queue_type=self.queue_type) for message in messages]
