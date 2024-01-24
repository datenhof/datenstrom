import os
import signal
import base64

from typing import List, Any
from datetime import datetime, timezone, timedelta
import boto3
import threading
from concurrent.futures import ThreadPoolExecutor

from datenstrom.connectors.sinks.base import Sink


COUNTER_RESET_INTERVAL = timedelta(seconds=60)
MAX_ERRORS_PER_INTERVAL = 10


boto3_client_lock = threading.Lock()


class SQSSink(Sink):
    """SQS sink class."""

    def __init__(self, config: Any, queue_type: str):
        """Initialize."""
        super().__init__(config, queue_type=queue_type)

        if queue_type == "raw":
            if not config.sqs_queue_raw:
                raise ValueError("Missing sqs_queue_url_raw config")
            self.queue_name= config.sqs_queue_raw
        elif queue_type == "events":
            if not config.sqs_queue_events:
                raise ValueError("Missing sqs_queue_url_events config")
            self.queue_name = config.sqs_queue_events
        elif queue_type == "errors":
            if not config.sqs_queue_errors:
                raise ValueError("Missing sqs_queue_url_errors config")
            self.queue_name = config.sqs_queue_errors

        with boto3_client_lock:
            self.sqs = boto3.client("sqs")
            queue_url_result = self.sqs.get_queue_url(QueueName=self.queue_name)
            self.queue_url = queue_url_result["QueueUrl"]

        self.counter = dict(ok=0, err=0, last_reset=datetime.now(timezone.utc))
        self._cancelled = False
        # self._producer = Producer({
        #     'bootstrap.servers': self.bootstrap_servers
        # })
        # self._thread = Thread(target=self._run)
        self._executor = ThreadPoolExecutor(max_workers=10)

    # def _run(self):
    #     while not self._cancelled:
    #         self._producer.poll(0.1)

    def count_ok(self):
        self.counter["ok"] += 1
        # check if counter needs to be reset
        now = datetime.now(timezone.utc)
        if now - self.counter["last_reset"] > COUNTER_RESET_INTERVAL:
            print(f"[SQS Sink]: {self.counter}")
            # print counter
            self.counter["last_reset"] = now
            self.counter["ok"] = 0
            self.counter["err"] = 0

    def count_err(self):
        self.counter["err"] += 1
        # check if we have to many errors
        if self.counter["err"] > MAX_ERRORS_PER_INTERVAL:
            print(f"[SQS Sink]: too many errors, crashing")
            os.kill(os.getpid(), signal.SIGINT)

    def _send(self, message: bytes) -> str:
        if self.queue_type == "raw":
            body = base64.b64encode(message).decode("utf-8")
        else:
            body = message.decode("utf-8")

        resp = self.sqs.send_message(
            QueueUrl=self.queue_url,
            MessageBody=body,
        )
        return resp["MessageId"]

    def write(self, data: List[bytes]):
        """Write data to the development sink."""
        for d in data:
            result_future = self._executor.submit(self._send, d)
            result_future.add_done_callback(self.on_result)

    def on_result(self, future):
        try:
            result = future.result()
        except Exception as exc:
            print(f"[SQS Sink] Error: {exc}")
            self.count_err()
        else:
            self.count_ok()

    def close(self):
        """Close the sink."""
        self._executor.shutdown(wait=True)
