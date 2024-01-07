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


class FirehoseSink(Sink):
    """Firehose sink class."""

    def __init__(self, config: Any, queue_type: str):
        """Initialize."""
        super().__init__(config, queue_type=queue_type)
        if not config.firehose_stream_name:
            raise ValueError("Missing firehose_stream_name config")
        self.stream_name= config.firehose_stream_name

        with boto3_client_lock:
            self.firehose = boto3.client("firehose")

        self.counter = dict(ok=0, err=0, last_reset=datetime.now(timezone.utc))
        self._cancelled = False
        self._executor = ThreadPoolExecutor(max_workers=10)

    def count_ok(self):
        self.counter["ok"] += 1
        # check if counter needs to be reset
        now = datetime.now(timezone.utc)
        if now - self.counter["last_reset"] > COUNTER_RESET_INTERVAL:
            print(f"[Firehose Sink]: {self.counter}")
            # print counter
            self.counter["last_reset"] = now
            self.counter["ok"] = 0
            self.counter["err"] = 0

    def count_err(self):
        self.counter["err"] += 1
        # check if we have to many errors
        if self.counter["err"] > MAX_ERRORS_PER_INTERVAL:
            print(f"[Firehose Sink]: too many errors, crashing")
            os.kill(os.getpid(), signal.SIGINT)

    def _send(self, message: bytes) -> str:
        resp = self.firehose.put_record(
            DeliveryStreamName=self.stream_name,
            Record={
                "Data": message
            }
        )
        return resp["RecordId"]

    def write(self, data: List[bytes]):
        """Write data to the development sink."""
        for d in data:
            result_future = self._executor.submit(self._send, d)
            result_future.add_done_callback(self.on_result)

    def on_result(self, future):
        try:
            result = future.result()
        except Exception as exc:
            print(f"[Firehose Sink] Error: {exc}")
            self.count_err()
        else:
            self.count_ok()

    def close(self):
        """Close the sink."""
        self._executor.shutdown(wait=True)
