from typing import List,Any
from datenstrom.connectors.sinks.base import Sink


class DevSink(Sink):
    """The development sink class."""
    def __init__(self, config: Any, queue_type: str):
        """Initialize."""
        super().__init__(config, queue_type=queue_type)
        self.last_record = None

    def write(self, data: List[bytes]) -> int:
        """Write data to std out."""
        size = 0
        for d in data:
            self.last_record = d
            print(d)
            size += len(d)
        return size

    def close(self):
        """Close the sink."""
        pass