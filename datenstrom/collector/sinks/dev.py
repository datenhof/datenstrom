from typing import List
from datenstrom.collector.sinks.base import Sink


class DevSink(Sink):
    """The development sink class."""

    def write(self, data: List[bytes]):
        """Write data to std out."""
        for d in data:
            print(d)

    def close(self):
        """Close the sink."""
        pass