from typing import List, Any
from abc import ABC, abstractmethod

class Sink(ABC):
    """The sink class."""

    def __init__(self, config: Any, queue_type: str):
        """Initialize."""
        self.config = config
        self.queue_type = self.check_queue_type(queue_type)

    def check_queue_type(self, queue_type: str) -> str:
        if queue_type in ("raw", "events", "errors"):
            return queue_type
        raise ValueError(f"Unknown queue type {queue_type} for sink.")

    @abstractmethod
    def write(self, data: List[bytes]) -> int:
        """Write data to the sink."""
        pass

    @abstractmethod
    def close(self):
        """Close the sink."""
        pass