from typing import List, Any
from abc import ABC, abstractmethod


class Message(ABC):
    @abstractmethod
    def data(self) -> bytes:
        pass

    @abstractmethod
    def ack(self):
        pass


class Source(ABC):
    """The source class."""

    def __init__(self, config: Any, queue_type: str):
        """Initialize."""
        self.config = config
        self.queue_type = self.check_queue_type(queue_type)

    def check_queue_type(self, queue_type: str) -> str:
        if queue_type in ("raw", "events"):
            return queue_type
        raise ValueError(f"Unknown queue type {queue_type} for source.")

    @abstractmethod
    def read(self) -> List[Message]:
        """Read data from the source."""
        pass