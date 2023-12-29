from typing import List
from abc import ABC, abstractmethod

class Sink(ABC):
    """The sink class."""

    def __init__(self, config):
        """Initialize."""
        self.config = config

    @abstractmethod
    def write(self, data: List[bytes]):
        """Write data to the sink."""
        pass

    @abstractmethod
    def close(self):
        """Close the sink."""
        pass