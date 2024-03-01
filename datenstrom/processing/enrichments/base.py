from abc import ABC, abstractmethod
from typing import Any, Optional, Dict, List, NamedTuple

from pydantic import ValidationError

from datenstrom.common.schema.raw import CollectorPayload
from datenstrom.common.schema.atomic import AtomicEvent, SelfDescribingContext, SelfDescribingEvent


class TemporaryAtomicEvent():
    MODEL_FIELDS = set(AtomicEvent.model_fields.keys())

    def __init__(self, raw_event: CollectorPayload,
                 initial_data: Optional[Dict] = None) -> None:
        self.raw_event = raw_event
        self.temp_data = initial_data or {}
        self.atomic = {}

    def __setitem__(self, key: str, value: Any) -> None:
        self.temp_data[key] = value

    def __getitem__(self, key: str) -> Any:
        return self.temp_data[key]

    def __contains__(self, key: str) -> bool:
        return key in self.temp_data

    def __delitem__(self, key: str) -> None:
        del self.temp_data[key]

    def keys(self):
        return self.temp_data.keys()

    def set_value(self, key: str, value: Any) -> None:
        if key in ["contexts", "event"]:
            raise KeyError(f"Field {key} is not allowed to be set directly")
        if key not in self.MODEL_FIELDS:
            raise KeyError(f"Field {key} is not part of the atomic event")
        self.temp_data[key] = value
        self.atomic[key] = value

    def add_context(self, context: SelfDescribingContext) -> None:
        if "contexts" not in self.atomic:
            self.atomic["contexts"] = {}
        if context.schema_name in self.atomic["contexts"]:
            raise ValueError(f"Context {context.schema_name} already exists")
        self.atomic["contexts"][context.schema_name] = context

    def has_context(self, schema_name: str) -> bool:
        if "contexts" not in self.atomic:
            return False
        for c in self.atomic["contexts"]:
            if c.schema_name == schema_name:
                return True
        return False

    def set_event(self, event: SelfDescribingEvent) -> None:
        if "event" in self.atomic:
            raise ValueError("Event data already set")
        self.atomic["event"] = event

    def has_event(self) -> bool:
        return "event" in self.atomic and self.atomic["event"] is not None

    def to_atomic_event(self) -> "AtomicEvent":
        # for k, v in self.temp_data.items():
        #     if k in self.MODEL_FIELDS:
        #         self.atomic[k] = v
        try:
            return AtomicEvent(**self.atomic)
        except ValidationError as e:
            errors = e.errors()
            no_errors = len(errors)
            fields = fields = [" ".join(err["loc"]) for err in e.errors()]
            raise ValueError(f"Invalid atomic event: {no_errors} errors in {fields}")

    def get_event(self) -> Optional[SelfDescribingEvent]:
        return self.atomic.get("event")
    
    def get_contexts(self) -> List[SelfDescribingContext]:
        context_dict: Dict[str, SelfDescribingContext] = self.atomic.get("contexts")
        if not context_dict:
            return list()
        return list(context_dict.values())


class BaseEnrichment(ABC):
    """The base enrichment class."""
    def __init__(self, config: Any) -> None:
        self.config = config

    @abstractmethod
    def enrich(self, event: TemporaryAtomicEvent) -> None:
        """Enrich data."""
        pass


class RemoteEnrichmentConfig(NamedTuple):
    enable_full_ip: bool