from typing import List

from datenstrom.settings import get_settings

from datenstrom.common.schema.atomic import AtomicEvent
from datenstrom.common.schema.raw import ErrorPayload
from datenstrom.processing.processor import AtomicEventProcessor, ErrorEventProcessor


class MyAtomicConsumer(AtomicEventProcessor):
    def process(self, events: List[AtomicEvent]) -> List[bool]:
        results = []
        for ev in events:
            print(ev)
            results.append(True)
        return results


class MyErrorConsumer(ErrorEventProcessor):
    def process(self, errors: List[ErrorPayload]) -> List[bool]:
        results = []
        for e in errors:
            print(e)
            results.append(True)
        return results


if __name__ == "__main__":
    config = get_settings()
    enricher = MyAtomicConsumer(config=config)
    enricher.run()
