from typing import List

from datenstrom.settings import get_settings

from datenstrom.common.schema.atomic import AtomicEvent
from datenstrom.processing.processor import AtomicEventProcessor


class MyProcessor(AtomicEventProcessor):
    def process(self, events: List[AtomicEvent]) -> List[bool]:
        results = []
        for ev in events:
            print(ev)
            results.append(True)
        return results


if __name__ == "__main__":
    config = get_settings()
    enricher = MyProcessor(config=config)
    enricher.run()
