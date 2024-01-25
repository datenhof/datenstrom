from datenstrom.settings import get_settings

from datenstrom.common.schema.atomic import AtomicEvent
from datenstrom.processing.processor import AtomicEventProcessor


class MyProcessor(AtomicEventProcessor):
    def process(self, atomic: AtomicEvent) -> bool:
        print(atomic)
        return True


if __name__ == "__main__":
    config = get_settings()
    enricher = MyProcessor(config=config)
    enricher.run()
