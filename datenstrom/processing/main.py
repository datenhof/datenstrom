from datenstrom.settings import get_settings
from datenstrom.processing.enrich import Enricher


if __name__ == "__main__":
    config = get_settings()
    enricher = Enricher(config=config)
    enricher.run()
