from datenstrom.settings import config
from datenstrom.processing.enrich import Enricher


if __name__ == "__main__":
    enricher = Enricher(config=config)
    enricher.run()
