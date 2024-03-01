from urllib.parse import urlparse, parse_qs

from datenstrom.processing.enrichments.base import BaseEnrichment, TemporaryAtomicEvent
from datenstrom.common.schema.atomic import SelfDescribingContext


CAMPAIGN_FIELD = "utm_campaign"
SOURCE_FIELD = "utm_source"
MEDIUM_FIELD = "utm_medium"
TERM_FIELD = "utm_term"
CONTENT_FIELD = "utm_content"

CLICK_ID_MAP = {
    "gclid": "google",
    "msclkid": "bing",
    "fbclid": "facebook",
    "dclid": "doubleclick",
}


class CampaignEnrichment(BaseEnrichment):
    """Marketing Campaign enrichment."""

    def enrich(self, event: TemporaryAtomicEvent) -> None:
        if "page_url" not in event:
            return

        url_parts = urlparse(event["page_url"])
        if not url_parts.query:
            return

        query = parse_qs(url_parts.query)
        if not query:
            return

        context_params = {}
        if CAMPAIGN_FIELD in query:
            context_params["campaign"] = query[CAMPAIGN_FIELD][0]
        if SOURCE_FIELD in query:
            context_params["source"] = query[SOURCE_FIELD][0]
        if MEDIUM_FIELD in query:
            context_params["medium"] = query[MEDIUM_FIELD][0]
        if TERM_FIELD in query:
            context_params["term"] = query[TERM_FIELD][0]
        if CONTENT_FIELD in query:
            context_params["content"] = query[CONTENT_FIELD][0]

        for click_id, network in CLICK_ID_MAP.items():
            if click_id in query:
                context_params["network"] = network
                context_params["click_id"] = query[click_id][0]
                break

        if len(context_params) > 0:
            schema = "iglu:io.datenstrom/campaign_attribution/jsonschema/1-0-0"
            # validate the context
            # self.registry.validate(schema=schema, data=data)
            event.add_context(SelfDescribingContext(schema=schema, data=context_params))
