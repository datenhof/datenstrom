from uuid import uuid4
from datetime import datetime, timezone

from datenstrom.processing.enrichments.base import BaseEnrichment, TemporaryAtomicEvent


class PostProcessingEnrichment(BaseEnrichment):
    """Post processing enrichment."""

    def enrich(self, event: TemporaryAtomicEvent) -> None:
        """Enrich data."""
        # add the event id if we dont have one yet
        if "event_id" not in event.atomic:
            event.set_value("event_id", str(uuid4()))
        
        # fix the timestamps
        if "true_tstamp" in event:
            event.set_value("tstamp", event["true_tstamp"])
        elif "dvce_created_tstamp" in event and "dvce_sent_tstamp" in event:
            # calculate offset and apply to collector_tstamp
            delta = event["dvce_sent_tstamp"] - event["dvce_created_tstamp"]
            new_tstamp = event["collector_tstamp"] - delta
            event.set_value("tstamp", new_tstamp)
        else:
            event.set_value("tstamp", event["collector_tstamp"])

        # set etl timestamp
        event.set_value("etl_tstamp", datetime.now(timezone.utc))

        # set platform to web if not set
        if "platform" not in event:
            event.set_value("platform", "web")
