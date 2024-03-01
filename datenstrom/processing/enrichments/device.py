from datenstrom.processing.enrichments.base import BaseEnrichment, TemporaryAtomicEvent
from datenstrom.common.schema.atomic import SelfDescribingContext
from ua_parser import user_agent_parser

SCREEN_RESOLUTION_FIELD = "res"
VIEWPORT_RESOLUTION_FIELD = "vp"
COLOR_DEPTH_FIELD = "cd"
TIMEZONE_FIELD = "tz"
USERAGENT_FIELD = "ua"


class DeviceEnrichment(BaseEnrichment):
    """Device and user agent enrichment."""

    def enrich(self, event: TemporaryAtomicEvent) -> None:
        data_dict = {}

        if SCREEN_RESOLUTION_FIELD in event:
            data_dict["screen_resolution"] = event[SCREEN_RESOLUTION_FIELD]
        if VIEWPORT_RESOLUTION_FIELD in event:
            data_dict["viewport_resolution"] = event[VIEWPORT_RESOLUTION_FIELD]
        if USERAGENT_FIELD in event:
            user_agent = user_agent_parser.Parse(event[USERAGENT_FIELD])
            browser_family = user_agent["user_agent"].get("family")
            os_family = user_agent["os"].get("family")
            device_family = user_agent["device"].get("family")

            # check if at least one of the families is not "Other"
            if ((browser_family and browser_family != "Other") or
                (os_family and os_family != "Other") or
                (device_family and device_family != "Other")):
                data_dict["browser_family"] = browser_family
                data_dict["browser_version"] = user_agent["user_agent"].get("major")
                data_dict["os_family"] = os_family
                data_dict["os_version"] = user_agent["os"].get("major")
                data_dict["device_family"] = device_family

        if len(data_dict) > 0:
            schema = "iglu:io.datenstrom/device_info/jsonschema/1-0-0"
            event.add_context(SelfDescribingContext(schema=schema, data=data_dict))
