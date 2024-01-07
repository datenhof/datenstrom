# This transformation is created with the information from
# https://github.com/snowplow/enrich/blob/master/modules/common/src/main/scala/com.snowplowanalytics.snowplow.enrich/common/enrichments/Transform.scala
# to ensure we are compatible with snowplow trackers

from datetime import datetime, timezone
from typing import Dict, Any


from datenstrom.processing.enrichments.base import BaseEnrichment, TemporaryAtomicEvent


def transform_ip(ip: str) -> str:
    """Transform IP address."""
    if "," in ip:
        # print warning
        print("Multiple IPs found")
        print(ip)
        ip = ip.split(",")[0]
        ip = ip.replace("[", "").replace("]", "").replace(",", "")
    # replace all [ and ] and all ,
    return ip


def transform_string(value: str) -> str:
    """Default transform."""
    return value


def transform_int(value: str) -> int:
    """Transform int."""
    return int(value)


def transform_float(value: str) -> float:
    """Transform int."""
    return float(value)


def transform_tstamp(value: str) -> datetime:
    """Transform timestamp."""
    # first cast to int
    value = int(value)
    # this is a unix timestamp in milliseconds
    # convert it to an iso datetime string
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)


def transform_boolean(value: str) -> bool:
    """Transform boolean."""
    return value == "1"


def run_transformations(event: TemporaryAtomicEvent,
                        transformations_dict: Dict[str, Any]) -> None:
    """Run transformations."""
    for key, (transform, new_key) in transformations_dict.items():
        if key in event:
            value = event[key]
            # we skip None values
            if value is not None:
                event[new_key] = transform(value)
    return event


TRANSFORMATIONS = {
    # Metadata
    "eid": (transform_string, "event_id"),

    # Application Fields
    "aid": (transform_string, "identifier"),
    "p": (transform_string, "platform"),

    # Date and Time Fields
    "dtm": (transform_tstamp, "dvce_created_tstamp"),
    "ttm": (transform_tstamp, "true_tstamp"),
    "stm": (transform_tstamp, "dvce_sent_tstamp"),

    # Versioning
    "tv": (transform_string, "v_tracker"),
    "cv": (transform_string, "v_collector"),
    "tna": (transform_string, "name_tracker"),

    # User
    "ip": (transform_ip, "user_ipaddress"),
    "uid": (transform_string, "user_id"),
    "duid": (transform_string, "domain_userid"),
    "vid": (transform_int, "domain_sessionidx"),
    "sid": (transform_string, "domain_sessionid"),
    "nuid": (transform_string, "network_userid"),

    # Common
    "ua": (transform_string, "useragent"),
    "lang": (transform_string, "language"),
}


PAGE_VIEW_TRANSFORMATIONS = {
    # Page
    "refr": (transform_string, "page_referrer"),
    "url": (transform_string, "page_url"),
    "page": (transform_string, "page_title"),
}


PAGE_PING_TRANSFORMATIONS = {
    # PagePing
    "pp_mix": (transform_int, "pp_xoffset_min"),
    "pp_max": (transform_int, "pp_xoffset_max"),
    "pp_miy": (transform_int, "pp_yoffset_min"),
    "pp_may": (transform_int, "pp_yoffset_max"),
}


STRUCTURED_EVENT_SCHEMA = {
    "se_category": (transform_string, "category"),
    "se_action": (transform_string, "action"),
    "se_label": (transform_string, "label"),
    "se_property": (transform_string, "property"),
    "se_value": (transform_string, "value"),
}


TRANSACTION_TRANSFORMATIONS = {
    # Transaction
    "tid": (transform_string, "txn_id"),
    "tr_id": (transform_string, "tr_orderid"),
    "tr_af": (transform_string, "tr_affiliation"),
    "tr_tt": (transform_float, "tr_total"),
    "tr_tx": (transform_float, "tr_tax"),
    "tr_sh": (transform_float, "tr_shipping"),
    "tr_ci": (transform_string, "tr_city"),
    "tr_st": (transform_string, "tr_state"),
    "tr_co": (transform_string, "tr_country"),
    "tr_cu": (transform_string, "tr_currency"),
}


TRANSACTION_ITEM_TRANSFORMATIONS = {
    # Transaction Item
    "ti_id": (transform_string, "ti_orderid"),
    "ti_sk": (transform_string, "ti_sku"),
    "ti_na": (transform_string, "ti_name"),
    "ti_nm": (transform_string, "ti_name"),
    "ti_ca": (transform_string, "ti_category"),
    "ti_pr": (transform_float, "ti_price"),
    "ti_qu": (transform_int, "ti_quantity"),
    "ti_cu": (transform_string, "ti_currency"),
}


class TransformEnrichment(BaseEnrichment):
    """Transform enrichment."""
    def enrich(self, event: TemporaryAtomicEvent) -> TemporaryAtomicEvent:
        for key in list(event.keys()):
            # we skip keys that are not in the transformation dict
            if key in TRANSFORMATIONS:
                transform, new_key = TRANSFORMATIONS[key]
                # we skip None values
                value = event[key]
                if value is not None:
                    event.set_value(new_key, transform(value))
