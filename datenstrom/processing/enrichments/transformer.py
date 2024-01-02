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


TEMP_TRANSFORMATIONS = {
    # Browser Features
    # "f_pdf": (transform_boolean, "br_features_pdf"),
    # "f_fla": (transform_boolean, "br_features_flash"),
    # "f_java": (transform_boolean, "br_features_java"),
    # "f_dir": (transform_boolean, "br_features_director"),
    # "f_qt": (transform_boolean, "br_features_quicktime"),
    # "f_realp": (transform_boolean, "br_features_realplayer"),
    # "f_wma": (transform_boolean, "br_features_windowsmedia"),
    # "f_gears": (transform_boolean, "br_features_gears"),
    # "f_ag": (transform_boolean, "br_features_silverlight"),
    # "cookie": (transform_boolean, "br_cookies"),
    # "vp": (transform_string, "br_viewport"),

    # Device
    # "res": (transform_string, "dvce_screen"),
    # "cd": (transform_string, "br_colordepth"),
    # "tz": (transform_string, "os_timezone"),

    # Page
    # "refr": (transform_string, "page_referrer"),
    "url": (transform_string, "page_url"),
    # "page": (transform_string, "page_title"),

    # Doc
    # "ds": (transform_string, "doc_size"),
    # "cs": (transform_string, "doc_charset"),

    # PagePing
    # "pp_mix": (transform_int, "pp_xoffset_min"),
    # "pp_max": (transform_int, "pp_xoffset_max"),
    # "pp_miy": (transform_int, "pp_yoffset_min"),
    # "pp_may": (transform_int, "pp_yoffset_max"),

    # structured event
    # "se_ca": (transform_string, "se_category"),

    # transaction and transaction item

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
            elif key in TEMP_TRANSFORMATIONS:
                transform, new_key = TEMP_TRANSFORMATIONS[key]
                # we skip None values
                value = event[key]
                if value is not None:
                    event[new_key] = transform(value)
