import os
import json
import base64
import pytest
import copy
import urllib.parse

from datenstrom.processing.raw_processor import RawProcessor
from datenstrom.common.schema.raw import from_avro
from datenstrom.settings import get_test_settings
from datenstrom.processing.enrichments.base import TemporaryAtomicEvent
from datenstrom.processing.enricher import Enricher

test_config = get_test_settings()


def load_data():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(current_dir, "raw_data.avro.json")) as f:
        test_data = json.load(f)
    assert test_data["magic"] == "magic"
    data_d = {}
    for t in ["event1", "webevent", "webevent_get", "iglu_event"]:
        temp = base64.b64decode(test_data[t])
        data_d[t] = from_avro(temp)
    data_d["webevent_raw"] = base64.b64decode(test_data["webevent_get"])
    return data_d


def test_raw_get():
    d = load_data()
    raw_event = d["webevent_get"]
    p = RawProcessor(test_config)
    out = p.process_raw_event(raw_event)
    assert len(out) == 1
    get_event = out[0].model_dump(by_alias=True)
    assert get_event["event_id"] == '23cb931d-2853-4cd2-841e-a428fba922f2'
    assert get_event["platform"] == 'web'
    assert get_event["event_name"] == 'page_view'
    assert len(get_event["contexts"]) == 2
    assert 'iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0' in get_event["contexts"]
    assert get_event["event"]["schema"] == 'iglu:io.datenstrom/page_view/jsonschema/1-0-0'
    assert get_event["event"]["data"]["page_url"] == 'http://127.0.0.1:8000/?test=ok&hello=world'


def test_raw_event1():
    d = load_data()
    raw_event = d["event1"]
    p = RawProcessor(test_config)
    out = p.process_raw_event(raw_event)
    assert len(out) == 2

    event1 = out[0].model_dump(by_alias=True)
    assert event1["event_id"] == 'a43c4229-9ef3-49fe-9412-3c8dc55f5581'
    assert event1["platform"] == 'pc'
    assert event1["event_name"] == 'page_view'
    assert len(event1["contexts"]) == 1
    assert event1["event"]["schema"] == 'iglu:io.datenstrom/page_view/jsonschema/1-0-0'
    assert event1["event"]["data"]["page_url"] == 'http://www.example.com'

    event2 = out[1].model_dump(by_alias=True)
    assert event2["event_id"] == '792f30b1-7066-429a-86e0-bc779e01843f'
    assert event2["platform"] == 'pc'
    assert event2["event_name"] == 'link_click'
    assert len(event2["contexts"]) == 1
    assert event2["event"]["schema"] == 'iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1'
    assert event2["event"]["data"]["targetUrl"] == 'https://www.snowplow.io'


def test_raw_post():
    d = load_data()
    raw_event = d["webevent"]
    p = RawProcessor(test_config)
    out = p.process_raw_event(raw_event)
    assert len(out) == 1
    post_event = out[0].model_dump(by_alias=True)
    assert post_event["event_id"] == '08e6e671-3d88-4d91-be42-fc01241824a0'
    assert post_event["platform"] == 'web'
    assert post_event["user_ipaddress"] == '172.17.0.x'
    assert post_event["event_name"] == 'page_view'
    assert len(post_event["contexts"]) == 2
    assert 'iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0' in post_event["contexts"]
    assert post_event["event"]["schema"] == 'iglu:io.datenstrom/page_view/jsonschema/1-0-0'
    assert post_event["event"]["data"]["page_url"] == 'http://127.0.0.1:8000/?test=ok&hello=world'


def test_raw_iglu():
    d = load_data()
    raw_event = d["iglu_event"]
    p = RawProcessor(test_config)
    out = p.process_raw_event(raw_event)
    assert len(out) == 1
    iglu_event = out[0].model_dump()
    assert len(iglu_event["event_id"]) == 36
    assert iglu_event["platform"] == 'mob'
    assert iglu_event["event_name"] == 'social_interaction'
    assert len(iglu_event["contexts"]) == 1
    assert iglu_event["event"]["schema"] == 'iglu:com.snowplowanalytics.snowplow/social_interaction/jsonschema/1-0-0'
    assert iglu_event["event"]["data"]["action"] == 'retweet'
    assert iglu_event["event"]["data"]["network"] == 'twitter'


def test_enricher_invalid_event():
    d = load_data()
    raw_event = d["webevent_raw"]
    enricher = Enricher(test_config)

    # valid event
    payload = enricher._decode_raw_message(raw_event)
    ev = enricher.process_raw([payload])[0]
    assert ev is True

    # invalid event
    payload2 = enricher._decode_raw_message(raw_event)
    payload2.querystring = "e=pv"
    ev = enricher.process_raw([payload2])[0]
    assert ev is False


def test_invalid_temporary_atomic_event():
    d = load_data()
    raw_event = d["webevent_get"]
    ta = TemporaryAtomicEvent(raw_event)
    with pytest.raises(ValueError):
        ta.to_atomic_event()


def test_campaign_enrichment():
    d = load_data()
    raw_event = copy.copy(d["webevent_get"])
    page_url = "http://example.com/?test=ok&utm_campaign=summer&utm_medium=email&utm_source=newsletter&gclid=abc"
    encoded_url = urllib.parse.quote(page_url)
    print(encoded_url)
    raw_event.querystring = f"e=pv&p=web&url={encoded_url}"
    p = RawProcessor(test_config)
    out = p.process_raw_event(raw_event)
    assert len(out) == 1
    get_event = out[0].model_dump(by_alias=True)
    assert get_event["platform"] == 'web'
    assert get_event["event_name"] == 'page_view'
    assert get_event["event"]["schema"] == 'iglu:io.datenstrom/page_view/jsonschema/1-0-0'
    assert get_event["event"]["data"]["page_url"] == page_url
    assert len(get_event["contexts"]) == 2

    contexts = get_event["contexts"]
    attribution = contexts['iglu:io.datenstrom/campaign_attribution/jsonschema/1-0-0']
    device_info = contexts['iglu:io.datenstrom/device_info/jsonschema/1-0-0']

    assert attribution["schema"] == 'iglu:io.datenstrom/campaign_attribution/jsonschema/1-0-0'
    assert attribution["data"]["campaign"] == 'summer'
    assert attribution["data"]["medium"] == 'email'
    assert attribution["data"]["source"] == 'newsletter'
    assert attribution["data"]["network"] == 'google'
    assert attribution["data"]["click_id"] == 'abc'

    print(device_info)
    assert device_info["schema"] == 'iglu:io.datenstrom/device_info/jsonschema/1-0-0'
    assert device_info["data"]["browser_family"] == 'Chrome'
    assert device_info["data"]["os_family"] == 'Windows'
