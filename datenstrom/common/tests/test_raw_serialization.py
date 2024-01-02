import os
import json
import base64
from datenstrom.common.schema.raw import CollectorPayload, to_thrift, to_avro, from_thrift


def load_data():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(current_dir, "raw_data.thrift.json")) as f:
        test_data = json.load(f)
    assert test_data["magic"] == "magic"
    return {
        "event1": base64.b64decode(test_data["event1"]),
        "webevent": base64.b64decode(test_data["webevent"]),
        "webevent_get": base64.b64decode(test_data["webevent_get"]),
        "iglu_event": base64.b64decode(test_data["iglu_event"]),
    }


def test_to_thrift():
    p = CollectorPayload(
        schema="iglu:com.acme/user/jsonschema/1-0-0",
        ipAddress="sdsd",
        timestamp=123,
        encoding="sdsd",
        collector="sdsd",
    )
    t = to_thrift(p)
    assert t == (b'\x0bzi\x00\x00\x00#iglu:com.acme/user/jsonschema/1-0-0'
                 b'\x0b\x00d\x00\x00\x00\x04sdsd\n\x00\xc8\x00\x00\x00\x00'
                 b'\x00\x00\x00{\x0b\x00\xd2\x00\x00\x00\x04sdsd\x0b\x00'
                 b'\xdc\x00\x00\x00\x04sdsd\x00')
    # compact protocol
    # assert t == (b'\x08\xd2\xe9\x03#iglu:com.acme/user/jsonschema/1-0-0'
    #              b'\x08\xc8\x01\x04sdsd\x06\x90\x03\xf6\x01\xa8\x04sdsd'
    #              b'\xa8\x04sdsd\x00')
    back = from_thrift(t)
    assert back == p

def test_thrift():
    test_data = load_data()
    p = from_thrift(test_data["event1"])
    d = p.model_dump(by_alias=True)

    assert d["schema"] == "iglu:com.snowplowanalytics.snowplow/CollectorPayload/thrift/1-0-0"
    # back to thrift
    t1 = to_thrift(p)
    t2 = to_thrift(from_thrift(t1))
    assert t1 == t2
    # assert t1 == test_data["event1"]
