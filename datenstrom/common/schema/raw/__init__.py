from typing import Optional, List, Dict
from io import BytesIO
import orjson
import logging
from datetime import datetime, timezone


from thriftpy2.protocol.binary import TBinaryProtocol
# from thriftpy2.protocol import TCompactProtocol

from thriftpy2.thrift import TPayload, TType
from fastavro import parse_schema, schemaless_writer, schemaless_reader
from pydantic import BaseModel, Field


THRIFT_PROTO = TBinaryProtocol
#THRIFT_PROTO = TCompactProtocol
SNOWPLOW_COLLECTOR_PAYLOAD_SCHEMA = (
    "iglu:com.snowplowanalytics.snowplow/CollectorPayload/thrift/1-0-0"
)
AVRO_SCHEMA_NAME = "CollectorPayload"


logger = logging.getLogger(__name__)


class PayloadException(Exception):
    pass


class CollectorPayload(BaseModel):
    schema_name: str = Field(alias="schema", default=AVRO_SCHEMA_NAME)
    ipAddress: str
    timestamp: int
    encoding: str
    collector: str

    userAgent: Optional[str] = None
    refererUri: Optional[str] = None
    path: Optional[str] = None
    querystring: Optional[str] = None
    body: Optional[bytes] = None
    headers: Optional[List[str]] = None
    contentType: Optional[str] = None
    hostname: Optional[str] = None
    networkUserId: Optional[str] = None

    def split_and_serialize(self, format: str, max_size: int) -> List[bytes]:
        # try to serialize the full event
        serialized = self.serialize(format)
        if len(serialized) <= max_size:
            return [serialized]

        # try to serialize without the body
        temp_body = self.body
        self.body = None
        serialized = self.serialize(format)
        size_without_body = len(serialized)
        if size_without_body >= max_size:
            raise PayloadException(f"Event without body too large: {size_without_body} > {max_size}")

        # if we dont have a body, we are done
        if not temp_body:
            return [serialized]

        self.body = temp_body
        # parse and split the body
        parsed_body = orjson.loads(self.body)
        # check schema and data
        if "schema" not in parsed_body:
            raise PayloadException("Missing schema in body")
        if "data" not in parsed_body:
            raise PayloadException("Missing data in body")
        schema = parsed_body["schema"]
        try:
            data_list = list(parsed_body["data"])
        except TypeError:
            raise PayloadException("Data is not iterable")

        # if we have an empty list, we are done
        if len(data_list) == 0:
            return [serialized]

        # split the data
        logger.info(f"Splitting {len(data_list)} items")
        result = []
        current_data = []
        current_body = None

        while len(data_list) > 0:
            # check if we can add another item
            previous_body = current_body
            item = data_list[0]
            current_data.append(item)
            current_body = orjson.dumps({
                "schema": schema,
                "data": current_data
            })
            if len(current_body) + size_without_body > max_size:
                # we are over the limit
                # we need to serialize
                if not previous_body:
                    # we are to large to fit a single item
                    raise PayloadException(f"Splitted single item too large: {len(current_body) + size_without_body} > {max_size}")
                # otherwise, we serialize the previous body
                self.body = previous_body
                result.append(self.serialize(format))
                # and reset the current body
                current_data = []
                current_body = None
            else:
                # we can add another item
                data_list.pop(0)
        # serialize the last body
        if current_body:
            self.body = current_body
            result.append(self.serialize(format))

        # splitting done
        sizes = [len(x) for x in result]
        logger.info(f"Split into {len(result)} items with sizes {sizes}")
        return result

    def serialize(self, format: str) -> bytes:
        if format == "thrift":
            self.schema_name = SNOWPLOW_COLLECTOR_PAYLOAD_SCHEMA
            return to_thrift(self)
        elif format == "avro":
            self.schema_name = AVRO_SCHEMA_NAME
            return to_avro(self)
        else:
            raise ValueError(f"Unknown format {format}")

    def to_json(self):
        return self.model_dump_json(by_alias=True)

    def to_thrift(self):
        return to_thrift(self)

    def to_avro(self):
        return to_avro(self)

    def get_headers_dict(self) -> Dict[str, str]:
        if self.headers is None:
            return {}
        result = {}
        for h in self.headers:
            try:
                k, v = h.split(":", 1)
                result[k.strip()] = v.strip()
            except ValueError:
                pass
        return result

    @classmethod
    def from_thrift(cls, b: bytes):
        return from_thrift(b)

    @classmethod
    def from_avro(cls, b: bytes):
        return from_avro(b)


class ErrorPayload(BaseModel):
    collector_domain: str
    reason: str
    tstamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    payload: Optional[bytes] = None

    def to_bytes(self) -> bytes:
        return self.model_dump_json(by_alias=True).encode("utf-8")


# Thrift Schema

# struct CollectorPayload {
#     31337: string schema

#     // Required fields which are intrinsic properties of HTTP
#     100: string ipAddress

#     // Required fields which are Snowplow-specific
#     200: i64 timestamp
#     210: string encoding
#     220: string collector

#     // Optional fields which are intrinsic properties of HTTP
#     300: optional string userAgent
#     310: optional string refererUri
#     320: optional string path
#     330: optional string querystring
#     340: optional string body
#     350: optional list&lt;string&gt; headers
#     360: optional string contentType

#     // Optional fields which are Snowplow-specific
#     400: optional string hostname
#     410: optional string networkUserId
# }


class TCollectorPayload(TPayload):
    thrift_spec = {
        31337: (TType.STRING, "schema", False),
        100: (TType.STRING, "ipAddress", False),
        200: (TType.I64, "timestamp", False),
        210: (TType.STRING, "encoding", False),
        220: (TType.STRING, "collector", False),
        300: (TType.STRING, "userAgent", False),
        310: (TType.STRING, "refererUri", False),
        320: (TType.STRING, "path", False),
        330: (TType.STRING, "querystring", False),
        340: (TType.STRING, "body", False),
        350: (TType.LIST, "headers", (TType.STRING), False),
        360: (TType.STRING, "contentType", False),
        400: (TType.STRING, "hostname", False),
        410: (TType.STRING, "networkUserId", False)
    }
    default_spec = [
        ("schema", None),
        ("ipAddress", None),
        ("timestamp", None),
        ("encoding", None),
        ("collector", None),
        ("userAgent", None),
        ("refererUri", None),
        ("path", None),
        ("querystring", None),
        ("body", None),
        ("headers", None),
        ("contentType", None),
        ("hostname", None),
        ("networkUserId", None)
    ]


RAW_AVRO_SCHEMA = {
    "type": "record",
    "name": "CollectorPayload",
    "namespace": "io.datenstrom",
    "fields": [
        {"name": "schema", "type": "string"},
        {"name": "ipAddress", "type": "string"},
        {"name": "timestamp", "type": "long"},
        {"name": "encoding", "type": "string"},
        {"name": "collector", "type": "string"},
        {"name": "userAgent", "type": ["null", "string"]},
        {"name": "refererUri", "type": ["null", "string"]},
        {"name": "path", "type": ["null", "string"]},
        {"name": "querystring", "type": ["null", "string"]},
        {"name": "body", "type": ["null", "bytes"]},
        {"name": "headers", "type": ["null", {"type": "array", "items": "string"}]},
        {"name": "contentType", "type": ["null", "string"]},
        {"name": "hostname", "type": ["null", "string"]},
        {"name": "networkUserId", "type": ["null", "string"]}
    ]
}
AVRO_SCHEMA = parse_schema(RAW_AVRO_SCHEMA)


def to_thrift(e: CollectorPayload):
    o = BytesIO()
    d = e.model_dump(by_alias=True)
    te = TCollectorPayload(
        schema=d["schema"],
        ipAddress=d["ipAddress"],
        timestamp=d["timestamp"],
        encoding=d["encoding"],
        collector=d["collector"],
        userAgent=d["userAgent"],
        refererUri=d["refererUri"],
        path=d["path"],
        querystring=d["querystring"],
        body=d["body"],
        headers=d["headers"],
        contentType=d["contentType"],
        hostname=d["hostname"],
        networkUserId=d["networkUserId"]
    )
    proto = THRIFT_PROTO(o)
    proto.write_struct(te)
    return o.getvalue()


def from_thrift(b: bytes) -> CollectorPayload:
    i = BytesIO(b)
    proto = THRIFT_PROTO(i)
    te = TCollectorPayload()
    proto.read_struct(te)
    return CollectorPayload(
        schema=te.schema,
        ipAddress=te.ipAddress,
        timestamp=te.timestamp,
        encoding=te.encoding,
        collector=te.collector,
        userAgent=te.userAgent,
        refererUri=te.refererUri,
        path=te.path,
        querystring=te.querystring,
        body=te.body,
        headers=te.headers,
        contentType=te.contentType,
        hostname=te.hostname,
        networkUserId=te.networkUserId
    )


def to_avro(e: CollectorPayload):
    o = BytesIO()
    d = e.model_dump(by_alias=True)
    schemaless_writer(o, AVRO_SCHEMA, d)
    return o.getvalue()


def from_avro(b: bytes) -> CollectorPayload:
    i = BytesIO(b)
    try:
        d = schemaless_reader(i, AVRO_SCHEMA)
    except EOFError:
        raise ValueError(f"Invalid AVRO message: {b}")
    return CollectorPayload(
        schema=d["schema"],
        ipAddress=d["ipAddress"],
        timestamp=d["timestamp"],
        encoding=d["encoding"],
        collector=d["collector"],
        userAgent=d["userAgent"],
        refererUri=d["refererUri"],
        path=d["path"],
        querystring=d["querystring"],
        body=d["body"],
        headers=d["headers"],
        contentType=d["contentType"],
        hostname=d["hostname"],
        networkUserId=d["networkUserId"]
    )
