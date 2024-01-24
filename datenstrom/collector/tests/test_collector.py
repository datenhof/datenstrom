import os
import jwt
import pytest

config_file = os.path.join(os.path.dirname(__file__), "config.json")
os.environ["DATENSTROM_CONFIG"] = config_file

from fastapi.testclient import TestClient
from datenstrom.collector.app import create_app
from datenstrom.common.schema.raw import CollectorPayload


client = TestClient(create_app())


def generate_jwt_token():
    token = jwt.encode(
        {"iss": "https://datenstrom.io",
         "sub": "my_user"},
        "secret",
        algorithm="HS256",
    )
    return token


@pytest.fixture
def last_record():
    def _last_record():
        sink = client.app.state.sink
        format = client.app.config.record_format
        if sink.last_record:
            if format == "avro":
                return CollectorPayload.from_avro(sink.last_record)
            elif format == "thrift":
                return CollectorPayload.from_thrift(sink.last_record)
    return _last_record


def test_config():
    assert client.app.config.kafka_brokers == "testbroker:9092"
    assert client.app.config.sink == "dev"


def test_health():
    response = client.get("/health")
    assert response.status_code == 200


def test_tp2():
    response = client.post("/io.datenstrom/tp2")
    assert response.status_code == 200


def test_authorized(last_record):
    # generate jwt token
    jwt_token = generate_jwt_token()
    headers = {"Authorization": f"Bearer {jwt_token}"}
    response = client.post("/io.datenstrom/tp2", headers=headers)
    assert response.status_code == 200
    assert response.text == ""
    record = last_record()
    assert record.path == "/io.datenstrom/tp2"
    # decode headers
    headers = record.get_headers_dict()
    assert "authorization" in headers
    assert headers["authorization"] == f"Bearer {jwt_token}"
    # decode jwt
    token_info = jwt.decode(jwt_token, "secret", algorithms=["HS256"])
    assert token_info["iss"] == "https://datenstrom.io"
