import os

config_file = os.path.join(os.path.dirname(__file__), "config.json")
os.environ["DATENSTROM_CONFIG"] = config_file

from fastapi.testclient import TestClient
from datenstrom_collector.app import create_app


client = TestClient(create_app())


def test_config():
    assert client.app.config.kafka_brokers == "testbroker:9092"


def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"i am": "ok"}


def test_tp2():
    response = client.post("/io.datenstrom/tp2")
    assert response.status_code == 200
