from typing import List
from datenstrom_collector.settings import BaseConfig


class Config(BaseConfig):
    vendors: List[str] = [
        "io.datenstrom"
    ]
    kafka_brokers: str = "testbroker:9092"
    record_format: str = "avro"
    sink: str = "dev"
    enable_redirect_tracking: bool = True