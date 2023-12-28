import os
import logging
import sys

import importlib.util
from typing import Optional, List
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, AliasChoices, ImportString
from functools import lru_cache


_config_dir = os.path.dirname(os.path.realpath(__file__))
_base_dir = os.path.abspath(os.path.join(_config_dir, ".."))


class BaseConfig(BaseSettings):
    model_config = SettingsConfigDict()

    base_dir: str = _base_dir
    max_bytes: int = 1000000  # 1 MB

    vendors: List[str] = [
        "com.ruzd"
    ]
    enable_redirect_tracking: bool = False

    record_format: str = Field(validation_alias=AliasChoices("thrift", "avro"))
    sink: str = Field(validation_alias=AliasChoices("dev", "kafka"))

    kafka_topic: str = "datenstrom_raw"
    kafka_brokers: str = Field()

    cookie_enabled: bool = True
    cookie_expiration_days: int = 365
    cookie_name: str = "sp"
    cookie_secure: bool = True
    cookie_http_only: bool = True
    cookie_same_site: str = "None"

    cookie_domains: Optional[List[str]] = None
    cookie_fallback_domain: Optional[str] = None


class DefaultConfig(BaseConfig):
    record_format: str = "avro"
    sink: str = "kafka"
    kafka_brokers: str = "localhost:9093"
    enable_redirect_tracking: bool = True



@lru_cache()
def get_settings():
    config_file = os.environ.get("DATENSTROM_CONFIG", None)
    config_cls = DefaultConfig

    if config_file:
        spec = importlib.util.spec_from_file_location("CustomConfig", config_file)
        custom_config_cls = importlib.util.module_from_spec(spec)
        sys.modules["CustomConfig"] = custom_config_cls
        spec.loader.exec_module(custom_config_cls)
        config_cls = custom_config_cls.Config
        logging.warning(f"Using config file: {config_file}")

    return config_cls()


config = get_settings()
