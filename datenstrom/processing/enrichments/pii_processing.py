from typing import Optional
from datenstrom.processing.enrichments.base import TemporaryAtomicEvent, RemoteEnrichmentConfig


REDACT_IP_PARTS = 3


class PIIProcessor:
    def __init__(self, config: Optional[RemoteEnrichmentConfig] = None):
        self.full_ip = False
        if config:
            self.full_ip = config.enable_full_ip

    def redact_ip(self, ip: str) -> str:
        parts = ip.split(".")
        return ".".join(parts[:REDACT_IP_PARTS] + ["x"] * (4 - REDACT_IP_PARTS))

    def run(self, event: TemporaryAtomicEvent) -> None:
        if "user_ipaddress" in event:
            if not self.full_ip:
                event.set_value("user_ipaddress", self.redact_ip(event["user_ipaddress"]))

